//! `lance_nd`: a table type that reads a lance-nd table as flat rows.
//!
//! `CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '<uri>'` opens the lance-nd table at `<uri>`.
//! The columns are the variables of the table, then the keys of its global attributes.
//! A wildcard in the last path segment of `<uri>` reads each matching lance-nd table as a shard.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::datafusion::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_plugin_sdk::datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use beacon_plugin_sdk::datafusion::common::tree_node::{Transformed, TreeNode};
use beacon_plugin_sdk::datafusion::common::{
    Column as DfColumn, DFSchema, DataFusionError, Result, plan_datafusion_err, plan_err,
};
use beacon_plugin_sdk::datafusion::datasource::TableType as DfTableType;
use beacon_plugin_sdk::datafusion::logical_expr::execution_props::ExecutionProps;
use beacon_plugin_sdk::datafusion::logical_expr::{
    CreateExternalTable, Expr, TableProviderFilterPushDown,
};
use beacon_plugin_sdk::datafusion::physical_expr::{LexOrdering, create_physical_expr};
use beacon_plugin_sdk::datafusion::physical_plan::ExecutionPlan;
use beacon_plugin_sdk::datafusion::physical_plan::streaming::{
    PartitionStream, StreamingTableExec,
};
use beacon_plugin_sdk::table_type::{TableType, table_option};
use futures::{StreamExt, TryStreamExt, stream};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::table::format::Fragment;

use crate::filter::to_lance;
use crate::schema::{Column, SHARD_COLUMN, merge_schemas, table_schema};
use crate::shard::{expand, has_wildcard};
use crate::stream::{LanceNdPartition, ShardPart};

/// The most shards that a CREATE or a scan opens at the same time.
const OPEN_AT_ONCE: usize = 16;

/// The option key for the number of scan partitions.
const PARTITIONS: &str = "partitions";

/// Makes the table of each `CREATE EXTERNAL TABLE ... STORED AS lance_nd`.
#[derive(Debug)]
struct LanceNdFactory;

#[async_trait]
impl TableProviderFactory for LanceNdFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        if !cmd.schema.fields().is_empty() {
            return plan_err!("lance_nd infers its columns from the table; remove the column list");
        }
        let partitions = match table_option(cmd, PARTITIONS) {
            None => None,
            Some(text) => match text.parse::<usize>() {
                Ok(count) if (1..=512).contains(&count) => Some(count),
                _ => return plan_err!("lance_nd: {PARTITIONS} must be 1 to 512, not `{text}`"),
            },
        };
        let location = cmd.location.clone();
        let storage = storage_options(&cmd.options);
        let sharded = has_wildcard(&location);
        let found = expand(&location, &storage).await?;
        let schemas: Vec<(String, SchemaRef, Vec<Column>)> = stream::iter(found.clone())
            .map(|(name, uri)| {
                let storage = storage.clone();
                async move {
                    let dataset = open(&uri, &storage).await?;
                    let (schema, columns) =
                        table_schema(&dataset).map_err(|e| e.context(format!("table `{uri}`")))?;
                    Ok::<_, DataFusionError>((name, schema, columns))
                }
            })
            .buffered(OPEN_AT_ONCE)
            .try_collect()
            .await?;
        let (schema, mut columns) = merge_schemas(&schemas)?;
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        if sharded {
            let reserved = schemas
                .iter()
                .find(|(_, _, columns)| columns.iter().any(|c| c.name() == SHARD_COLUMN));
            if let Some((name, _, _)) = reserved {
                return plan_err!(
                    "lance_nd: `{SHARD_COLUMN}` is reserved in a sharded table, and shard {name} has a column with this name"
                );
            }
            fields.push(Field::new(SHARD_COLUMN, DataType::Utf8, false));
            columns.push(Column::Shard);
        }
        let shards = found
            .into_iter()
            .zip(schemas)
            .map(|((name, uri), (_, _, columns))| Shard { name, uri, columns })
            .collect();
        Ok(Arc::new(LanceNdTable {
            location,
            shards,
            storage,
            schema: Arc::new(Schema::new(fields)),
            columns: columns.into(),
            partitions,
        }))
    }
}

/// One lance-nd table of a `lance_nd` table.
#[derive(Debug, Clone)]
pub struct Shard {
    /// The last path segment of the shard, the value of `__shard`.
    pub name: String,
    pub uri: String,
    /// The columns of this shard at CREATE.
    pub columns: Vec<Column>,
}

/// A lance-nd table, or a set of shards. Each scan opens the latest version of each shard.
pub struct LanceNdTable {
    location: String,
    shards: Vec<Shard>,
    /// Lance storage options, for example the credentials of an object store.
    storage: HashMap<String, String>,
    schema: SchemaRef,
    columns: Arc<[Column]>,
    partitions: Option<usize>,
}

impl fmt::Debug for LanceNdTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceNdTable")
            .field("location", &self.location)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LanceNdTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> DfTableType {
        DfTableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let sharded = self.columns.contains(&Column::Shard);
        // DataFusion applies each Inexact filter again, so Lance may keep extra rows.
        Ok(filters
            .iter()
            .map(|f| match to_lance(f, &self.columns) {
                // All rows of a shard have the same `__shard`, so the shard selection is exact.
                None if sharded && on_shard_only(f) => TableProviderFilterPushDown::Exact,
                Some(_) => TableProviderFilterPushDown::Inexact,
                None => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // In a table without shards, `__shard` can be an ordinary variable.
        let shards = match self.columns.contains(&Column::Shard) {
            true => selected(&self.shards, &shard_filter(filters))?,
            false => self.shards.iter().collect(),
        };
        let uris: Vec<String> = shards.iter().map(|shard| shard.uri.clone()).collect();
        let datasets: Vec<Arc<Dataset>> = stream::iter(uris)
            .map(|uri| {
                let storage = self.storage.clone();
                async move { open(&uri, &storage).await.map(Arc::new) }
            })
            .buffered(OPEN_AT_ONCE)
            .try_collect()
            .await?;
        let (schema, columns): (SchemaRef, Arc<[Column]>) = match projection {
            Some(indices) => (
                Arc::new(self.schema.project(indices)?),
                indices.iter().map(|i| self.columns[*i].clone()).collect(),
            ),
            None => (Arc::clone(&self.schema), Arc::clone(&self.columns)),
        };
        let read = variables_to_read(&columns);
        let parts: Vec<ShardPart> = shards
            .iter()
            .zip(&datasets)
            .map(|(shard, dataset)| ShardPart {
                name: Arc::from(shard.name.as_str()),
                dataset: Arc::clone(dataset),
                fragments: Vec::new(),
                // A shard without a column of a filter does not get that filter.
                filter: filters
                    .iter()
                    .filter_map(|f| to_lance(f, &shard.columns))
                    .reduce(Expr::and),
                read: read
                    .iter()
                    .filter(|name| shard.columns.contains(&Column::Variable(name.to_string())))
                    .cloned()
                    .collect(),
            })
            .collect();
        let items: Vec<(usize, Fragment)> = datasets
            .iter()
            .enumerate()
            .flat_map(|(index, dataset)| {
                dataset.fragments().iter().map(move |f| (index, f.clone()))
            })
            .collect();
        let count = self
            .partitions
            .unwrap_or_else(|| state.config().target_partitions());
        let partitions = fragment_groups(items, count)
            .into_iter()
            .map(|group| {
                let mut own: Vec<ShardPart> = Vec::new();
                for (index, fragment) in group {
                    // Each group is sorted by shard, so a new shard starts a new part.
                    match own.last_mut() {
                        Some(part) if Arc::ptr_eq(&part.dataset, &datasets[index]) => {
                            part.fragments.push(fragment)
                        }
                        _ => own.push(ShardPart {
                            fragments: vec![fragment],
                            ..parts[index].clone()
                        }),
                    }
                }
                Arc::new(LanceNdPartition {
                    parts: own,
                    schema: Arc::clone(&schema),
                    columns: Arc::clone(&columns),
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        let exec = StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            Vec::<LexOrdering>::new(),
            false,
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}

/// `true` when `expr` uses columns, and only the column `__shard`.
fn on_shard_only(expr: &Expr) -> bool {
    let columns = expr.column_refs();
    !columns.is_empty() && columns.iter().all(|c| c.name == SHARD_COLUMN)
}

/// The filters that use only the column `__shard`.
fn shard_filter(filters: &[Expr]) -> Vec<&Expr> {
    filters.iter().filter(|f| on_shard_only(f)).collect()
}

/// The shards whose name makes each of `filters` true. A null result skips the shard.
fn selected<'a>(shards: &'a [Shard], filters: &[&Expr]) -> Result<Vec<&'a Shard>> {
    if filters.is_empty() {
        return Ok(shards.iter().collect());
    }
    let schema = Schema::new(vec![Field::new(SHARD_COLUMN, DataType::Utf8, false)]);
    let names = StringArray::from_iter_values(shards.iter().map(|s| s.name.as_str()));
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(names)])?;
    let df_schema = DFSchema::try_from(schema)?;
    let props = ExecutionProps::new();
    let mut keep = vec![true; shards.len()];
    for filter in filters {
        let filter = (*filter)
            .clone()
            .transform(|e| match e {
                Expr::Column(c) => Ok(Transformed::yes(Expr::Column(DfColumn::new_unqualified(
                    c.name,
                )))),
                e => Ok(Transformed::no(e)),
            })?
            .data;
        let result = create_physical_expr(&filter, &df_schema, &props)?
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;
        let result = result.as_boolean();
        for (index, kept) in keep.iter_mut().enumerate() {
            *kept &= result.is_valid(index) && result.value(index);
        }
    }
    Ok(shards
        .iter()
        .zip(keep)
        .filter_map(|(shard, kept)| kept.then_some(shard))
        .collect())
}

/// `items` round robin over at most `count` groups, each group sorted by shard. Always at least
/// one group.
fn fragment_groups(items: Vec<(usize, Fragment)>, count: usize) -> Vec<Vec<(usize, Fragment)>> {
    let mut groups = vec![Vec::new(); count.min(items.len()).max(1)];
    let len = groups.len();
    for (index, item) in items.into_iter().enumerate() {
        groups[index % len].push(item);
    }
    for group in &mut groups {
        group.sort_by_key(|(shard, _)| *shard);
    }
    groups
}

/// The variables that `columns` read: each variable, and each variable with a selected
/// attribute. Each name comes once, in column order.
fn variables_to_read(columns: &[Column]) -> Vec<String> {
    let mut read: Vec<String> = Vec::new();
    for column in columns {
        let name = match column {
            Column::Variable(name) => name,
            Column::VariableAttribute { variable, .. } => variable,
            Column::Attribute(_) | Column::Shard => continue,
        };
        if !read.contains(name) {
            read.push(name.clone());
        }
    }
    read
}

/// The table options that go to Lance as storage options.
pub fn storage_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    options
        .iter()
        .map(|(key, value)| (key.strip_prefix("format.").unwrap_or(key), value))
        .filter(|(key, _)| *key != PARTITIONS)
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect()
}

/// Opens the latest version of the Lance table at `uri`.
async fn open(uri: &str, storage: &HashMap<String, String>) -> Result<Dataset> {
    DatasetBuilder::from_uri(uri)
        .with_storage_options(storage.clone())
        .load()
        .await
        .map_err(|e| plan_datafusion_err!("lance_nd: cannot open `{uri}`: {e}"))
}

/// Returns every table type of the plugin.
pub fn tables() -> Vec<TableType> {
    vec![TableType::new("lance_nd", Arc::new(LanceNdFactory))]
}

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::datafusion::arrow::array::{
        AsArray, Int32Array, RecordBatch, RecordBatchIterator, TimestampNanosecondArray,
    };
    use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Float64Type, Schema};
    use beacon_plugin_sdk::datafusion::physical_plan::collect;
    use beacon_plugin_sdk::datafusion::prelude::SessionContext;
    use nd_arrow_array::{Attributes, Dimensions, NdArrowArray};
    use std::collections::HashMap;

    use super::*;

    use crate::fixture::{
        field_type, row, row0, row1, row2, session, write_fixture, write_rows, write_rows_with,
        write_shards, write_table,
    };

    async fn create(sql: &str) -> String {
        let ctx = session();
        ctx.sql(sql).await.unwrap_err().to_string()
    }

    #[tokio::test]
    async fn create_infers_the_schema() {
        let dir = tempfile::tempdir().unwrap();
        let uri = write_fixture(dir.path()).await;
        let ctx = session();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}'");
        ctx.sql(&sql).await.unwrap();
        let schema = ctx.table("t").await.unwrap().schema().as_arrow().clone();
        let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names.pop(), Some(".id"));
        assert_eq!(names.pop(), Some("temperature.units"));
        names.sort();
        assert_eq!(names, ["pressure", "temperature", "x", "y"]);
        assert_eq!(field_type(&schema, ".id"), DataType::Utf8);
        assert_eq!(field_type(&schema, "temperature.units"), DataType::Utf8);
        assert_eq!(field_type(&schema, "temperature"), DataType::Float32);
        assert_eq!(field_type(&schema, "x"), DataType::Float64);
    }

    #[tokio::test]
    async fn missing_dataset_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("none.lance");
        let sql = format!(
            "CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{}'",
            uri.display()
        );
        let error = create(&sql).await;
        assert!(error.contains("none.lance"), "{error}");
    }

    #[tokio::test]
    async fn a_column_list_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let uri = write_fixture(dir.path()).await;
        let sql = format!("CREATE EXTERNAL TABLE t (a INT) STORED AS lance_nd LOCATION '{uri}'");
        let error = create(&sql).await;
        assert!(error.contains("infers its columns"), "{error}");
    }

    #[tokio::test]
    async fn partitions_must_be_in_range() {
        let dir = tempfile::tempdir().unwrap();
        let uri = write_fixture(dir.path()).await;
        let sql = format!(
            "CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}' \
             OPTIONS ('partitions' '0')"
        );
        let error = create(&sql).await;
        assert!(error.contains("must be 1 to 512"), "{error}");
    }

    /// A session with table `t` over the fixture, with `options` as the OPTIONS clause.
    async fn table(dir: &tempfile::TempDir, options: &str) -> SessionContext {
        let uri = write_fixture(dir.path()).await;
        let ctx = session();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}' {options}");
        ctx.sql(&sql).await.unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> usize {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    }

    async fn number(ctx: &SessionContext, sql: &str) -> f64 {
        let sql = format!("SELECT CAST(({sql}) AS DOUBLE)");
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).as_primitive::<Float64Type>().value(0)
    }

    #[tokio::test]
    async fn count_star_counts_datasets() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        assert_eq!(number(&ctx, "SELECT count(*) FROM t").await, 3.0);
        // Row 1 has no temperature: one cell with a null.
        assert_eq!(
            number(&ctx, "SELECT count(*) FROM t WHERE temperature IS NOT NULL").await,
            20.0
        );
    }

    #[tokio::test]
    async fn selected_variables_set_the_grid() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        assert_eq!(rows(&ctx, r#"SELECT x FROM t WHERE ".id" = 'a'"#).await, 3);
        assert_eq!(
            rows(&ctx, r#"SELECT x, temperature FROM t WHERE ".id" = 'a'"#).await,
            12
        );
        // An attribute of a variable does not add the dimensions of the variable.
        let sql = r#"SELECT x, "temperature.units" FROM t WHERE ".id" = 'a'"#;
        assert_eq!(rows(&ctx, sql).await, 3);
    }

    #[tokio::test]
    async fn different_grids_are_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let error = ctx
            .sql("SELECT x, y FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("no common grid"), "{error}");
        assert!(
            error.contains("(x=3): x") && error.contains("(y=4): y"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn filter_on_a_variable() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let sql = "SELECT count(*) FROM t WHERE temperature >= 100";
        assert_eq!(number(&ctx, sql).await, 8.0);
    }

    #[tokio::test]
    async fn sum_over_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        assert_eq!(number(&ctx, "SELECT sum(pressure) FROM t").await, 6.0);
    }

    #[tokio::test]
    async fn limit_stops_early() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        assert_eq!(rows(&ctx, "SELECT * FROM t LIMIT 5").await, 5);
    }

    #[tokio::test]
    async fn partitions_give_the_same_result() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "OPTIONS ('partitions' '4')").await;
        assert_eq!(number(&ctx, "SELECT count(*) FROM t").await, 3.0);
        assert_eq!(
            number(&ctx, "SELECT sum(temperature) FROM t").await,
            66.0 + 828.0
        );
    }

    #[tokio::test]
    async fn pushed_filter_gives_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let sql = r#"SELECT count(*) FROM t WHERE ".id" IN ('a', 'c')"#;
        assert_eq!(number(&ctx, sql).await, 2.0);
        assert_eq!(scan_rows(&ctx, sql).await, 2);
    }

    #[tokio::test]
    async fn keyword_attribute_is_pushed() {
        let dir = tempfile::tempdir().unwrap();
        let attributes = Attributes::new().with("id", "a").with("null", "k");
        let uri = write_rows(dir.path(), vec![row0().with_attributes(attributes), row1()]).await;
        let ctx = session();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}'");
        ctx.sql(&sql).await.unwrap();
        let sql = r#"SELECT count(*) FROM t WHERE ".null" = 'k'"#;
        assert_eq!(number(&ctx, sql).await, 1.0);
        assert_eq!(scan_rows(&ctx, sql).await, 1);
    }

    #[tokio::test]
    async fn non_nd_table_error_names_the_uri() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("plain.lance").to_str().unwrap().to_string();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let reader = RecordBatchIterator::new([Ok(batch)], schema);
        Dataset::write(reader, &uri, None).await.unwrap();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}'");
        let error = create(&sql).await;
        assert!(error.contains("plain.lance"), "{error}");
    }

    #[tokio::test]
    async fn filter_on_a_variable_attribute() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let sql = r#"SELECT count(*) FROM t WHERE "temperature.units" = 'C'"#;
        assert_eq!(number(&ctx, sql).await, 1.0);
        assert_eq!(scan_rows(&ctx, sql).await, 1);
    }

    #[tokio::test]
    async fn select_a_variable_attribute() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let sql = r#"SELECT DISTINCT "temperature.units" FROM t WHERE ".id" = 'b'"#;
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(batches[0].column(0).null_count(), 1);
    }

    /// The error of a CREATE at `location`, or `None` when it runs past 20 seconds.
    async fn create_error(location: &str, options: &str) -> Option<String> {
        let ctx = session();
        let sql = format!(
            "CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{location}' OPTIONS ({options})"
        );
        let run = async { ctx.sql(&sql).await.map(|_| ()) };
        match tokio::time::timeout(std::time::Duration::from_secs(20), run).await {
            Ok(result) => Some(result.unwrap_err().to_string()),
            Err(_) => None,
        }
    }

    #[tokio::test]
    async fn cloud_schemes_have_a_store() {
        let cases = [
            (
                "s3://bucket/t.lance",
                "'aws_endpoint' 'http://127.0.0.1:9', 'aws_allow_http' 'true',                  'aws_region' 'us-east-1', 'aws_access_key_id' 'k',                  'aws_secret_access_key' 's', 'client_max_retries' '0'",
            ),
            (
                "az://container/t.lance",
                "'azure_storage_account_name' 'account', 'azure_storage_account_key' 'a2V5',                  'client_max_retries' '0'",
            ),
            ("gs://bucket/t.lance", "'client_max_retries' '0'"),
        ];
        for (location, options) in cases {
            if let Some(error) = create_error(location, options).await {
                assert!(
                    !error.contains("No object store provider"),
                    "{location}: {error}"
                );
            }
        }
    }

    #[test]
    fn storage_options_skip_plugin_keys() {
        let options = HashMap::from([
            ("format.aws_region".to_string(), "eu-west-1".to_string()),
            ("format.partitions".to_string(), "4".to_string()),
            ("aws_endpoint".to_string(), "http://minio:9000".to_string()),
        ]);
        let storage = storage_options(&options);
        assert_eq!(storage.len(), 2);
        assert_eq!(storage["aws_region"], "eu-west-1");
        assert_eq!(storage["aws_endpoint"], "http://minio:9000");
    }

    #[tokio::test]
    async fn stats_column_is_not_a_variable() {
        let dir = tempfile::tempdir().unwrap();
        let uri = write_rows_with(dir.path(), vec![row0(), row1(), row2()], true).await;
        let ctx = session();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}'");
        ctx.sql(&sql).await.unwrap();
        let schema = ctx.table("t").await.unwrap().schema().as_arrow().clone();
        assert!(
            schema
                .fields()
                .iter()
                .all(|f| !f.name().contains("__stats__"))
        );
        assert_eq!(number(&ctx, "SELECT count(*) FROM t").await, 3.0);
    }

    /// The flat rows of the scan node alone, without the filter of DataFusion.
    async fn scan_rows(ctx: &SessionContext, sql: &str) -> usize {
        fn find(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
            if plan.downcast_ref::<StreamingTableExec>().is_some() {
                return Some(Arc::clone(plan));
            }
            plan.children().into_iter().find_map(find)
        }
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let scan = find(&plan).unwrap();
        let batches = collect(scan, ctx.task_ctx()).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    }

    async fn stats_table(dir: &tempfile::TempDir) -> SessionContext {
        let uri = write_rows_with(dir.path(), vec![row0(), row1(), row2()], true).await;
        let ctx = session();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}'");
        ctx.sql(&sql).await.unwrap();
        ctx
    }

    #[tokio::test]
    async fn stats_skip_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = stats_table(&dir).await;
        let sql = "SELECT temperature FROM t WHERE temperature > 50";
        // Only row 2 has a temperature above 50. Row 1 has no temperature.
        assert_eq!(scan_rows(&ctx, sql).await, 8);
        assert_eq!(rows(&ctx, sql).await, 8);
    }

    #[tokio::test]
    async fn stats_keep_the_whole_row() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = stats_table(&dir).await;
        let sql = "SELECT temperature FROM t WHERE temperature < 5";
        assert_eq!(scan_rows(&ctx, sql).await, 12);
        assert_eq!(rows(&ctx, sql).await, 5);
    }

    #[tokio::test]
    async fn stats_and_attribute_filters_combine() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = stats_table(&dir).await;
        let sql = r#"SELECT temperature FROM t WHERE ".id" IN ('a', 'c') AND temperature > 50"#;
        // The attribute part keeps rows 0 and 2. The stats part keeps row 2.
        assert_eq!(scan_rows(&ctx, sql).await, 8);
        assert_eq!(rows(&ctx, sql).await, 8);
    }

    #[tokio::test]
    async fn table_without_stats_keeps_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let sql = "SELECT temperature FROM t WHERE temperature > 50";
        // 12 and 8 cells, and one cell for row 1, which has no temperature.
        assert_eq!(scan_rows(&ctx, sql).await, 21);
        assert_eq!(rows(&ctx, sql).await, 8);
    }

    #[tokio::test]
    async fn mixed_or_skips_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = stats_table(&dir).await;
        let sql = r#"SELECT count(*) FROM t WHERE ".id" = 'b' OR temperature > 50"#;
        // Row 1 (one cell) by its id, row 2 (8 cells) by its stats. Row 0 has neither.
        assert_eq!(scan_rows(&ctx, sql).await, 9);
        assert_eq!(number(&ctx, sql).await, 9.0);
    }

    #[tokio::test]
    async fn not_over_an_attribute_is_exact() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let sql = r#"SELECT count(*) FROM t WHERE NOT (".id" = 'a')"#;
        assert_eq!(scan_rows(&ctx, sql).await, 2);
        assert_eq!(number(&ctx, sql).await, 2.0);
    }

    #[tokio::test]
    async fn not_over_a_variable_uses_the_stats() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = stats_table(&dir).await;
        let sql = "SELECT count(*) FROM t WHERE NOT (temperature > 50)";
        // DataFusion pushes `temperature <= 50`: row 0 stays, row 1 has no temperature.
        assert_eq!(scan_rows(&ctx, sql).await, 12);
        assert_eq!(number(&ctx, sql).await, 12.0);
    }

    #[tokio::test]
    async fn time_filter_uses_the_stats() {
        let dir = tempfile::tempdir().unwrap();
        let times = |values: Vec<i64>| {
            NdArrowArray::try_new(
                Arc::new(TimestampNanosecondArray::from(values)),
                Dimensions::try_new([("time", 2)]).unwrap(),
            )
            .unwrap()
        };
        let second = 1_000_000_000;
        let rows = vec![
            row(vec![("time", times(vec![100 * second, 200 * second]))], "a"),
            row(
                vec![("time", times(vec![1000 * second, 2000 * second]))],
                "b",
            ),
        ];
        let uri = write_rows_with(dir.path(), rows, true).await;
        let ctx = session();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}'");
        ctx.sql(&sql).await.unwrap();
        let sql = "SELECT count(*) FROM t WHERE time > TIMESTAMP '1970-01-01T00:08:20'";
        assert_eq!(scan_rows(&ctx, sql).await, 2);
        assert_eq!(number(&ctx, sql).await, 2.0);
    }

    #[tokio::test]
    async fn like_between_and_cast_are_pushed() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = stats_table(&dir).await;
        let cases = [
            (r#"SELECT count(*) FROM t WHERE ".id" LIKE 'c%'"#, 1, 1.0),
            (
                "SELECT count(*) FROM t WHERE temperature BETWEEN 50 AND 200",
                8,
                8.0,
            ),
            (
                r#"SELECT count(*) FROM t WHERE CAST(".id" AS VARCHAR) = 'a'"#,
                1,
                1.0,
            ),
        ];
        for (sql, scanned, count) in cases {
            assert_eq!(scan_rows(&ctx, sql).await, scanned, "{sql}");
            assert_eq!(number(&ctx, sql).await, count, "{sql}");
        }
    }

    async fn shard_table(location: &str, options: &str) -> SessionContext {
        let ctx = session();
        let sql =
            format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{location}' {options}");
        ctx.sql(&sql).await.unwrap();
        ctx
    }

    /// The first `StreamingTableExec` in `plan`.
    fn scan_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&StreamingTableExec> {
        if let Some(exec) = plan.downcast_ref::<StreamingTableExec>() {
            return Some(exec);
        }
        plan.children().into_iter().find_map(scan_exec)
    }

    #[tokio::test]
    async fn shards_read_as_one_table() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = shard_table(&write_shards(dir.path()).await, "").await;
        assert_eq!(number(&ctx, "SELECT count(*) FROM t").await, 3.0);
        assert_eq!(
            number(&ctx, "SELECT sum(temperature) FROM t").await,
            66.0 + 828.0
        );
        let sql = r#"SELECT "__shard", count(*) FROM t GROUP BY 1 ORDER BY 1"#;
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let batch = &batches[0];
        let shards: Vec<String> = (0..batch.num_rows())
            .map(|i| batch.column(0).as_string::<i32>().value(i).to_string())
            .collect();
        assert_eq!(shards, ["a.lance", "b.lance"]);
        let counts = batch
            .column(1)
            .as_primitive::<beacon_plugin_sdk::datafusion::arrow::datatypes::Int64Type>();
        assert_eq!(counts.values().to_vec(), [2, 1]);
    }

    #[tokio::test]
    async fn columns_of_one_shard_are_null_in_the_other() {
        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name).to_str().unwrap().to_string();
        write_table(&path("a.lance"), vec![row0(), row1()], false).await;
        let attributes = Attributes::new().with("id", "c").with("station", "s1");
        write_table(
            &path("b.lance"),
            vec![row2().with_attributes(attributes)],
            false,
        )
        .await;
        let location = format!("{}/*.lance", dir.path().to_str().unwrap());
        let ctx = shard_table(&location, "").await;
        assert_eq!(
            number(&ctx, r#"SELECT count(".station") FROM t"#).await,
            1.0
        );
        assert_eq!(number(&ctx, "SELECT count(*) FROM t").await, 3.0);
    }

    #[tokio::test]
    async fn filter_on_a_column_of_one_shard() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = shard_table(&write_shards(dir.path()).await, "").await;
        // `pressure` is in a.lance only. Row 1 has the pressures 1, 2 and 3.
        assert_eq!(
            number(&ctx, "SELECT count(*) FROM t WHERE pressure > 1").await,
            2.0
        );
        // a.lance: row 0 (one cell, no pressure) and row 1 (3 cells). b.lance gets no filter: one cell.
        assert_eq!(
            scan_rows(&ctx, "SELECT count(*) FROM t WHERE pressure > 1").await,
            1 + 3 + 1
        );
    }

    #[tokio::test]
    async fn partitions_spread_fragments_of_all_shards() {
        let dir = tempfile::tempdir().unwrap();
        let location = write_shards(dir.path()).await;
        let ctx = shard_table(&location, "OPTIONS ('partitions' '3')").await;
        let sql = "SELECT count(*) FROM t";
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert_eq!(scan_exec(&plan).unwrap().partitions().len(), 3);
        assert_eq!(number(&ctx, sql).await, 3.0);
        assert_eq!(
            number(&ctx, "SELECT sum(temperature) FROM t").await,
            66.0 + 828.0
        );
    }

    #[tokio::test]
    async fn stray_match_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let location = write_shards(dir.path()).await;
        std::fs::create_dir(dir.path().join("c.lance")).unwrap();
        std::fs::write(dir.path().join("c.lance").join("notes.txt"), b"x").unwrap();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{location}'");
        let error = create(&sql).await;
        assert!(error.contains("c.lance"), "{error}");
    }

    #[tokio::test]
    async fn reserved_shard_name_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name).to_str().unwrap().to_string();
        write_table(&path("a.lance"), vec![row0()], false).await;
        let values =
            Arc::new(beacon_plugin_sdk::datafusion::arrow::array::Float64Array::from(vec![1.0]));
        let shard =
            NdArrowArray::try_new(values, Dimensions::try_new([("n", 1)]).unwrap()).unwrap();
        write_table(
            &path("b.lance"),
            vec![row(vec![("__shard", shard)], "z")],
            false,
        )
        .await;
        let location = format!("{}/*.lance", dir.path().to_str().unwrap());
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{location}'");
        let error = create(&sql).await;
        assert!(
            error.contains("__shard") && error.contains("reserved"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn one_location_has_no_shard_column() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        let schema = ctx.table("t").await.unwrap().schema().as_arrow().clone();
        assert!(schema.field_with_name("__shard").is_err());
    }

    #[tokio::test]
    async fn shard_filter_selects_shards() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = shard_table(&write_shards(dir.path()).await, "").await;
        let only_b = r#"SELECT count(*) FROM t WHERE "__shard" = 'b.lance'"#;
        assert_eq!(scan_rows(&ctx, only_b).await, 1);
        assert_eq!(number(&ctx, only_b).await, 1.0);
        let like_a = r#"SELECT count(*) FROM t WHERE "__shard" LIKE 'a%'"#;
        assert_eq!(number(&ctx, like_a).await, 2.0);
        let both = r#"SELECT count(*) FROM t WHERE "__shard" IN ('a.lance', 'b.lance')"#;
        assert_eq!(number(&ctx, both).await, 3.0);
    }

    #[tokio::test]
    async fn shard_filter_skips_deleted_shard() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = shard_table(&write_shards(dir.path()).await, "").await;
        std::fs::remove_dir_all(dir.path().join("a.lance")).unwrap();
        let sql = r#"SELECT count(*) FROM t WHERE "__shard" = 'b.lance'"#;
        assert_eq!(number(&ctx, sql).await, 1.0);
    }

    #[tokio::test]
    async fn deleted_shard_names_itself() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = shard_table(&write_shards(dir.path()).await, "").await;
        std::fs::remove_dir_all(dir.path().join("a.lance")).unwrap();
        let error = ctx
            .sql("SELECT count(*) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("a.lance"), "{error}");
    }

    #[tokio::test]
    async fn one_location_filters_a_variable_named_shard() {
        let dir = tempfile::tempdir().unwrap();
        let values = Arc::new(
            beacon_plugin_sdk::datafusion::arrow::array::Float64Array::from(vec![1.0, 0.0]),
        );
        let shard =
            NdArrowArray::try_new(values, Dimensions::try_new([("n", 2)]).unwrap()).unwrap();
        let uri = write_rows(dir.path(), vec![row(vec![("__shard", shard)], "z")]).await;
        let ctx = shard_table(&uri, "").await;
        let sql = r#"SELECT count(*) FROM t WHERE "__shard" > 0.5"#;
        assert_eq!(number(&ctx, sql).await, 1.0);
    }

    #[tokio::test]
    async fn read_error_names_the_shard() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = shard_table(
            &write_shards(dir.path()).await,
            "OPTIONS ('partitions' '1')",
        )
        .await;
        // x and y have no common grid in row 0 of a.lance.
        let error = ctx
            .sql(r#"SELECT x, y FROM t WHERE "__shard" = 'a.lance'"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("no common grid"), "{error}");
        assert!(error.contains("a.lance"), "{error}");
    }
}
