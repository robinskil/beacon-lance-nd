//! `lance_nd`: a table type that reads a lance-nd table as flat rows.
//!
//! `CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '<uri>'` opens the lance-nd table at `<uri>`.
//! The columns are the variables of the table, then the keys of its global attributes.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::datafusion::arrow::datatypes::SchemaRef;
use beacon_plugin_sdk::datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use beacon_plugin_sdk::datafusion::common::{Result, plan_datafusion_err, plan_err};
use beacon_plugin_sdk::datafusion::datasource::TableType as DfTableType;
use beacon_plugin_sdk::datafusion::logical_expr::{
    CreateExternalTable, Expr, TableProviderFilterPushDown,
};
use beacon_plugin_sdk::datafusion::physical_expr::LexOrdering;
use beacon_plugin_sdk::datafusion::physical_plan::ExecutionPlan;
use beacon_plugin_sdk::datafusion::physical_plan::streaming::{
    PartitionStream, StreamingTableExec,
};
use beacon_plugin_sdk::table_type::{TableType, table_option};
use lance::Dataset;
use lance::table::format::Fragment;

use crate::filter::to_lance;
use crate::schema::{Column, table_schema};
use crate::stream::LanceNdPartition;

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
        let uri = cmd.location.clone();
        let dataset = Dataset::open(&uri)
            .await
            .map_err(|e| plan_datafusion_err!("lance_nd: cannot open `{uri}`: {e}"))?;
        let (schema, columns) =
            table_schema(&dataset).map_err(|e| e.context(format!("table `{uri}`")))?;
        let columns = columns.into();
        Ok(Arc::new(LanceNdTable {
            uri,
            schema,
            columns,
            partitions,
        }))
    }
}

/// A lance-nd table. Each scan opens the latest version.
pub struct LanceNdTable {
    uri: String,
    schema: SchemaRef,
    columns: Arc<[Column]>,
    partitions: Option<usize>,
}

impl fmt::Debug for LanceNdTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceNdTable")
            .field("uri", &self.uri)
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
        // DataFusion applies each filter again, so Lance may keep extra rows.
        Ok(filters
            .iter()
            .map(|f| match to_lance(f, &self.columns) {
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
        let uri = &self.uri;
        let dataset = Dataset::open(uri)
            .await
            .map_err(|e| plan_datafusion_err!("lance_nd: cannot open `{uri}`: {e}"))?;
        let dataset = Arc::new(dataset);
        let (schema, columns): (SchemaRef, Arc<[Column]>) = match projection {
            Some(indices) => (
                Arc::new(self.schema.project(indices)?),
                indices.iter().map(|i| self.columns[*i].clone()).collect(),
            ),
            None => (Arc::clone(&self.schema), Arc::clone(&self.columns)),
        };
        let pushed: Vec<String> = filters
            .iter()
            .filter_map(|f| to_lance(f, &self.columns))
            .collect();
        let filter = match pushed.len() {
            0 => None,
            1 => pushed.into_iter().next(),
            _ => Some(format!("({})", pushed.join(") AND ("))),
        };
        let count = self
            .partitions
            .unwrap_or_else(|| state.config().target_partitions());
        let partitions = fragment_groups(dataset.fragments(), count)
            .into_iter()
            .map(|fragments| {
                Arc::new(LanceNdPartition {
                    dataset: Arc::clone(&dataset),
                    fragments,
                    filter: filter.clone(),
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

/// `fragments` round robin over at most `count` groups. Always at least one group.
fn fragment_groups(fragments: &[Fragment], count: usize) -> Vec<Vec<Fragment>> {
    let mut groups = vec![Vec::new(); count.min(fragments.len()).max(1)];
    let len = groups.len();
    for (index, fragment) in fragments.iter().enumerate() {
        groups[index % len].push(fragment.clone());
    }
    groups
}

/// Returns every table type of the plugin.
pub fn tables() -> Vec<TableType> {
    vec![TableType::new("lance_nd", Arc::new(LanceNdFactory))]
}

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::datafusion::arrow::array::{
        AsArray, Int32Array, RecordBatch, RecordBatchIterator,
    };
    use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Float64Type, Schema};
    use beacon_plugin_sdk::datafusion::prelude::SessionContext;
    use nd_arrow_array::Attributes;

    use super::*;

    use crate::fixture::{field_type, row0, session, write_fixture, write_rows};

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
    async fn count_star_counts_cells() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        assert_eq!(number(&ctx, "SELECT count(*) FROM t").await, 23.0);
    }

    #[tokio::test]
    async fn projection_keeps_the_full_grid() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = table(&dir, "").await;
        assert_eq!(rows(&ctx, r#"SELECT x FROM t WHERE ".id" = 'a'"#).await, 12);
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
        assert_eq!(number(&ctx, "SELECT count(*) FROM t").await, 23.0);
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
        assert_eq!(number(&ctx, sql).await, 20.0);
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let shown = scan_partitions(&plan);
        assert!(shown.contains("__global_attrs__.id = 'c'"), "{shown}");
    }

    /// The Debug text of the partitions of the first `StreamingTableExec` in `plan`.
    fn scan_partitions(plan: &Arc<dyn ExecutionPlan>) -> String {
        if let Some(exec) = plan.downcast_ref::<StreamingTableExec>() {
            return format!("{:?}", exec.partitions());
        }
        plan.children().into_iter().map(scan_partitions).collect()
    }

    #[tokio::test]
    async fn keyword_attribute_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let attributes = Attributes::new().with("id", "a").with("null", "k");
        let uri = write_rows(dir.path(), vec![row0().with_attributes(attributes)]).await;
        let ctx = session();
        let sql = format!("CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '{uri}'");
        ctx.sql(&sql).await.unwrap();
        let sql = r#"SELECT count(*) FROM t WHERE ".null" = 'k'"#;
        assert_eq!(number(&ctx, sql).await, 12.0);
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
        assert_eq!(number(&ctx, sql).await, 8.0);
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let shown = scan_partitions(&plan);
        assert!(shown.contains("temperature.attrs.units = 'C'"), "{shown}");
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
}
