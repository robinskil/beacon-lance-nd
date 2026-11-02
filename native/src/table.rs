//! `lance_nd`: a table type that reads a lance-nd table as flat rows.
//!
//! `CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '<uri>'` opens the lance-nd table at `<uri>`.
//! The columns are the variables of the table, then the keys of its global attributes.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::datafusion::arrow::datatypes::SchemaRef;
use beacon_plugin_sdk::datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use beacon_plugin_sdk::datafusion::common::{Result, not_impl_err, plan_datafusion_err, plan_err};
use beacon_plugin_sdk::datafusion::datasource::TableType as DfTableType;
use beacon_plugin_sdk::datafusion::logical_expr::{CreateExternalTable, Expr};
use beacon_plugin_sdk::datafusion::physical_plan::ExecutionPlan;
use beacon_plugin_sdk::table_type::{TableType, table_option};
use lance::Dataset;

use crate::schema::{Column, columns, table_schema};

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
        let schema = table_schema(&dataset)?;
        let columns = columns(&schema, &dataset).into();
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

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _ = (&self.columns, self.partitions);
        not_impl_err!("lance_nd scan")
    }
}

/// Returns every table type of the plugin.
pub fn tables() -> Vec<TableType> {
    vec![TableType::new("lance_nd", Arc::new(LanceNdFactory))]
}

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::datafusion::arrow::datatypes::DataType;

    use crate::fixture::{field_type, session, write_fixture};

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
        assert_eq!(names.pop(), Some("id"));
        names.sort();
        assert_eq!(names, ["pressure", "temperature", "x", "y"]);
        assert_eq!(field_type(&schema, "id"), DataType::Utf8);
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
}
