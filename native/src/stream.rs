//! One scan partition: a group of Lance fragments, read as flat record batches.

use std::fmt;
use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::datatypes::SchemaRef;
use beacon_plugin_sdk::datafusion::common::DataFusionError;
use beacon_plugin_sdk::datafusion::execution::{SendableRecordBatchStream, TaskContext};
use beacon_plugin_sdk::datafusion::logical_expr::Expr;
use beacon_plugin_sdk::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use beacon_plugin_sdk::datafusion::physical_plan::streaming::PartitionStream;
use futures::{StreamExt, TryStreamExt, stream};
use lance::Dataset;
use lance::table::format::Fragment;

use crate::flatten::flatten;
use crate::schema::Column;

/// Reads `fragments` of `dataset` and flattens each row onto its grid.
pub struct LanceNdPartition {
    pub dataset: Arc<Dataset>,
    pub fragments: Vec<Fragment>,
    /// A filter on the Lance columns. lance-nd prunes its value parts with the stats.
    pub filter: Option<Expr>,
    /// The projected schema.
    pub schema: SchemaRef,
    /// The source of each field of `schema`.
    pub columns: Arc<[Column]>,
}

impl fmt::Debug for LanceNdPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ids: Vec<u64> = self.fragments.iter().map(|fragment| fragment.id).collect();
        f.debug_struct("LanceNdPartition")
            .field("fragments", &ids)
            .field("filter", &self.filter.as_ref().map(|e| e.to_string()))
            .finish()
    }
}

fn external(error: lance_nd::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

impl PartitionStream for LanceNdPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batch_size = ctx.session_config().batch_size();
        let dataset = Arc::clone(&self.dataset);
        let fragments = self.fragments.clone();
        let filter = self.filter.clone();
        let schema = Arc::clone(&self.schema);
        let columns = Arc::clone(&self.columns);
        let read = variables_to_read(&columns);
        let rows = stream::once(async move {
            let mut scanner = lance_nd::scan(&dataset)
                .project(&read.iter().map(String::as_str).collect::<Vec<_>>())
                .fragments(fragments);
            if let Some(filter) = filter {
                scanner = scanner.filter_expr(filter);
            }
            let rows = scanner.try_into_stream().await.map_err(external)?;
            Ok::<_, DataFusionError>(rows.map_err(external))
        })
        .try_flatten();
        let flat_schema = Arc::clone(&schema);
        let batches = rows
            .map(move |row| {
                row.and_then(|row| flatten(&row, &flat_schema, &columns, "", batch_size))
            })
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
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
