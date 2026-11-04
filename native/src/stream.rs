//! One scan partition: groups of Lance fragments of one or more shards, read as flat record batches.

use std::fmt;
use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::array::RecordBatch;
use beacon_plugin_sdk::datafusion::arrow::datatypes::SchemaRef;
use beacon_plugin_sdk::datafusion::common::{DataFusionError, Result};
use beacon_plugin_sdk::datafusion::execution::{SendableRecordBatchStream, TaskContext};
use beacon_plugin_sdk::datafusion::logical_expr::Expr;
use beacon_plugin_sdk::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use beacon_plugin_sdk::datafusion::physical_plan::streaming::PartitionStream;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use lance::Dataset;
use lance::table::format::Fragment;

use crate::flatten::flatten;
use crate::schema::Column;

/// The fragments of one shard that a partition reads.
#[derive(Clone)]
pub struct ShardPart {
    /// The value of the `__shard` column.
    pub name: Arc<str>,
    pub dataset: Arc<Dataset>,
    pub fragments: Vec<Fragment>,
    /// A filter on the Lance columns of this shard. lance-nd prunes its value parts with the stats.
    pub filter: Option<Expr>,
    /// The variables of this shard that the scan reads.
    pub read: Vec<String>,
}

/// Reads its shard parts one after another and flattens each row onto its grid.
pub struct LanceNdPartition {
    pub parts: Vec<ShardPart>,
    /// The projected schema.
    pub schema: SchemaRef,
    /// The source of each field of `schema`.
    pub columns: Arc<[Column]>,
}

impl fmt::Debug for LanceNdPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for part in &self.parts {
            let ids: Vec<u64> = part.fragments.iter().map(|fragment| fragment.id).collect();
            let filter = part.filter.as_ref().map(|e| e.to_string());
            list.entry(&(&*part.name, ids, filter));
        }
        list.finish()
    }
}

fn external(error: lance_nd::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// The flat record batches of one shard part.
fn read_part(
    part: &ShardPart,
    schema: SchemaRef,
    columns: Arc<[Column]>,
    batch_size: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    let dataset = Arc::clone(&part.dataset);
    let fragments = part.fragments.clone();
    let filter = part.filter.clone();
    let read = part.read.clone();
    let name = Arc::clone(&part.name);
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
    let shard = Arc::clone(&name);
    rows.map(move |row| row.and_then(|row| flatten(&row, &schema, &columns, &name, batch_size)))
        .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
        .try_flatten()
        .map_err(move |e: DataFusionError| e.context(format!("lance-nd table {shard}")))
        .boxed()
}

impl PartitionStream for LanceNdPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batch_size = ctx.session_config().batch_size();
        let parts: Vec<_> = self
            .parts
            .iter()
            .map(|part| {
                read_part(
                    part,
                    Arc::clone(&self.schema),
                    Arc::clone(&self.columns),
                    batch_size,
                )
            })
            .collect();
        let batches = stream::iter(parts).flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batches,
        ))
    }
}
