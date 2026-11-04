//! The test dataset of three lance-nd rows.

use std::path::Path;
use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array};
use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Schema};
use beacon_plugin_sdk::datafusion::prelude::SessionContext;
use futures::stream;
use lance_nd::{WriteMode, WriteOptions};
use nd_arrow_array::{Attributes, Dimensions, NdArrowArray, NdRecordBatch};

use crate::table::tables;

fn array(values: ArrayRef, dims: &[(&str, usize)]) -> NdArrowArray {
    NdArrowArray::try_new(values, Dimensions::try_new(dims.iter().copied()).unwrap()).unwrap()
}

/// An nd record batch of `columns` with the global attribute `id`.
pub fn row(columns: Vec<(&str, NdArrowArray)>, id: &str) -> NdRecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
        .collect();
    let arrays = columns.into_iter().map(|(_, a)| a).collect();
    NdRecordBatch::try_new_inferred(Arc::new(Schema::new(fields)), arrays)
        .unwrap()
        .with_attributes(Attributes::new().with("id", id))
}

/// Row 0: `x (x=3)`, `y (y=4)`, `temperature (x=3, y=4)` with `units = "K"`, and `id = "a"`.
/// 12 cells.
pub fn row0() -> NdRecordBatch {
    grid_row(&[0.0, 1.0, 2.0], 0.0, "K", "a")
}

/// Row 1: `pressure (station=3)` and `id = "b"`. 3 cells.
pub fn row1() -> NdRecordBatch {
    let pressure = Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0]));
    row(vec![("pressure", array(pressure, &[("station", 3)]))], "b")
}

/// Row 2: `x (x=2)`, `y (y=4)`, `temperature (x=2, y=4)` with `units = "C"`, and `id = "c"`.
/// 8 cells.
pub fn row2() -> NdRecordBatch {
    grid_row(&[5.0, 6.0], 100.0, "C", "c")
}

fn grid_row(x: &[f64], base: f32, units: &str, id: &str) -> NdRecordBatch {
    let n = x.len();
    let y = Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0]));
    let temperature = Arc::new(Float32Array::from_iter_values(
        (0..n * 4).map(|i| base + i as f32),
    ));
    row(
        vec![
            (
                "x",
                array(Arc::new(Float64Array::from(x.to_vec())), &[("x", n)]),
            ),
            ("y", array(y, &[("y", 4)])),
            (
                "temperature",
                array(temperature, &[("x", n), ("y", 4)])
                    .with_attributes(Attributes::new().with("units", units)),
            ),
        ],
        id,
    )
}

/// Writes each row in its own commit, so each row is one fragment. Returns the URI.
pub async fn write_rows(dir: &Path, rows: Vec<NdRecordBatch>) -> String {
    write_rows_with(dir, rows, false).await
}

/// Writes one commit per row, with per-row stats when `stats` is `true`. Returns the URI.
pub async fn write_rows_with(dir: &Path, rows: Vec<NdRecordBatch>, stats: bool) -> String {
    let uri = dir.join("table.lance").to_str().unwrap().to_string();
    write_table(&uri, rows, stats).await;
    uri
}

/// Writes one commit per row to the table at `uri`, with per-row stats when `stats` is `true`.
pub async fn write_table(uri: &str, rows: Vec<NdRecordBatch>, stats: bool) {
    for (index, row) in rows.into_iter().enumerate() {
        let options = WriteOptions {
            mode: if index == 0 {
                WriteMode::Create
            } else {
                WriteMode::Append
            },
            stats,
            ..WriteOptions::default()
        };
        lance_nd::write(uri, stream::iter([Ok(row)]), &options)
            .await
            .unwrap();
    }
}

/// Writes the shards `a.lance` (rows 0 and 1) and `b.lance` (row 2) in `dir`. Returns the
/// LOCATION `<dir>/*.lance`.
pub async fn write_shards(dir: &Path) -> String {
    write_table(
        dir.join("a.lance").to_str().unwrap(),
        vec![row0(), row1()],
        false,
    )
    .await;
    write_table(dir.join("b.lance").to_str().unwrap(), vec![row2()], false).await;
    format!("{}/*.lance", dir.to_str().unwrap())
}

/// Writes the three fixture rows. Returns the URI.
pub async fn write_fixture(dir: &Path) -> String {
    write_rows(dir, vec![row0(), row1(), row2()]).await
}

/// A DataFusion session with the table types under their `STORED AS` names.
pub fn session() -> SessionContext {
    let ctx = SessionContext::new();
    let state = ctx.state_ref();
    let mut state = state.write();
    for table_type in tables() {
        let name = table_type.name.to_ascii_uppercase();
        state.table_factories_mut().insert(name, table_type.factory);
    }
    drop(state);
    ctx
}

/// The type of a field, for schema checks.
pub fn field_type(schema: &Schema, name: &str) -> DataType {
    schema.field_with_name(name).unwrap().data_type().clone()
}
