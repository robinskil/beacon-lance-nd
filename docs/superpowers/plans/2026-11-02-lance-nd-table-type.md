# lance-nd Table Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Beacon plugin with the table type `lance_nd`, so `CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '<uri>'` reads a lance-nd table as flat rows.

**Architecture:** The plugin opens the Lance dataset at `CREATE` time and infers the schema from it. A scan streams `NdRecordBatch` rows with `lance_nd::scan`, broadcasts each row onto its grid, and emits flat Arrow record batches. Beacon does not change: the plugin does the nd-to-flat step itself.

**Tech Stack:** Rust 2024, `beacon-plugin-sdk` (DataFusion 54, Arrow 58), `lance-nd` (Lance 11), `nd-arrow-array`.

**Spec:** No separate spec. The decisions are in "Design" below. The user chose these two in chat:
- Plugin only, flat rows. No Beacon host change. (The nd region optimizations of Beacon do not apply.)
- Each `__global_attrs__` key is a column. Its value repeats on every flat row of that lance-nd row.

## Design

| Topic | Decision | Source |
|---|---|---|
| Where nd becomes flat | In the plugin scan | user |
| Global attributes | One column per key, the scalar value repeated on each flat row | user |
| Table schema | Inferred at `CREATE`: `lance_nd::variables(&dataset)` fields, then the children of `__global_attrs__`, in that order | my call |
| Column list in `CREATE` | Refused with a plan error. The schema always comes from the dataset. | my call |
| Name clash of a variable and a global attribute key | Refused at `CREATE` with a plan error that names the key | my call |
| The grid of a lance-nd row | The target of the **full** row (all variables), not of the projected columns. So a projection never changes the row count, and `count(*)` and `count(x)` agree with DataFusion. | my call |
| A variable that the row does not have | Null on every flat row of that row | my call |
| A row without variables | Target is a scalar, so one flat row with the attribute values | my call |
| `__stats__` and per-variable `attrs` | Not exposed | my call |
| Dataset version | Each scan reads the latest version. New columns show only after `DROP` and `CREATE` again. | my call |
| Writes (`INSERT INTO`, `COPY TO`) | Not in scope | my call |
| Storage | Local paths and `file://` URIs. No cloud features of Lance. | my call |
| Filters | Filters on global attribute columns go to Lance as `Inexact`. DataFusion applies every filter again. | my call |
| Partitions | Fragments split round robin over N partitions. N is option `partitions`, default the session `target_partitions`. | my call |
| Template examples | Removed: the scalar functions, `example_tsv`, `example_memory` and `sql/functions.sql` | my call |

## Global Constraints

- Arrow major version: `58`. One `arrow-array` version in `cargo tree -d`.
- Lance: `=11.0.0`, the same pin as Beacon `Cargo.toml`.
- `beacon-plugin-sdk`: git `https://github.com/maris-development/beacon`, `branch = "main"`, as the template has it.
- `lance-nd`: git `https://github.com/robinskil/lance-nd`. Pin a `rev`, because the crate is not on crates.io.
- Plugin name: `lance_nd`. Crate: `lance-nd-native`. Library: `lance_nd_native`. `STORED AS` name: `lance_nd`.
- Table option keys: `partitions` (integer 1 to 512).
- Write docs and comments in ASD-STE100. Comments: one short line, intent only.
- Commit messages: imperative, about 50 characters, no prefix like `feat:`, no trailers.
- Do not commit `.env` files or keys.

## Review Focus

1. **`count(*)` with an empty projection.** DataFusion asks for zero columns. Expect the cell count of every full row grid, not one row per lance-nd row. Test: `count_star_counts_cells` (Task 3).
2. **A projection of one coordinate only.** `SELECT x FROM t WHERE id = 'a'` must give 12 rows, not 3, because the grid is the full row. Test: `projection_keeps_the_full_grid` (Task 3).
3. **A row without the projected variable.** `SELECT pressure` on a row without `pressure` gives nulls on that row's cells. Test: `absent_variable_is_null` (Task 2).
4. **A string attribute with a quote.** A filter `id = 'o''brien'` must reach Lance escaped, or fall back. Test: `quote_in_literal_is_escaped` (Task 4).
5. **A missing or wrong `LOCATION`.** Expect a plan error that names the URI, not a panic over FFI. Test: `missing_dataset_is_an_error` (Task 1).

Known limits, not tested: one lance-nd row materializes fully in memory after the broadcast. A scan reads all variables to find the row grid, also for a narrow projection. A follow-up in lance-nd can add a scan of `dim_names` and `dim_sizes` only.

---

## Test fixture

Tasks 2 to 5 use one fixture. Task 1 creates it in `native/src/fixture.rs` (`#[cfg(test)]`).

`async fn write_fixture(dir: &std::path::Path) -> String` writes three rows with `lance_nd::write` and returns the URI:

| Row | Variables | `__global_attrs__` | Flat rows |
|---|---|---|---|
| 0 | `x` Float64 `(x=3)` = `[0,1,2]`; `y` Float64 `(y=4)` = `[0,1,2,3]`; `temperature` Float32 `(x=3,y=4)` = `0..12` | `id = "a"` | 12 |
| 1 | `pressure` Float32 `(station=3)` = `[1,2,3]` | `id = "b"` | 3 |
| 2 | `x` `(x=2)` = `[5,6]`; `y` `(y=4)`; `temperature` `(x=2,y=4)` = `100..108` | `id = "c"` | 8 |

Total: 23 flat rows. Table schema: `pressure`, `temperature`, `x`, `y` (the order `variables()` gives), then `id` (Utf8). Tests must read the variable order from the schema, not assume it.

`fn session() -> SessionContext` registers `tables()` under the upper-case name, as the template test in `native/src/memory.rs` does.

---

### Task 1: Plugin skeleton and `CREATE EXTERNAL TABLE`

**Files:**
- Modify: `beacon-plugin.toml`, `native/Cargo.toml`, `.github/workflows/release.yml` (library name), `native/src/lib.rs`
- Delete: `native/src/memory.rs`, `native/src/tsv.rs`, `native/testdata/stations.tsv`, `sql/functions.sql`
- Create: `native/src/table.rs`, `native/src/schema.rs`, `native/src/fixture.rs`

**Interfaces:**
- Produces:
  - `schema::table_schema(dataset: &lance::Dataset) -> datafusion::common::Result<SchemaRef>`
  - `schema::Column` enum: `Variable(String)`, `Attribute(String)`; `schema::columns(schema: &Schema, dataset: &Dataset) -> Vec<Column>` in schema order.
  - `table::LanceNdFactory` (implements `TableProviderFactory`), `table::LanceNdTable { uri: String, dataset: Arc<Dataset>, schema: SchemaRef, columns: Arc<[Column]>, partitions: Option<usize> }` (implements `TableProvider`; `scan` returns `not_impl_err!` until Task 3).
  - `table::tables() -> Vec<TableType>` with one entry, `lance_nd`.
  - `lib.rs`: `beacon_plugin_sdk::export_plugin!(tables = table::tables);`

- [ ] **Step 1: Rename and strip the template.** Set the names from Global Constraints. Remove `[sql]` from the manifest (it is optional). Add deps: `lance-nd` (git, pinned rev), `lance = { version = "=11.0.0", default-features = false }`, `nd-arrow-array` (git, the rev in the `Cargo.lock` of lance-nd), `async-trait`, `futures`. Dev-deps: `tokio`, `tempfile = "3"`.
- [ ] **Step 2: Check one Arrow version.** Run `cargo tree -d -p lance-nd-native -e normal | grep arrow-array`. Expected: no output. If two Arrow versions show, pin the lance-nd rev that matches Lance 11 and Arrow 58.
- [ ] **Step 3: Write the fixture** in `native/src/fixture.rs` as specified above.
- [ ] **Step 4: Write the failing tests** in `native/src/table.rs`:
  - `create_infers_the_schema`: `CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '<uri>'`, then `ctx.table("t").schema()` has fields `pressure, temperature, x, y` (as `variables()` orders them) and the last field `id: Utf8`.
  - `missing_dataset_is_an_error`: `LOCATION '<tempdir>/none.lance'`. Error text contains `none.lance`.
  - `a_column_list_is_refused`: `CREATE EXTERNAL TABLE t (a INT) STORED AS lance_nd ...`. Error contains `infers its columns`.
  - `partitions_must_be_in_range`: `OPTIONS ('partitions' '0')`. Error contains `must be 1 to 512`.
  - In `schema.rs`, `attribute_clash_is_refused`: a fixture row with variable `id` and global attribute `id`. Error contains `` `id` ``.
- [ ] **Step 5: Run** `cargo test -p lance-nd-native`. Expected: the five tests FAIL.
- [ ] **Step 6: Implement** `table_schema`, `columns`, `LanceNdFactory::create` and `tables`. Open with `lance::Dataset::open(&cmd.location)`. Map `lance_nd::Error` and Lance errors to `DataFusionError::Plan` with the URI in the text. Read the attribute fields from the `__global_attrs__` struct of `Schema::from(dataset.schema())` (`nd_arrow_array::extension::GLOBAL_ATTRIBUTES_FIELD`).
- [ ] **Step 7: Run** `cargo test -p lance-nd-native`. Expected: PASS. Run `cargo clippy -p lance-nd-native -- -D warnings`. Expected: no warnings.
- [ ] **Step 8: Commit.** `git commit -m "Add lance_nd table type that infers its schema"`

### Task 2: Flatten one nd row

**Files:**
- Create: `native/src/flatten.rs`

**Interfaces:**
- Consumes: `schema::Column`.
- Produces: `flatten::flatten(row: &NdRecordBatch, schema: &SchemaRef, columns: &[Column], batch_size: usize) -> datafusion::common::Result<Vec<RecordBatch>>`. `schema` and `columns` are the **projected** ones. Each output batch has at most `batch_size` rows. An empty `columns` gives batches with zero columns and a row count (`RecordBatchOptions::with_row_count`).

- [ ] **Step 1: Write the failing tests** in `flatten.rs`. Build rows in code with `NdArrowArray::try_new` and `NdRecordBatch::try_new`; no Lance:
  - `broadcast_follows_c_order`: row 0 of the fixture, columns `[x, y, temperature]`. 12 rows. `x` = `[0,0,0,0,1,1,1,1,2,2,2,2]`, `y` = `[0,1,2,3]` three times, `temperature` = `0..12`.
  - `attribute_repeats`: row 0, columns `[id]`. 12 rows, each `"a"`.
  - `absent_variable_is_null`: row 1, columns `[pressure, temperature]`. 3 rows, `temperature` all null, `pressure` = `[1,2,3]`.
  - `no_columns_keeps_the_count`: row 0, columns `[]`. Total row count 12.
  - `batches_respect_batch_size`: row 0, `batch_size = 5`. Batch lengths `[5,5,2]`.
  - `row_without_variables_is_one_row`: a row with only `id = "z"`. 1 row.
- [ ] **Step 2: Run** `cargo test -p lance-nd-native flatten`. Expected: FAIL (module empty).
- [ ] **Step 3: Implement `flatten`.** The grid is `row.target()`. Variables: `nd_arrow_array::compute::shape::broadcast_to(array, row.target())`, then `values()`. Absent variable: `new_null_array(field.data_type(), n)`. Attribute: `ScalarValue::try_from_array(value, 0)?.to_array_of_size(n)`; absent key gives nulls. Cast to the field type when the row type differs (for example a narrower string type). Slice the full batch with `RecordBatch::slice`.
- [ ] **Step 4: Run** `cargo test -p lance-nd-native flatten`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Flatten nd rows onto their grid"`

### Task 3: Scan

**Files:**
- Modify: `native/src/table.rs`
- Create: `native/src/stream.rs`

**Interfaces:**
- Consumes: `flatten::flatten`, `LanceNdTable`.
- Produces: `stream::LanceNdPartition { uri: String, fragments: Vec<lance::table::format::Fragment>, filter: Option<String>, schema: SchemaRef, columns: Arc<[Column]> }` implementing `datafusion::physical_plan::streaming::PartitionStream`. `LanceNdTable::scan` returns `StreamingTableExec::try_new(projected_schema, partitions, None, [], false, limit)`.

- [ ] **Step 1: Write the failing tests** in `table.rs` (SQL over the fixture):
  - `count_star_counts_cells`: `SELECT count(*) FROM t` = 23.
  - `projection_keeps_the_full_grid`: `SELECT x FROM t WHERE id = 'a'` gives 12 rows.
  - `filter_on_a_variable`: `SELECT count(*) FROM t WHERE temperature >= 100` = 8.
  - `sum_over_rows`: `SELECT sum(pressure) FROM t` = 6.0.
  - `limit_stops_early`: `SELECT * FROM t LIMIT 5` gives 5 rows.
  - `partitions_give_the_same_result`: with `OPTIONS ('partitions' '4')`, `count(*)` = 23. Write the fixture as three appends first, so the table has three fragments.
- [ ] **Step 2: Run** `cargo test -p lance-nd-native table`. Expected: FAIL with `not implemented`.
- [ ] **Step 3: Implement the scan.** In `scan`, check out the latest version: `Dataset::open(&self.uri)`. Split `dataset.get_fragments()` round robin into `partitions.unwrap_or(state.config().target_partitions())` groups; skip empty groups; keep at least one partition. In `PartitionStream::execute`, use `futures::stream::once` around `lance_nd::scan(&dataset).fragments(..)` with `.filter(..)` when set, then `try_flatten`, and `flatten` each row with `ctx.session_config().batch_size()`. Do not call `project` on the scanner: the full row is needed for the grid. Map errors to `DataFusionError::External`.
- [ ] **Step 4: Run** `cargo test -p lance-nd-native`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Scan lance-nd rows as flat record batches"`

### Task 4: Filter pushdown on global attributes

**Files:**
- Create: `native/src/filter.rs`
- Modify: `native/src/table.rs` (`supports_filters_pushdown`, pass filters to `scan`)

**Interfaces:**
- Consumes: `schema::Column`.
- Produces: `filter::to_lance(expr: &Expr, columns: &[Column]) -> Option<String>`. `Some` only when every column in `expr` is a `Column::Attribute`. `supports_filters_pushdown` returns `Inexact` for those, `Unsupported` for the rest. `scan` joins the `Some` results with ` AND ` into `LanceNdPartition::filter`.

- [ ] **Step 1: Write the failing tests** in `filter.rs`:
  - `equality_maps_to_the_struct_field`: `col("id").eq(lit("a"))` gives `__global_attrs__.id = 'a'`.
  - `comparison_and_in_list`: `id >= 'b' AND id IN ('b','c')` gives `(__global_attrs__.id >= 'b') AND (__global_attrs__.id IN ('b', 'c'))`.
  - `variable_filter_is_not_pushed`: `col("temperature").gt(lit(5.0))` gives `None`.
  - `quote_in_literal_is_escaped`: `lit("o'brien")` gives `'o''brien'`.
  - `other_expressions_are_not_pushed`: `upper(id) = 'A'` gives `None`.
  - In `table.rs`, `pushed_filter_gives_the_same_rows`: `SELECT count(*) FROM t WHERE id IN ('a','c')` = 20, and `EXPLAIN` of the same query shows the filter text on the scan.
- [ ] **Step 2: Run** `cargo test -p lance-nd-native filter`. Expected: FAIL.
- [ ] **Step 3: Implement `to_lance`.** Support `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IN` list, `IS NULL`, `IS NOT NULL`. Literals: strings (quote doubled), integers, floats, booleans. Any other node gives `None`. Show the filter in `StreamingTableExec` display by a `LanceNdPartition` `Debug` that prints it.
- [ ] **Step 4: Run** `cargo test -p lance-nd-native`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Push global attribute filters into Lance"`

### Task 5: Package and document

**Files:**
- Modify: `README.md`, `scripts/package-plugin.py` (only if it names the old library), `.github/workflows/release.yml`

- [ ] **Step 1: Rewrite `README.md`** for this plugin: install line, the `CREATE EXTERNAL TABLE` example, the schema rules from "Design", the `partitions` option, and the known limits. ASD-STE100 style.
- [ ] **Step 2: Build the release library.** Run `cargo build --release -p lance-nd-native`. Expected: `native/target/release/lance_nd_native.dll` (Windows) exists.
- [ ] **Step 3: Package.** Run `python scripts/package-plugin.py --library native/target/release/lance_nd_native.dll --target x86_64-pc-windows-msvc` with the other arguments the script needs. Expected: an archive with `beacon-plugin.toml` and `lib/lance_nd_native.dll`, and no `sql/` folder.
- [ ] **Step 4: Smoke test in a local Beacon** (not on the `S:` disk). Install the archive, run `CREATE EXTERNAL TABLE t STORED AS lance_nd LOCATION '<fixture uri>'` and `SELECT count(*) FROM t`. Expected: the row count of that dataset. If no local Beacon is available, report this step as not done.
- [ ] **Step 5: Commit.** `git commit -m "Document the lance_nd table type"`
