# beacon-lance-nd

A [Beacon](https://github.com/maris-development/beacon) plugin that reads [lance-nd](https://github.com/robinskil/lance-nd) tables.
The plugin adds the table type `lance_nd`.
Beacon reads each lance-nd row as flat rows: one flat row for each cell of the row grid.

## Install the plugin

An admin installs the plugin from a GitHub release:

```sql
INSTALL lance_nd FROM 'github:robinskil/beacon-lance-nd@v0.1.0'
```

`BEACON_PLUGINS=lance_nd=github:robinskil/beacon-lance-nd@v0.1.0` installs the plugin at server start.
`SELECT * FROM beacon.system.plugins` shows the installed plugins. `UNINSTALL lance_nd` removes the plugin.

## Read a lance-nd table

```sql
CREATE EXTERNAL TABLE obs STORED AS lance_nd LOCATION '/data/obs.lance';
SELECT time, lat, sst FROM obs WHERE id = 'a' AND sst > 290;
```

`LOCATION` is the path of the lance-nd table, or a `file://` URI.
A relative path is relative to the working directory of the Beacon server.

### Columns

The plugin reads the columns from the table. Do not give a column list.

| Column | Source | Value on each flat row |
| --- | --- | --- |
| One column for each variable | The `nd_arrow.array` columns of the table | The value of the variable at the cell. |
| One column for each global attribute key | The children of `__global_attrs__` | The value of the attribute. The value is the same on all flat rows of one lance-nd row. |

Rules:

- The grid of a lance-nd row comes from all variables of that row. The column list of a query does not change the grid, so `SELECT x` and `SELECT x, sst` give the same number of rows.
- A variable that a row does not have is null on all flat rows of that row.
- A row without variables gives one flat row.
- `CREATE` fails when a global attribute key has the same name as a variable.
- The plugin does not read `__stats__` or the attributes of each variable.

### Filters

A filter on global attribute columns goes to Lance. Lance then reads only the rows that match.
These filters go to Lance: `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IN`, `IS NULL` and `IS NOT NULL`, with string, number and boolean values.
Beacon applies all filters again on the flat rows. Other filters, for example on variables, run in Beacon only.
A filter on an attribute key named `true`, `false` or `null` also runs in Beacon only.

### Options

| Option | Default | Description |
| --- | --- | --- |
| `partitions` | The `target_partitions` of the session | The number of scan partitions, from 1 to 512. The plugin divides the Lance fragments between the partitions. |

```sql
CREATE EXTERNAL TABLE obs STORED AS lance_nd LOCATION '/data/obs.lance' OPTIONS ('partitions' '8');
```

### Table versions

Each query reads the latest version of the table.
`CREATE` reads the columns one time. To see new variables or attribute keys, use `DROP TABLE` and `CREATE` again.

## Limits

- The plugin reads lance-nd tables. It does not write them. `INSERT INTO` and `COPY TO` are not available.
- The plugin reads local files only. Cloud object stores are not available.
- The plugin keeps one full lance-nd row in memory after it flattens the row. Large rows need much memory.
- A query reads all variables of a row to find its grid, also when the query uses fewer columns.

## Build and test

```bash
cargo test --manifest-path native/Cargo.toml
```

To release, set `plugin.version` in `beacon-plugin.toml`, commit and push a tag `v<version>`.
The release workflow builds the archives for Linux, macOS and Windows.

## License

Apache-2.0. See [LICENSE](LICENSE).
