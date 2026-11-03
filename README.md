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
SELECT time, lat, sst, "sst.units" FROM obs WHERE ".id" = 'a' AND sst > 290;
```

### Locations

`LOCATION` is the URI of the lance-nd table.

| Store | `LOCATION` |
| --- | --- |
| Local disk | `/data/obs.lance` or `file:///data/obs.lance`. A relative path is relative to the working directory of the Beacon server. |
| Amazon S3, or a store that is compatible with S3, such as MinIO | `s3://bucket/path/obs.lance` |
| Azure Blob Storage | `az://container/path/obs.lance` |
| Google Cloud Storage | `gs://bucket/path/obs.lance` |

Each `OPTIONS` key, except `partitions`, goes to Lance as a storage option.
If a key is not in `OPTIONS`, Lance reads it from the environment variables of the Beacon server: `AWS_*`, `AZURE_*` and `GOOGLE_*`.
Use environment variables for secrets. Then the keys are not in the SQL text.

Amazon S3, with the credentials in the environment:

```sql
CREATE EXTERNAL TABLE obs STORED AS lance_nd LOCATION 's3://my-bucket/obs.lance'
OPTIONS ('aws_region' 'eu-west-1');
```

MinIO, or another store that is compatible with S3:

```sql
CREATE EXTERNAL TABLE obs STORED AS lance_nd LOCATION 's3://my-bucket/obs.lance'
OPTIONS (
  'aws_endpoint' 'http://minio:9000',
  'aws_allow_http' 'true',
  'aws_region' 'us-east-1',
  'aws_access_key_id' 'minio',
  'aws_secret_access_key' 'minio-secret'
);
```

Azure Blob Storage:

```sql
CREATE EXTERNAL TABLE obs STORED AS lance_nd LOCATION 'az://my-container/obs.lance'
OPTIONS ('azure_storage_account_name' 'myaccount');
```

Google Cloud Storage, with a service account key file:

```sql
CREATE EXTERNAL TABLE obs STORED AS lance_nd LOCATION 'gs://my-bucket/obs.lance'
OPTIONS ('google_service_account' '/secrets/service-account.json');
```

Frequent keys:

| Store | Keys |
| --- | --- |
| Amazon S3 | `aws_region`, `aws_endpoint`, `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `aws_allow_http` |
| Azure | `azure_storage_account_name`, `azure_storage_account_key`, `azure_storage_sas_key` |
| Google Cloud Storage | `google_service_account` (a key file), `google_service_account_key` (the JSON key) |
| All stores | `timeout`, `connect_timeout`, `client_max_retries` |

See [object_store](https://docs.rs/object_store/0.13.2/object_store/) for all keys.

### Columns

The plugin reads the columns from the table. Do not give a column list.

| Column | Source | Value on each flat row |
| --- | --- | --- |
| One column for each variable | The `nd_arrow.array` columns of the table | The value of the variable at the cell. |
| `var.key` for each attribute of a variable, for example `sst.units` | The `attrs` of the variable | The value of the attribute. The value is the same on all flat rows of one lance-nd row. |
| `.key` for each global attribute, for example `.id` | The children of `__global_attrs__` | The value of the attribute. The value is the same on all flat rows of one lance-nd row. |

These names are the same as the names of the Beacon NetCDF, Zarr and HDF5 readers.
Put quotes around an attribute column in SQL: `"sst.units"` and `".id"`. Without quotes, SQL reads `sst.units` as column `units` of table `sst`.

Rules:

- The grid of a lance-nd row comes from all variables of that row. The column list of a query does not change the grid, so `SELECT x` and `SELECT x, sst` give the same number of rows.
- A variable that a row does not have is null on all flat rows of that row. Its attributes are null too.
- A row without variables gives one flat row.
- The plugin does not read `__stats__`.

### Examples

Select the values of a variable with its unit:

```sql
SELECT time, lat, lon, sst, "sst.units" FROM obs;
```

Select the rows of one lance-nd row, with a global attribute:

```sql
SELECT * FROM obs WHERE ".id" = 'a';
```

Show the units of each variable:

```sql
SELECT DISTINCT "sst.units", "temperature.units" FROM obs;
```

Count the cells of each dataset:

```sql
SELECT ".id", count(*) FROM obs GROUP BY ".id";
```

Filter on a variable attribute. The filter goes to Lance, so Lance reads only the rows with this unit:

```sql
SELECT avg(sst) FROM obs WHERE "sst.units" = 'K';
```

### Filters

A filter on attribute columns goes to Lance. Lance then reads only the rows that match.
These filters go to Lance: `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IN`, `IS NULL` and `IS NOT NULL`, with string, number and boolean values.
A filter on an attribute key named `true`, `false` or `null` runs in Beacon only.

A table can have stats: the minimum and maximum value of each variable in each lance-nd row.
lance-nd writes stats when you set `WriteOptions { stats: true, .. }` on a new table.
With stats, a filter on variables also goes to Lance. Lance then skips the lance-nd rows that have no matching value.
For example, `WHERE sst > 290` skips each lance-nd row with a maximum `sst` of 290 or lower.
Lance also skips a lance-nd row without the variable.
These variable filters use the stats: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`, `AND` and `OR`, with number and string values.
A table without stats reads all lance-nd rows.

Lance keeps or skips full lance-nd rows. Beacon applies all filters again on the flat rows, so the result is always correct.
Other filters run in Beacon only.

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
