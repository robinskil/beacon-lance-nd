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

### Shards

A wildcard in the last path segment of `LOCATION` reads many lance-nd tables as one table. Each matching table is a shard.

```sql
-- Each directory in /data/argo that ends in .lance is a shard.
CREATE EXTERNAL TABLE argo STORED AS lance_nd LOCATION '/data/argo/*.lance';

-- Object stores work the same. The storage options apply to the listing and to each shard.
CREATE EXTERNAL TABLE argo STORED AS lance_nd LOCATION 's3://bucket/argo/dac_*.lance'
OPTIONS ('aws_region' 'eu-west-1');

-- __shard is the last path segment of the shard.
SELECT "__shard", count(*) AS datasets FROM argo GROUP BY 1;

-- A filter on __shard alone selects shards. The scan does not open the other shards.
SELECT avg("TEMP") FROM argo
WHERE "__shard" IN ('aoml.lance', 'coriolis.lance') AND "PRES" < 100;
```

`*` matches any run of characters, also none. `?` matches one character.
Shards let each writer write its own table, for example one table for each data center, with no commit conflicts.

| Case | Behavior |
| --- | --- |
| `LOCATION` without `*` or `?` | One table. No `__shard` column. |
| `*` or `?` in a segment before the last | `CREATE` fails. A wildcard is allowed in the last path segment only. |
| No entry matches | `CREATE` fails. |
| An entry matches but is not a lance-nd table | `CREATE` fails and names that entry. |
| A column in some shards only | The column is in the table. It is null for the rows of the other shards. |
| Same column name with different types in two shards | `CREATE` fails and names the column, both shards and both types. With `widen_types`, the column gets one wider type when a wider type exists. See below. |
| A variable named `__shard` in a shard | `CREATE` fails. `__shard` is reserved in a sharded table. |
| A filter uses a column that a shard does not have | That filter does not go to Lance for that shard. Beacon applies it on the flat rows. |
| A shard is gone after `CREATE` | A query that reads the shard fails and names it. |
| `partitions` option | The plugin divides the fragments of all shards between the partitions. |

#### Different types in shards

By default, a column must have the same type in each shard. Set `widen_types` to read a column with different types as one wider type. The wider type holds each value with no loss.

```sql
CREATE EXTERNAL TABLE argo STORED AS lance_nd LOCATION '/data/argo/*.lance'
OPTIONS ('widen_types' 'true');
```

| Types in the shards | Type in the table |
| --- | --- |
| Two signed integers, or two unsigned integers | The wider type, for example `Int32` and `Int64` give `Int64`. |
| A signed and an unsigned integer | The smallest signed type that holds both, for example `UInt8` and `Int8` give `Int16`. `UInt64` with a signed type fails. |
| Two floats | The wider float, for example `Float32` and `Float64` give `Float64`. |
| An integer of at most 32 bits and a float | `Float64`. |
| `Utf8`, `LargeUtf8` and `Utf8View` | `Utf8`. |
| Timestamps with the same time zone and different units | The finer unit, for example seconds and nanoseconds give nanoseconds. |
| All other pairs, for example a 64-bit integer and a float, two time zones, or a number and a string | `CREATE` fails. |

The plugin casts each value to the table type when it reads it. Filters still go to Lance for each shard.
A nanosecond timestamp holds the years 1677 to 2262 only. A value in seconds outside that range gives an error when the plugin reads it.

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

- The variables of the query set the grid of each lance-nd row. The grid is the dimensions of one selected variable. The dimensions of each other selected variable must be in that grid. For example, `SELECT time, lat, sst` with `sst(time, lat)` gives one flat row for each value of `sst`, and `time` and `lat` repeat.
- If no selected variable holds the dimensions of all other selected variables, the query fails. For example, `PRES(N_PROF, N_LEVELS)` and `PARAMETER(N_PROF, N_CALIB, N_PARAM)` have no common grid. The error names the dimension sets of the selected variables. So `SELECT *` fails on a table with variables of different grids.
- An attribute column, for example `"sst.units"`, does not add the dimensions of its variable to the grid.
- A query without variables gives one flat row for each lance-nd row. So `SELECT count(*) FROM obs` counts the lance-nd rows.
- A selected variable that a row does not have is null on all flat rows of that row. Its attributes are null too.
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

Count the values of `sst` in each dataset:

```sql
SELECT ".id", count(sst) FROM obs GROUP BY ".id";
```

Average the temperature per 1° cell for one area, one time range and one pressure range:

```sql
SELECT floor("LATITUDE") AS lat, floor("LONGITUDE") AS lon, avg("TEMP") AS mean_temp
FROM argo
WHERE "LATITUDE" BETWEEN 20 AND 30 AND "LONGITUDE" BETWEEN -98 AND -80
  AND "JULD" >= TIMESTAMP '2010-01-01' AND "JULD" < TIMESTAMP '2016-01-01'
  AND "PRES" BETWEEN 0 AND 100 AND "TEMP_QC" = '1'
GROUP BY 1, 2;
```

Filter on a variable attribute. The filter goes to Lance, so Lance reads only the rows with this unit:

```sql
SELECT avg(sst) FROM obs WHERE "sst.units" = 'K';
```

### Filters

Beacon gives each filter to Lance when the filter uses only these parts:

- Columns: variables, attributes of variables and global attributes.
- Operators: `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `LIKE`, `IS NULL`, `IS NOT NULL` and `CAST`.
- Values: literals, for example strings, numbers, booleans and times.

A filter with other parts, for example a function or arithmetic, runs in Beacon only.

Lance keeps or skips full lance-nd rows:

- A part on attributes is exact. All flat rows of one lance-nd row have the same attribute values.
- A part on variables uses the stats of the table. Lance skips a lance-nd row when the stats show that no value of the row matches. Lance also skips a lance-nd row without the variable.
- A part on variables without a stats form keeps all lance-nd rows. A table without stats has no stats form.
- The parts combine with `AND`, `OR` and `NOT`, also when one filter has attributes and variables:

```sql
-- Lance reads the lance-nd rows of dataset 'a', and the lance-nd rows with a maximum sst above 290.
SELECT * FROM obs WHERE ".id" = 'a' OR sst > 290;
```

The stats are the minimum and maximum value of each variable in each lance-nd row.
lance-nd writes stats when you set `WriteOptions { stats: true, .. }` on a new table.
These variable filters use the stats: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN` and `IN`, with number, string and time values.

Beacon applies all filters again on the flat rows, so the result is always correct.

### Options

| Option | Default | Description |
| --- | --- | --- |
| `partitions` | The `target_partitions` of the session | The number of scan partitions, from 1 to 512. The plugin divides the Lance fragments between the partitions. |
| `widen_types` | `false` | `true` or `false`. With `true`, a column with different types in two shards gets one wider type. See [Different types in shards](#different-types-in-shards). |

```sql
CREATE EXTERNAL TABLE obs STORED AS lance_nd LOCATION '/data/obs.lance' OPTIONS ('partitions' '8');
```

### Table versions

Each query reads the latest version of the table.
`CREATE` reads the columns and the list of shards one time. To see new variables, attribute keys or shards, use `DROP TABLE` and `CREATE` again.

## Limits

- The plugin reads lance-nd tables. It does not write them. `INSERT INTO` and `COPY TO` are not available.
- The plugin keeps the selected variables of one lance-nd row in memory after it flattens the row. Large rows need much memory.
- Each query opens each selected shard. Thousands of shards on an object store make each query slower.
- A query reads the selected variables of each row. An attribute column of a variable, for example `"sst.units"`, also reads the values of that variable.

## Build and test

```bash
cargo test --manifest-path native/Cargo.toml
```

To release, set `plugin.version` in `beacon-plugin.toml`, commit and push a tag `v<version>`.
The release workflow builds the archives for Linux, macOS and Windows.

## License

Apache-2.0. See [LICENSE](LICENSE).
