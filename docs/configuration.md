# Configuration

## Docker Quickstart

Pull and run the latest image:

```bash
docker run -d \
  --name gigapipe \
  -p 3100:3100 \
  -e CLICKHOUSE_SERVER=clickhouse.example.com \
  -e CLICKHOUSE_AUTH=admin:password \
  ghcr.io/metrico/gigapipe:latest
```

The container image is published to `ghcr.io/metrico/gigapipe:latest` with multi-arch support (linux/amd64, linux/arm64). Gigapipe exposes port `3100` by default.

## ClickHouse Connection

- **`CLICKHOUSE_SERVER`** - ClickHouse server hostname or IP address (default: `localhost`)
- **`CLICKHOUSE_PORT`** - ClickHouse TCP port (default: `9000`)
- **`CLICKHOUSE_DB`** - Database name (default: `cloki`)
- **`CLICKHOUSE_AUTH`** - Authentication credentials in format `username:password`
- **`CLICKHOUSE_PROTO`** - Protocol to use: `http`, `https`, or `tls` (default: `http`)
- **`SELF_SIGNED_CERT`** - Skip TLS certificate verification for self-signed certificates (`true`, `false`)

## Cluster Configuration

- **`CLUSTER_NAME`** - Enables clustered mode and sets the cluster name. When set, gigapipe operates in distributed mode.
- **`CLICKHOUSE_READ_DIST_SUFFIX`** - Suffix for read-path distributed tables (default: `_dist`). Used for cross-cluster reads in multi-cluster deployments. See [Cross-Cluster Deployment](#cross-cluster-deployment) below.

## Environment variable names

Gigapipe's own variables are prefixed **`GIGAPIPE_`**. New deployments should
use that prefix.

The former `QRYN_` and `CLOKI_` prefixes are still accepted so existing
deployments keep working unchanged. At start-up each `GIGAPIPE_` variable is
applied to its legacy equivalent, so when both are set the `GIGAPIPE_` value is
the one that takes effect. Setting only a legacy name logs a deprecation
warning naming the variable to move to.

Variables that are not Gigapipe-specific — `CLICKHOUSE_*`, `PORT`, `HOST`,
`BULK_*` and so on — are unprefixed and unchanged.

## Authentication

- **`GIGAPIPE_LOGIN`** - Username for HTTP basic authentication
- **`GIGAPIPE_PASSWORD`** - Password for HTTP basic authentication
- **`QRYN_LOGIN`**, **`QRYN_PASSWORD`** - deprecated aliases
- **`CLOKI_LOGIN`**, **`CLOKI_PASSWORD`** - deprecated aliases

## HTTP Settings

- **`PORT`** - HTTP server port (default: `3100`)
- **`HOST`** - HTTP server bind address (default: `0.0.0.0`)
- **`CORS_ALLOW_ORIGIN`** - Enable CORS and set allowed origin (e.g., `https://example.com`)

## Write Settings

- **`BULK_MAX_SIZE_BYTES`** - Maximum batch size in bytes before flushing to ClickHouse
- **`BULK_MAX_AGE_MS`** - Maximum age in milliseconds before flushing batch (default: `100`)
- **`GIGAPIPE_SYSTEM_SETTINGS_OTLP_MAX_MESSAGE_SIZE`** - Maximum decompressed size in bytes of a single OTLP export request, applied to both the OTLP/HTTP body limit and the OTLP/gRPC max receive message size (default: `67108864`, i.e. 64 MiB). Also settable in the config file as `system_settings.otlp_max_message_size`.

## Advanced Settings

- **`ADVANCED_SAMPLES_ORDERING`** - ClickHouse `ORDER BY` clause for the `samples_v3` table (default: `timestamp_ns`). Applies **only when the table is first created** — setting it on a deployment that already has `samples_v3` has no effect, and the sort key cannot be changed by `ALTER`. The value is interpolated into DDL without validation. See [Table Ordering](table-ordering.md).
- **`ADVANCED_PROMETHEUS_MAX_SAMPLES`** - Maximum number of samples returned in Prometheus queries
- **`ADVANCED_OMIT_EMPTY_VALUES`** - Omit empty values in query results (`true`, `false`)
- **`OMIT_CREATE_TABLES`** - Skip the schema step (table creation and upgrade) on start in `all`, `writer`, `init_only` and `migrate_metrics` mode (`true`, `false`)

## Storage and Retention

- **`SAMPLES_DAYS`** - TTL in days for logs, traces and profiles, and the default lifetime of raw metric samples (default: `7`)
- **`STORAGE_POLICY`** - ClickHouse storage policy name for data placement
- **`METRICS_15S_TTL_DAYS`** - TTL in days for the `metrics_15s` table, the 15-second log rollup that LogQL `rate` and `count_over_time` over a plain stream selector read (default: the database's samples TTL). This sets when rollup rows are dropped; any move-to-disk rules from the samples retention policy still apply to the table unchanged, so a longer rollup TTL keeps rows past the point where the policy has already moved them to colder storage.

### Metric retention tiers

Metric samples are kept in three retention tiers, each a table with its own lifetime in whole days:

| Tier | Table | Resolution | Setting | Default |
|---|---|---|---|---|
| raw | `metric_samples` | as received | `METRICS_RAW_DAYS` | `SAMPLES_DAYS` |
| 5m | `metrics_5m` | 5 minutes | `METRICS_5M_DAYS` | the larger of `30` and the raw tier's days |
| 1h | `metrics_1h` | 1 hour | `METRICS_1H_DAYS` | the larger of `365` and the 5m tier's days |

- **`METRICS_RAW_DAYS`** - Lifetime in days of raw metric samples and their exemplars (default: `SAMPLES_DAYS`)
- **`METRICS_5M_DAYS`** - Lifetime in days of the 5-minute tier (default: the larger of `30` and the raw tier's days)
- **`METRICS_1H_DAYS`** - Lifetime in days of the 1-hour tier (default: the larger of `365` and the 5m tier's days). The series index lives as long as this tier, so every stored bucket keeps its labels.

All three are also accepted with the `GIGAPIPE_` prefix. Each value must be a positive whole number of days, and a coarser tier must live at least as long as a finer one: `METRICS_RAW_DAYS` ≤ `METRICS_5M_DAYS` ≤ `METRICS_1H_DAYS`. A default follows the finer tier up, so only values that are set can break the rule; start-up fails when they do. Every tier is always written; to keep no long history, give the coarser tiers the same lifetime as raw.

The lifetimes, the `STORAGE_POLICY` and the move-to-disk rules of the retention policy are applied to the metric tables each time start-up initializes the database (every start in `all`, `writer`, `init_only` and `migrate_metrics` mode unless `OMIT_CREATE_TABLES` is set), as they are to the other tables.

A PromQL query is served whole from one tier. While its earliest read (the query start less its longest range, offset and the 5-minute lookback) lies inside raw's lifetime, it reads `metrics_5m` when every evaluation timestamp falls on the 5-minute grid, every range is a multiple of 5 minutes and no selector is left to the engine (an offset, an `@`, a subquery, a function the engine evaluates), and raw samples otherwise. Past raw's lifetime it reads the finest tier whose lifetime still covers that read, `metrics_1h` once past the 5m tier's: each evaluation timestamp is snapped down to the tier's grid and each range widened to whole buckets, so such a query lags by less than one bucket and `rate(x[1m])` reads as `rate(x[5m])`.

- **`METRICS_READ_TIER`** - Test knob for compliance runs, not for production: `raw`, `5m` or `1h` makes every PromQL query read that table, a tier read snapping timestamps and widening ranges as above whether or not the query is aligned (default: unset, the rules above pick the table). Also accepted with the `GIGAPIPE_` prefix; any other value fails start-up.

### Upgrading a deployment with metric history

`MODE=migrate_metrics` copies the metric history of the legacy tables into the metric tables: metric rows of `samples_v3` into `metric_samples` and both tiers (`metrics_5m`, `metrics_1h`), `time_series` into `metric_series` and `metric_metadata`, and `metrics_15s` history older than `SAMPLES_DAYS` as approximate samples. Each run is incremental: it copies what earlier runs have not, prints a summary and exits. The legacy tables are only read; their rows age out by their TTL.

The steps below are for a single ClickHouse server. A database with `CLUSTER_NAME` set is refused with exit code 2.

#### Single node

1. With the old release still serving all reads and writes, run the new release once with `MODE=migrate_metrics`. Its schema step only adds tables and views, so the old release keeps working beside it.
2. Swap the readers and writers to the new release.
3. Run `MODE=migrate_metrics` again, as many times as needed, until it prints `metric migration: no new data to migrate`. Each re-run copies again the last `MIGRATE_METRICS_LOOKBACK` below the previous run's top, which picks up what the old release wrote before the swap.

A run prints one summary line per database to stdout:

```
metric migration: <database>: <n> raw samples, <n> approximate 15s samples, <n> series, <n> metadata, <n> recovered units in <duration>
```

followed by `metric migration: no new data to migrate` when it copied no raw or 15s samples and recovered no unit. Its exit code says what to do next:

| Exit | Printed | What to do |
|---|---|---|
| `0` | the summary of each database | Re-run until it prints `no new data to migrate`. |
| `1` | `metric migration: <database> failed at <step>: <error>; re-run MODE=migrate_metrics to resume` | Fix the cause the error names if any (memory, disk, connection) and re-run. Units a failed run left started are recovered by the next run. A failed schema step exits 1 with `metric migration: schema step failed: <error>; re-run MODE=migrate_metrics to resume`. A run stopped by `SIGINT` or `SIGTERM` exits 1 as well, after killing its running queries; a signal during the schema step lets it finish and prints `metric migration: stopped by a signal before the migration started; re-run MODE=migrate_metrics to resume`. A second signal terminates the run at once. |
| `2` | `metric migration refused: <reason>` | Nothing was written, and the schema step did not run. Fix the variable or setting the reason names and re-run. |
| `3` | `metric migration lease held by <instance>, renewed <time>, taken over in <duration>` | Another run holds the lease. Wait for it to finish, or for the lease to expire after the `taken over in` time if that run died, and re-run. |

Refusals (exit 2) name the variable and what it expects, for example `MIGRATE_METRICS_PAUSE="soon": expected a positive duration such as 30s, 5m or 1h`. A run is also refused for a `MIGRATE_METRICS_MAX_SPLIT` below `MIGRATE_METRICS_SPLIT`, a `MIGRATE_METRICS_DSN` naming another database than the one migrated, a `METRICS_15S_TTL_DAYS` that is not a positive number of days, or a database with `CLUSTER_NAME` set.

`OMIT_CREATE_TABLES` skips the schema step in this mode as in the others.

#### Migrator settings

Every variable is also accepted with the `GIGAPIPE_` prefix, which wins when both are set. An empty value counts as unset. Durations are Go durations (`30s`, `5m`, `1h`) and must be positive; counts are positive integers; sizes are plain bytes or a number with `KiB`, `MiB`, `GiB`, `TiB`, `KB`, `MB`, `GB` or `TB`; fractions lie in (0, 1]. The defaults are conservative, for a run beside live traffic.

| Variable | Default | Meaning |
|---|---|---|
| `MIGRATE_METRICS_LOOKBACK` | `1h` | How far below the previous run's top a re-run copies again |
| `MIGRATE_METRICS_MARGIN` | `5m` | Hours ending after now − margin are left to a later run |
| `MIGRATE_METRICS_SPLIT` | `1` | Parts a unit is first split into, by `cityHash64(fingerprint) % N` |
| `MIGRATE_METRICS_MAX_SPLIT` | `64` | Largest split a unit is retried at after a memory (241), time (159) or spill (307) limit error; at least `MIGRATE_METRICS_SPLIT` |
| `MIGRATE_METRICS_MAX_THREADS` | `2` | `max_threads` of each statement (`max_insert_threads` is `1`) |
| `MIGRATE_METRICS_MAX_MEMORY` | `2GiB` | `max_memory_usage` of each statement |
| `MIGRATE_METRICS_MAX_EXECUTION_TIME` | `30m` | `max_execution_time` of each statement |
| `MIGRATE_METRICS_EXTERNAL_SORT` | `512MiB` | `max_bytes_before_external_sort` |
| `MIGRATE_METRICS_EXTERNAL_GROUP_BY` | `512MiB` | `max_bytes_before_external_group_by` |
| `MIGRATE_METRICS_MAX_SPILL` | `20GiB` | `max_temporary_data_on_disk_size_for_query` |
| `MIGRATE_METRICS_PAUSE` | `1s` | Wait between units |
| `MIGRATE_METRICS_BACKOFF_PARTS` | `0.5` | Back off while the busiest partition of a written table holds at least this fraction of `parts_to_delay_insert` parts |
| `MIGRATE_METRICS_BACKOFF_MERGES` | `16` | Back off while more merges than this are running |
| `MIGRATE_METRICS_BACKOFF_MEMORY` | `0.8` | Back off while server memory use is at or above this fraction of its limit |
| `MIGRATE_METRICS_DSN` | unset | The migrator's own ClickHouse DSN, such as `clickhouse://migrator:password@host:9000`; unset, it uses the instance's connection. It may name no database or the one migrated. |

Before each statement the run waits while any back-off signal is over its threshold, or while a mutation is unfinished or inserts are being delayed, reading the signals again every 10 seconds; every minute of waiting it logs `metric migration: back-off on <signal>: <value> at threshold <threshold>, table <table>, waited <duration>`. The client read timeout is the larger of `1h` and `MIGRATE_METRICS_MAX_EXECUTION_TIME` + 5 minutes, unless the DSN sets `read_timeout`.

#### A ClickHouse user for the migrator

The schema step uses the instance's connection; `MIGRATE_METRICS_DSN` lets the copy run as its own user under its own quotas. That user needs a settings profile with `readonly = 0`, because every statement carries its limits as query settings, and these grants:

```sql
CREATE SETTINGS PROFILE migrator_profile SETTINGS readonly = 0;
CREATE USER migrator IDENTIFIED BY '...' SETTINGS PROFILE 'migrator_profile';
GRANT SELECT ON cloki.samples_v3 TO migrator;
GRANT SELECT ON cloki.time_series TO migrator;
GRANT SELECT ON cloki.metrics_15s TO migrator;
GRANT SELECT, INSERT ON cloki.metric_samples TO migrator;
GRANT SELECT, INSERT, ALTER DELETE ON cloki.metrics_5m TO migrator;
GRANT SELECT, INSERT, ALTER DELETE ON cloki.metrics_1h TO migrator;
GRANT INSERT ON cloki.metric_series TO migrator;
GRANT INSERT ON cloki.metric_metadata TO migrator;
GRANT SELECT, INSERT ON cloki.settings TO migrator;
```

with `cloki` replaced by the database migrated. The run also reads `system.settings`, `system.server_settings`, `system.merge_tree_settings`, `system.parts`, `system.merges`, `system.mutations`, `system.metrics`, `system.asynchronous_metrics` and `system.processes`, and kills only its own queries.

#### Units, recovery and state

A run holds a lease in the `settings` table while it works, renewing it every minute; the lease expires 3 minutes after its last renewal. It copies in units of one hour, `(H, H+1h]`, newest first:

- The first run records a floor, the hour of now − `SAMPLES_DAYS`. It copies every raw hour from the last hour boundary at or before now − `MIGRATE_METRICS_MARGIN` down to the floor, then every hour below the floor that has metric rows in `metrics_15s`, down to that table's lifetime (`METRICS_15S_TTL_DAYS`).
- A later run copies every hour not yet done, and again every hour ending after its previous top − `MIGRATE_METRICS_LOOKBACK`.

A unit is marked started, counts its raw rows, copies the legacy rows into `metric_samples`, and counts again; the difference is the rows it reports. An hour whose count did not change is marked done there. Otherwise the unit deletes that hour's buckets from `metrics_5m` and `metrics_1h` and rebuilds them from `metric_samples`, then is marked done. After the last unit the run copies the series index and metadata, then records its top. The series copy takes everything on the first run, then only `time_series` rows dated from the previous top − `MIGRATE_METRICS_LOOKBACK`; a run that fails before recording its top leaves the previous one in place.

A unit left started by a failed or stopped run is redone in full by the next one, which reports it among the recovered units. A copy or rebuild that hits a memory, time or spill limit is retried with the unit split into twice as many parts, up to `MIGRATE_METRICS_MAX_SPLIT`. A unit that still hits a limit there fails the run (exit 1) and logs `metric migration: <unit> hit <setting> (code <code>) at the maximum split <n>; giving up`.

The run logs each unit as `metric migration: <i>/<n> <kind> (<from>, <to>]: <rows> rows in <duration>`, each retry as `metric migration: <unit> hit <setting> (code <code>); retrying split <n>`, each back-off as `metric migration: back-off on <signal>: <value> at threshold <threshold>, table <table>` and `metric migration: back-off resumed after <duration>`, and a statement still running every minute as `metric migration: <query id> <op> <table> running <duration>: <n> rows read`, where the duration is the time since the unit started.

The run's state is kept in the `settings` table under `type = 'metric_migrate'`: `floor` and `watermark` (the previous top) in Unix milliseconds, `series` once the first series copy is done, `lease` as `<instance>:<unix seconds>`, and one record per unit, `raw:<end>` or `15s:<end>` in Unix milliseconds, valued `started` or `done`. To inspect it:

```sql
SELECT argMax(name, inserted_at) AS name, argMax(value, inserted_at) AS value
FROM settings WHERE type = 'metric_migrate' GROUP BY fingerprint ORDER BY name
```

#### Limits

- A legacy row that arrives below `watermark − MIGRATE_METRICS_LOOKBACK` after its hour was copied is not migrated. To cover a slow swap, raise `MIGRATE_METRICS_LOOKBACK` for the re-runs.
- A late live sample at or before now − `MIGRATE_METRICS_MARGIN` that arrives between a unit's tier delete and its rebuild is counted twice in the tiers.
- A sample that lands in a unit's range between its two raw counts is reported as migrated.
- History in `metrics_15s` beyond `SAMPLES_DAYS` is approximate: each 15-second row becomes one sample at its start, carrying its last value. That is exact at a 15-second scrape interval; a faster scrape undercounts samples and sums.
- A series with no `time_series` labels left has its 15s history copied but cannot be queried.

## Mode

- **`MODE`** - Operating mode:
  - `all` - Run both reader and writer (default)
  - `reader` - Run query/read endpoints only
  - `writer` - Run ingestion/write endpoints only
  - `init_only` - Initialize database and exit
  - `migrate_metrics` - Initialize database, run one incremental metric migration and exit (see [Upgrading a deployment with metric history](#upgrading-a-deployment-with-metric-history))
- **`READONLY`** - Set to `true` to force reader mode (equivalent to `MODE=reader`)

## Logging

- **`LOG_LEVEL`** - Log level: `debug`, `info`, `warn`, `error`

## Log Drilldown

- **`LOG_DRILLDOWN`** - Enable log pattern detection and drilldown features (`true`, `false`)
- **`LOG_PATTERN_SIMILARITY`** - Similarity threshold for pattern grouping, range 0-1 (default: `0.7`). Higher values require more similarity.
- **`LOG_PATTERN_READ_LIMIT`** - Maximum number of log patterns to read per request (default: `300`)

## Recording Rules (Ruler)

The ruler evaluates LogQL and PromQL recording rules on a schedule and writes
the results back as new series. It is single-tenant and recording-only
(alerting rules may be stored but are never evaluated). It runs only in modes
`all`/`""`, after the writer and reader initialize.

- **`GIGAPIPE_RULER_ENABLED`** - Enable the ruler (`1`, `true`, `yes`, `on`; default: disabled). When disabled, the rule endpoints (`/api/v1/rules`, `/loki/api/v1/rules`, `/api/prom/rules`) are **not** served and return `404`.
- **`GIGAPIPE_RULER_POLL_INTERVAL`** - How often rule groups are reloaded from storage and rescheduled, as a Go duration (e.g. `15s`, `1m`; default: `30s`).
- **`GIGAPIPE_RULER_MAX_LOGQL_RESULT_BYTES`** - Maximum size, in bytes, of a single LogQL recording-rule result buffered before parsing; a rule exceeding it fails that evaluation (default: `10485760`, i.e. 10 MiB).

## Self-Profiling

- **`PYROSCOPE_SERVER_ADDRESS`** - Pyroscope server URL (e.g., `http://pyroscope:4040`)
- **`PYROSCOPE_APPLICATION_NAME`** - Application name in Pyroscope (default: `gigapipe`)

## Cross-Cluster Deployment

For multi-region or multi-cluster deployments, you can separate write and read paths using different distributed table configurations.

### Use Case

Use cross-cluster reads when you need to:
- Query data across multiple ClickHouse clusters
- Implement multi-region deployments with centralized querying
- Separate read and write workloads

### Architecture

Writes use local distributed tables (default `_dist` suffix) that target the local cluster. Reads can use tables with a custom suffix (configured via `CLICKHOUSE_READ_DIST_SUFFIX`) that target multiple clusters.

### Configuration Example

```bash
# Writer instances (local cluster writes)
MODE=writer
CLUSTER_NAME=local_cluster
CLICKHOUSE_SERVER=clickhouse-local.example.com

# Reader instances (cross-cluster reads)
MODE=reader
CLUSTER_NAME=local_cluster
CLICKHOUSE_SERVER=clickhouse-local.example.com
CLICKHOUSE_READ_DIST_SUFFIX=_dist_cross_cluster
```

In this setup:
- Write operations use `table_name_dist` (local cluster only)
- Read operations use `table_name_dist_cross_cluster` (can span multiple clusters)

### ClickHouse Setup

```sql
CREATE TABLE IF NOT EXISTS samples_v3_dist_cross_cluster ON CLUSTER '{cluster}'
AS samples_v3
ENGINE = Distributed(
  'cross_cluster_name',
  currentDatabase(),
  'samples_v3',
  rand()
)
SETTINGS skip_unavailable_shards = 1;
```

`skip_unavailable_shards=1` ensures queries continue even if some shards are temporarily unavailable.

### Backward Compatibility

Without setting `CLICKHOUSE_READ_DIST_SUFFIX`, gigapipe uses the default `_dist` suffix for both reads and writes, maintaining backward compatibility with existing deployments.

## Example Configuration

Minimal single-node setup:

```bash
CLICKHOUSE_SERVER=clickhouse.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=gigapipe
CLICKHOUSE_AUTH=admin:password
PORT=3100
```

Production clustered setup:

```bash
CLICKHOUSE_SERVER=clickhouse.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=gigapipe
CLICKHOUSE_AUTH=admin:password
CLUSTER_NAME=production_cluster
STORAGE_POLICY=tiered_storage
SAMPLES_DAYS=30
BULK_MAX_AGE_MS=100
PORT=3100
QRYN_LOGIN=admin
QRYN_PASSWORD=secure_password
LOG_LEVEL=info
```
