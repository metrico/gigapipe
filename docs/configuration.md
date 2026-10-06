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
- **`OMIT_CREATE_TABLES`** - Skip table creation on startup (`true`, `false`)

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

The lifetimes, the `STORAGE_POLICY` and the move-to-disk rules of the retention policy are applied to the metric tables each time start-up initializes the database (every start in `all`, `writer` and `init_only` mode unless `OMIT_CREATE_TABLES` is set), as they are to the other tables.

A PromQL query is served whole from one tier. While its earliest read (the query start less its longest range, offset and the 5-minute lookback) lies inside raw's lifetime, it reads `metrics_5m` when every evaluation timestamp falls on the 5-minute grid, every range is a multiple of 5 minutes and no selector is left to the engine (an offset, an `@`, a subquery, a function the engine evaluates), and raw samples otherwise. Past raw's lifetime it reads the finest tier whose lifetime still covers that read, `metrics_1h` once past the 5m tier's: each evaluation timestamp is snapped down to the tier's grid and each range widened to whole buckets, so such a query lags by less than one bucket and `rate(x[1m])` reads as `rate(x[5m])`.

- **`METRICS_READ_TIER`** - Test knob for compliance runs, not for production: `raw`, `5m` or `1h` makes every PromQL query read that table, a tier read snapping timestamps and widening ranges as above whether or not the query is aligned (default: unset, the rules above pick the table). Also accepted with the `GIGAPIPE_` prefix; any other value fails start-up.

### Upgrading a deployment with metric history

On first start the metric tables are created and the metric history held in `samples_v3`, `time_series` and `metrics_15s` is copied into them in the background, by one instance in `all` or `writer` mode at a time (a lease in the `settings` table, taken over when its holder stops renewing it). Metric history appears as the copy proceeds, newest day first: minutes to an hour at `INSERT SELECT` speed over at most `SAMPLES_DAYS` of data. `metrics_15s` history beyond that is imported approximately: each 15-second row becomes one sample carrying its last value, exact at a 15-second scrape, and a faster scrape undercounts samples and sums. The labels of a series come from `time_series`, which expires with `SAMPLES_DAYS`, so the 15-second history of a series not seen within `SAMPLES_DAYS` is copied but cannot be queried. After the history, rows that instances still running the earlier release add to `samples_v3` are copied until none has arrived for an hour; the import then records itself complete and does not run again. Progress is recorded per day of raw history and per week of 15-second history, so a restart resumes where the copy stopped; a day or week interrupted part-way is deleted from the aggregate tiers and copied again. That deletion also drops the tier contribution of samples the new release accepted with timestamps in that day at or before the hour before the upgrade (a replayed backlog, a lagging sender); raw keeps them. The import runs only on single-node deployments; with `CLUSTER_NAME` set it is skipped. A deployment without metric history completes the import at once.

A stop-start upgrade loses nothing. A rolling upgrade can lose rows an instance of the earlier release delivers late, below what has already been copied, and can count twice in the aggregate tiers a remote-write batch both releases accepted. Metric rows in the old tables are never modified; they age out under their existing TTLs.

## Mode

- **`MODE`** - Operating mode:
  - `all` - Run both reader and writer (default)
  - `reader` - Run query/read endpoints only
  - `writer` - Run ingestion/write endpoints only
  - `init_only` - Initialize database and exit
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
