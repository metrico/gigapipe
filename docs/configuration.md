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

### Metrics on a cluster

With `CLUSTER_NAME` set, every metric table has a `Distributed` table over it, named with the `_dist` suffix:

| Table | Distributed table | Sharding key |
|---|---|---|
| `metric_samples_in` (staging, stores nothing) | `metric_samples_in_dist` | `fingerprint` |
| `metric_samples` | `metric_samples_dist` | `fingerprint` |
| `metrics_5m`, `metrics_1h` | `metrics_5m_dist`, `metrics_1h_dist` | `fingerprint` |
| `metric_series` | `metric_series_dist` | `fingerprint` |
| `metric_exemplars` | `metric_exemplars_dist` | `fingerprint` |
| `metric_metadata` | `metric_metadata_dist` | `cityHash64(name)` |
| `metric_label_names` | `metric_label_names_dist` | `rand()` |

Samples are written to `metric_samples_in_dist`. Each block lands on the shard of its series' fingerprint, and that shard's materialized views fill its raw samples and both aggregate tiers. The raw samples, tier buckets, series row and exemplars of a series are therefore all on one shard. PromQL reads the `_dist` tables, and each shard filters its own samples by its own `metric_series` rows. This holds only while the samples, tier, series and exemplar tables are all sharded by `fingerprint`, so the sharding keys above must not be changed. Each shard's view keeps the label names of its own series in `metric_label_names`, so one name has rows on several shards; `/api/v1/labels` without a selector merges them on the initiator. Its wrapper's `rand()` key places only rows inserted through it.

- **Sender affinity.** With several writers, every sample of one series must reach the same writer, for example through a load balancer that is sticky per sender. In-order remote write already needs this. Without it the aggregate tiers can miss pairs of samples, so `rate`, `increase`, `resets` and `changes` served from a tier can be wrong. Raw samples stay exact.

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

`MODE=migrate_metrics` copies the metric history of the legacy tables into the metric tables: metric rows of `samples_v3` into `metric_samples` and both tiers (`metrics_5m`, `metrics_1h`), `time_series` into `metric_series` and `metric_metadata`, and `metrics_15s` history older than `SAMPLES_DAYS` as approximate samples into both tiers only, never into `metric_samples`. Each run is incremental: it copies what earlier runs have not, prints a summary and exits. The legacy tables are only read; their rows age out by their TTL.

The steps below hold for a single ClickHouse server and for a cluster; [Cluster](#cluster) lists what differs on a cluster.

#### Single node

1. With the old release still serving all reads and writes, run the new release once with `MODE=migrate_metrics`. Its schema step only adds tables and views, so the old release keeps working beside it.
2. Swap the readers and writers to the new release.
3. Run `MODE=migrate_metrics` again, as many times as needed, until it prints `metric migration: no new data to migrate`. Each re-run copies again the last `MIGRATE_METRICS_LOOKBACK` below the previous run's top, which picks up what the old release wrote before the swap.

A run prints one summary line per database to stdout:

```
metric migration: <database>: <n> raw samples, <n> approximate 15s samples, <n> series, <n> metadata, <n> recovered units in <duration>
```

followed by `metric migration: no new data to migrate` when it migrated no raw or 15s samples and recovered no unit. Its exit code says what to do next:

| Exit | Printed | What to do |
|---|---|---|
| `0` | the summary of each database | Re-run until it prints `no new data to migrate`. |
| `1` | `metric migration: <database> failed at <step>: <error>; re-run MODE=migrate_metrics to resume` | Fix the cause the error names if any (memory, disk, connection) and re-run. Units a failed run left started are recovered by the next run. A tier short of its samples (`<tier> holds <n> samples …`) needs only a re-run; a unit that hits a limit (241, 159, 396) at `MIGRATE_METRICS_MAX_SPLIT` needs a larger limit or split (see [Units](#units-recovery-and-state)). A failed schema step exits 1 with `metric migration: schema step failed: <error>; re-run MODE=migrate_metrics to resume`. A run stopped by `SIGINT` or `SIGTERM` exits 1 as well, after killing its running queries; a signal during the schema step lets it finish and prints `metric migration: stopped by a signal before the migration started; re-run MODE=migrate_metrics to resume`. A second signal terminates the run at once. |
| `2` | `metric migration refused: <reason>` | No database was migrated. Except for the cluster and `metrics_15s` refusals below, the schema step did not run either. Fix the variable, setting or table the reason names and re-run. |
| `3` | `metric migration lease held by <instance>, renewed <time>, taken over in <duration>` | Another run holds the lease. Wait for it to finish, or for the lease to expire after the `taken over in` time if that run died, and re-run. |

Refusals (exit 2) name the variable and what it expects, for example `MIGRATE_METRICS_PAUSE="soon": expected a positive duration such as 30s, 5m or 1h`. A run is also refused for a `MIGRATE_METRICS_MAX_SPLIT` below `MIGRATE_METRICS_SPLIT` or a `METRICS_15S_TTL_DAYS` that is not a positive number of days. On a cluster, a run is also refused after the schema step, before migrating any database, when a legacy `_dist` table names a cluster ClickHouse does not define (see [Cluster](#cluster)).

A run is also refused after the schema step, before migrating any database, when the migrator user may not see `metrics_15s` (`metrics_15s_dist` on a cluster); see [migrator user](#a-clickhouse-user-for-the-migrator).

`OMIT_CREATE_TABLES` skips the schema step in this mode as in the others.

#### Cluster

With `CLUSTER_NAME` set, run `MODE=migrate_metrics` from any one node, with the same steps as on a single node. The run differs as follows:

- **Distributed reads and writes.** It reads `samples_v3_dist`, `time_series_dist` and `metrics_15s_dist`, and writes through `metric_samples_dist`, `metrics_5m_dist`, `metrics_1h_dist`, `metric_series_dist` and `metric_metadata_dist`. Its reads and writes, run-state records included, carry `insert_distributed_sync = 1` and `skip_unavailable_shards = 0`, so it waits for each shard's write and fails rather than skip a shard, and `load_balancing = 'in_order'` with `use_hedged_requests = 0`, so a unit writes to and reads from the same replica of each shard: the starting node itself for its own shard, otherwise the first in configuration order among those with the fewest errors.
- **One run state.** The lease and the run's records live in `settings_dist`, so the whole cluster has one run state and one lease, whichever node a run starts from. Query it through `settings_dist` in place of `settings`.
- **`ON CLUSTER` deletes and kills.** A unit deletes the tier buckets of its hour with `DELETE FROM metrics_5m ON CLUSTER` and `DELETE FROM metrics_1h ON CLUSTER` on the local tables, waiting for the mutation on every node. A stopped run kills its queries on the node it runs on, then with `KILL QUERY ON CLUSTER` on every node.
- **Every node is checked.** The back-off gate reads the signals of every node through `clusterAllReplicas` and waits while any one node is over its own threshold. Failing mutations are skipped on every node, and the back-off line names the table and the oldest unfinished mutation's id as on a single node. Without `GRANT SHOW TABLES ON *.*`, the back-off signals see only the granted tables (see [migrator user](#a-clickhouse-user-for-the-migrator)), on the starting node with a user and password per replica, and on every node with `<secret>`. `max_bytes_ratio_before_external_sort` is set only when every node lists it.
- **Legacy cluster refusal.** Before migrating any database, the run reads the cluster each database's legacy `_dist` tables name, expanding macros from `system.macros`. It refuses with exit code 2, naming the database and the table, when `samples_v3_dist` or `time_series_dist` does not exist, `metrics_15s` exists but `metrics_15s_dist` does not, a table is not `Distributed`, a macro is not in `system.macros`, or the cluster is not in `system.clusters`, for example `metric migration refused: database cloki: samples_v3_dist names cluster 'old', which is not in system.clusters` or `metric migration refused: database cloki: time_series_dist does not exist; a run on cluster 'c1' reads it`. A legacy `_dist` table the migrator user has no `SELECT` on is refused the same way, as one that does not exist: `metric migration refused: database cloki: metrics_15s_dist does not exist or is not visible to the migrator user; a run on cluster 'c1' reads metrics_15s through it`, or, when `metrics_15s` is not visible either, `metrics_15s_dist is not visible to the migrator user; grant it SELECT on metrics_15s_dist`. With `GRANT SHOW TABLES ON *.*`, a missing `SELECT` fails the first read instead (exit 1). Create or recreate the table over a defined cluster, or grant it, and re-run. Unless `OMIT_CREATE_TABLES` is set, the schema step has run by then; it only adds tables and views. Only an undefined legacy cluster is refused: legacy tables over a defined cluster other than `CLUSTER_NAME` are copied through as the old release reads them, and the run logs `metric migration: legacy cluster: '<cluster>' (CLUSTER_NAME '<name>')` before migrating.
- **Cluster totals.** The summary counts the whole cluster: raw samples from `count()` of `metric_samples_dist FINAL` on the hour before and after each unit, 15s samples from the unit's source count through `metrics_15s_dist`, series and metadata from `count()` through `metric_series_dist` and `metric_metadata_dist` with `FINAL` before and after each copy. There are no per-shard lines. On a cluster, `series` and `metadata` therefore count the series and metric families the copy added, including any live ingest added meanwhile, so a re-run prints about 0. On a single node they count the rows the copy wrote, existing ones included.
- **Partial writes.** A unit's `_dist` insert that fails after some shards wrote leaves the unit started, and the next run recovers it like any other started unit.

The [migrator user](#a-clickhouse-user-for-the-migrator) is created with its profile and grants `ON CLUSTER`, with `c1` replaced by `CLUSTER_NAME`:

```sql
CREATE SETTINGS PROFILE migrator_profile ON CLUSTER c1 SETTINGS readonly = 0;
CREATE USER migrator ON CLUSTER c1 IDENTIFIED BY '...' SETTINGS PROFILE 'migrator_profile';
GRANT ON CLUSTER c1 SELECT ON cloki.samples_v3 TO migrator;
GRANT ON CLUSTER c1 SELECT ON cloki.time_series TO migrator;
GRANT ON CLUSTER c1 SELECT ON cloki.metrics_15s TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metric_samples TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT, ALTER DELETE, ALTER UPDATE(_row_exists) ON cloki.metrics_5m TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT, ALTER DELETE, ALTER UPDATE(_row_exists) ON cloki.metrics_1h TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metric_series TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metric_metadata TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.settings TO migrator;
GRANT ON CLUSTER c1 SELECT ON cloki.samples_v3_dist TO migrator;
GRANT ON CLUSTER c1 SELECT ON cloki.time_series_dist TO migrator;
GRANT ON CLUSTER c1 SELECT ON cloki.metrics_15s_dist TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metric_samples_dist TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metrics_5m_dist TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metrics_1h_dist TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metric_series_dist TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.metric_metadata_dist TO migrator;
GRANT ON CLUSTER c1 SELECT, INSERT ON cloki.settings_dist TO migrator;
GRANT ON CLUSTER c1 REMOTE, CLUSTER, KILL QUERY ON *.* TO migrator;
GRANT ON CLUSTER c1 SELECT(metric, value) ON system.asynchronous_metrics TO migrator;
GRANT ON CLUSTER c1 SELECT(metric, value) ON system.metrics TO migrator;
GRANT ON CLUSTER c1 SELECT(name, value) ON system.merge_tree_settings TO migrator;
GRANT ON CLUSTER c1 SELECT(name, value) ON system.server_settings TO migrator;
GRANT ON CLUSTER c1 SELECT(database, table) ON system.merges TO migrator;
GRANT ON CLUSTER c1 SELECT(database, table, is_done, latest_fail_reason, mutation_id, create_time) ON system.mutations TO migrator;
GRANT ON CLUSTER c1 SELECT(database, table, active, partition_id) ON system.parts TO migrator;
GRANT ON CLUSTER c1 SELECT(query_id, user, query) ON system.processes TO migrator;
GRANT ON CLUSTER c1 SELECT(macro, substitution) ON system.macros TO migrator;
GRANT ON CLUSTER c1 SELECT(cluster) ON system.clusters TO migrator;
```

`SELECT` on `metric_series` and `metric_metadata` serves the cluster totals, and `INSERT` on them the `_dist` writes that land on the starting node's shard. `REMOTE` serves the `clusterAllReplicas` reads of the gate and the settings check, `CLUSTER` the `ON CLUSTER` deletes and kills, and `KILL QUERY` the `KILL QUERY ON CLUSTER`. `system.macros` and `system.clusters` serve the legacy cluster check. `ON CLUSTER` statements run under the DDL worker's settings, never the migrator's profile. Which nodes need the user and its grants depends on how the cluster authenticates its nodes:

- **User and password per replica.** The remote parts of `_dist` reads and inserts and of `clusterAllReplicas` run as the cluster-config user (`<user>` in `remote_servers`), which must hold `SELECT` and `INSERT` on the local tables above and the same `system.*` columns; the writers' user normally does. The migrator user is needed only on the node a run starts from; `ON CLUSTER` lets any node start one.
- **`<secret>`.** Every node runs the remote parts as the migrator user, so the profile, the user and every grant must be created `ON CLUSTER`. A user missing on a node fails the run with code 516, and a node whose `migrator_profile` is `readonly = 1` or `2` with code 164. A node that never starts a run needs only `SELECT, INSERT` on `settings`, `metric_samples`, `metric_series`, `metric_metadata`, `metrics_5m` and `metrics_1h`, `SELECT` on `samples_v3`, `time_series` and `metrics_15s`, and the `system.*` grants above other than `system.processes`, `system.macros` and `system.clusters`.

#### Migrator settings

Every variable is also accepted with the `GIGAPIPE_` prefix, which wins when both are set. An empty value counts as unset. Durations are Go durations (`30s`, `5m`, `1h`) and must be positive; counts are positive integers; sizes are plain bytes or a number with `KiB`, `MiB`, `GiB`, `TiB`, `KB`, `MB`, `GB` or `TB`; fractions lie in (0, 1]. The defaults are conservative, for a run beside live traffic.

| Variable | Default | Meaning |
|---|---|---|
| `MIGRATE_METRICS_LOOKBACK` | `1h` | How far below the previous run's top a re-run copies again |
| `MIGRATE_METRICS_MARGIN` | `5m` | Hours ending after now − margin are left to a later run |
| `MIGRATE_METRICS_SPLIT` | `1` | Parts a unit is first split into: `N` equal-width fingerprint ranges |
| `MIGRATE_METRICS_MAX_SPLIT` | `16` | Largest split a unit is retried at after a memory (241), time (159) or spill (396) limit error; at least `MIGRATE_METRICS_SPLIT` |
| `MIGRATE_METRICS_MAX_THREADS` | `2` | `max_threads` of each statement (`max_insert_threads` is `1`) |
| `MIGRATE_METRICS_MAX_MEMORY` | `2GiB` | `max_memory_usage` of each statement |
| `MIGRATE_METRICS_MAX_EXECUTION_TIME` | `5m` | `max_execution_time` of each statement |
| `MIGRATE_METRICS_EXTERNAL_SORT` | `512MiB` | `max_bytes_before_external_sort` |
| `MIGRATE_METRICS_EXTERNAL_GROUP_BY` | `512MiB` | `max_bytes_before_external_group_by` |
| `MIGRATE_METRICS_MAX_SPILL` | `20GiB` | `max_temporary_data_on_disk_size_for_query` |
| `MIGRATE_METRICS_PAUSE` | `1s` | Wait between units |
| `MIGRATE_METRICS_BACKOFF_PARTS` | `0.5` | Back off while the busiest partition of a written table holds at least this fraction of `parts_to_delay_insert` parts |
| `MIGRATE_METRICS_BACKOFF_MERGES` | `8` | Back off while more merges than this are running |
| `MIGRATE_METRICS_BACKOFF_MEMORY` | `0.8` | Back off while server memory use is at or above this fraction of its limit |

Before each statement the run waits while any back-off signal is over its threshold, or while a mutation that is not failing is unfinished or inserts are being delayed, reading the signals again every 10 seconds; every minute of waiting it logs `metric migration: back-off on <signal>: <value> at threshold <threshold>[, table <table>[, mutation <id>]], waited <duration>`. The parts and merges signals name their busiest table, and mutations the table and id of the oldest unfinished mutation; delayed inserts and memory name none. The wait has no deadline: a long healthy mutation holds the run, and `SIGTERM` stops it (exit 1, recovered by the next run). The client read timeout is the larger of `1h` and `MIGRATE_METRICS_MAX_EXECUTION_TIME` + 5 minutes.

#### A ClickHouse user for the migrator

The run uses the instance's `CLICKHOUSE_*` connection. To run it as a dedicated user, set that user on the migrate command only, for example `MODE=migrate_metrics CLICKHOUSE_AUTH=migrator:<password> OMIT_CREATE_TABLES=true`; the readers and writers keep theirs. The user cannot run the schema step, so run it first with the instance's user, for example once with `MODE=init_only`, which runs the schema step and exits. The user needs a settings profile with `readonly = 0`, because every statement carries its limits as query settings, and these grants:

```sql
CREATE SETTINGS PROFILE migrator_profile SETTINGS readonly = 0;
CREATE USER migrator IDENTIFIED BY '...' SETTINGS PROFILE 'migrator_profile';
GRANT SELECT ON cloki.samples_v3 TO migrator;
GRANT SELECT ON cloki.time_series TO migrator;
GRANT SELECT ON cloki.metrics_15s TO migrator;
GRANT SELECT, INSERT ON cloki.metric_samples TO migrator;
GRANT SELECT, INSERT, ALTER DELETE, ALTER UPDATE(_row_exists) ON cloki.metrics_5m TO migrator;
GRANT SELECT, INSERT, ALTER DELETE, ALTER UPDATE(_row_exists) ON cloki.metrics_1h TO migrator;
GRANT INSERT, SELECT(first_seen, last_seen, labels, `labels.keys`) ON cloki.metric_series TO migrator;
GRANT INSERT ON cloki.metric_metadata TO migrator;
GRANT SELECT, INSERT ON cloki.settings TO migrator;
GRANT SELECT(metric, value) ON system.asynchronous_metrics TO migrator;
GRANT SELECT(metric, value) ON system.metrics TO migrator;
GRANT SELECT(name, value) ON system.merge_tree_settings TO migrator;
GRANT SELECT(name, value) ON system.server_settings TO migrator;
GRANT SELECT(database, table) ON system.merges TO migrator;
GRANT SELECT(database, table, is_done, latest_fail_reason, mutation_id, create_time) ON system.mutations TO migrator;
GRANT SELECT(database, table, active, partition_id) ON system.parts TO migrator;
GRANT SELECT(query_id, user, query) ON system.processes TO migrator;
```

with `cloki` replaced by the database migrated. The column `SELECT` on `metric_series` serves the view `metric_label_names_mv`, which fires on the run's `metric_series` writes. `system.settings` and `system.tables` need no grant. The run kills only its own queries.

Without a grant on `metrics_15s` the run is refused (exit 2) after the schema step, before migrating any database, with `metric migration refused: database cloki: metrics_15s is not visible to the migrator user; grant it SELECT on metrics_15s`. With `GRANT SHOW TABLES ON *.*`, a missing `SELECT` fails the first read instead (exit 1).

The run turns off insert deduplication for its own statements; live writers' tables and settings are unchanged. A profile constraint that pins `insert_deduplicate` fails the run (exit 1) at its first statement, the settings check.

Without further grants the back-off signals see only the migrator's granted tables: merges and mutations on other tables do not hold the run, and a parts back-off whose busiest partition lies in another table logs `table not visible to this user`. `GRANT SHOW TABLES ON *.* TO migrator`, run by the operator, widens them to every table. It is optional, and it exposes table names, part and merge metadata and mutation commands, but no table data.

#### Units, recovery and state

A run holds a lease in the `settings` table while it works, renewing it every minute; the lease expires 3 minutes after its last renewal. It copies in units of one hour, `(H, H+1h]`, newest first:

- The first run records a floor, the hour of now − `SAMPLES_DAYS`. It copies every raw hour from the last hour boundary at or before now − `MIGRATE_METRICS_MARGIN` down to the floor, then every hour below the floor that has metric rows in `metrics_15s`, down to that table's lifetime (`METRICS_15S_TTL_DAYS`). Each run copies raw only above its raw edge, the hour of now − `METRICS_RAW_DAYS` plus 25 hours (at least the floor), so every raw row it copies outlives the run by a day; an hour between the floor and the edge that has `metrics_15s` metric rows starting in it is a 15s unit, recorded as `15s:<end>` (and its `raw:<end>` record ends `done` when an earlier run left that started), and any other is a raw unit. An hour below the edge done of either kind is not copied again. The floor is fixed by the first run: a later `SAMPLES_DAYS` change does not move it. Editing the `floor` record makes the next run rebuild those hours' tiers from `metrics_15s`, replacing exact tier data with approximate data, so leave it as recorded.
- A later run copies every hour not yet done, and again every hour ending after its previous top − `MIGRATE_METRICS_LOOKBACK`.

A unit is marked started, counts its raw rows, copies the legacy rows into `metric_samples`, and counts again; the difference is the rows it reports. An hour whose count did not change is marked done there, unless a failed run left it started. Otherwise the unit deletes that hour's buckets from `metrics_5m` and `metrics_1h` and rebuilds them from `metric_samples`, then checks them: each tier must count at least the hour's non-stale raw samples. A unit whose tiers pass is marked done. A 15s unit counts its `metrics_15s` rows; with none it is marked done, after deleting the hour's tier buckets when an earlier run left its `raw:<end>` record started; otherwise it deletes and rebuilds that hour's tier buckets from `metrics_15s`, never writing `metric_samples`, and checks them against the non-stale 15s samples the same way. A tier short of its samples fails the run (exit 1) with `<tier> holds <n> samples, <source> holds <n>; the hour stays started and the next run rebuilds it`, where the source is `raw` or `metrics_15s`. The same hour failing its check twice in a row is structural: report it rather than re-run. After the last unit the run copies the series index and metadata, then records its top. The series copy takes everything on the first run, then only `time_series` rows dated from the previous top − `MIGRATE_METRICS_LOOKBACK`; a run that fails before recording its top leaves the previous one in place.

A unit left started by a failed or stopped run is redone in full by the next one, which reports it among the recovered units. A copy or rebuild that hits a memory (code 241), time (159) or spill (396) limit is retried with the unit split into twice as many parts, up to `MIGRATE_METRICS_MAX_SPLIT`. A unit that still hits a limit there fails the run (exit 1) and logs `metric migration: <unit> hit <setting> (code <code>) at the maximum split <n>; giving up`.

The run logs each unit as `metric migration: <i>/<n> <kind> (<from>, <to>]: <rows> rows in <duration>`, each retry as `metric migration: <unit> hit <setting> (code <code>); retrying split <n>`, each back-off as described under [Migrator settings](#migrator-settings) and its end as `metric migration: back-off resumed after <duration>`, and a statement still running every minute as `metric migration: <query id> <op> <table> running <duration>: <n> rows read`, where the duration is the time since the unit started.

The run's state is kept in the `settings` table under `type = 'metric_migrate'`: `floor` and `watermark` (the previous top) in Unix milliseconds, `series` once the first series copy is done, `lease` as `<instance>:<unix seconds>`, and one record per unit, `raw:<end>` or `15s:<end>` in Unix milliseconds, valued `started` or `done`. To inspect it:

```sql
SELECT argMax(name, inserted_at) AS name, argMax(value, inserted_at) AS value
FROM settings WHERE type = 'metric_migrate' GROUP BY fingerprint ORDER BY name
```

#### Limits

- A legacy row that arrives below `watermark − MIGRATE_METRICS_LOOKBACK` after its hour was copied is not migrated. To cover a slow swap, raise `MIGRATE_METRICS_LOOKBACK` for the re-runs.
- A late live sample at or before now − `MIGRATE_METRICS_MARGIN` that arrives between a unit's tier delete and its rebuild is counted twice in the tiers.
- A sample that lands in a unit's range between its two raw counts is reported as migrated.
- History in `metrics_15s` beyond `SAMPLES_DAYS` is approximate: each 15-second row becomes one sample 1 ms after its start, carrying its last value, aggregated into the tiers. A 15s unit reads the rows starting in its hour, so each row reaches the tiers once: through the 15s unit of the hour it starts in, or that hour's raw unit. Where an hour's kind differs from the hour below it, the raw sample at exactly the hour's start is counted twice or not at all. Apart from that, it is exact at a 15-second scrape interval, except that a sample exactly on a bucket boundary lands in the bucket after; a faster scrape undercounts samples and sums.
- A series with no `time_series` labels left has its 15s history in the tiers but cannot be queried.
- With `METRICS_RAW_DAYS` above `SAMPLES_DAYS`, a non-aligned read inside the raw lifetime over a 15s hour finds nothing until the floor ages past the raw lifetime. Aligned reads use the 5m tier and see the history.
- With `METRICS_RAW_DAYS` equal to `SAMPLES_DAYS` (the default), the hours between the floor and the raw edge that are 15s units hold no raw rows, so for about a day after the first run a non-aligned read over the oldest day of the raw lifetime finds nothing; aligned reads use the 5m tier and see it.
- The tier check misses a late live sample lost in the two race windows above.
- On a merged day partition a raw unit reads the whole partition per statement; `FINAL PREWHERE` halves it and the range split divides it.

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

Set `GIGAPIPE_RULER_ENABLED` on exactly one instance. Instances do not coordinate: a second instance with the ruler enabled evaluates every rule again and double-counts recorded series in the aggregate tiers. The rule endpoints are served only where the ruler is enabled.

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

Metrics are read the same way: `metric_samples`, `metrics_5m`, `metrics_1h`, `metric_series`, `metric_exemplars`, `metric_metadata` and `metric_label_names` each need a table with the read suffix, sharded by the keys listed in [Metrics on a cluster](#metrics-on-a-cluster).

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
