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
- **`CLICKHOUSE_READ_CLUSTER`** - Cluster the read-path distributed tables span. It applies only when it differs from `CLUSTER_NAME` and `CLICKHOUSE_READ_DIST_SUFFIX` is set to a suffix other than `_dist`. Set it on readers too: TraceQL structural queries and `compare()` then run on each shard of this cluster, else of `CLUSTER_NAME` (see [TraceQL](traceql.md#requirements)).

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
- **`COMPAT_4_0_19`** - Enable compatibility mode for v4.0.19 behavior (`true`, `false`)

## Storage and Retention

- **`SAMPLES_DAYS`** - TTL in days for stored samples (default: `7`)
- **`STORAGE_POLICY`** - ClickHouse storage policy name for data placement
- **`METRICS_15S_ENABLED`** - Aggregate metric samples into the `metrics_15s` rollup table (default: `true`). Set to `false` to stop the aggregation and delete the stored metric rollups; metric queries are then served from raw samples. Log queries keep using the table either way. At 15-second scrape resolution the rollup stores roughly one row per raw sample at several times the disk cost, so disabling it trades PromQL query speed on large windows for disk space. Re-enabling resumes aggregation from that moment: queries reaching back into the disabled period keep using raw samples.

  > **Warning:** disabling deletes the stored metric rollups permanently, and the delete cannot be undone by re-enabling. Metric history survives only where `samples_v3` still holds it, so check the raw retention first: if `samples_v3` has been given a shorter TTL than `metrics_15s` (`METRICS_15S_TTL_DAYS`, or a hand-edited TTL), the rollup is the only remaining copy of the older metric data and disabling destroys it.

- **`METRICS_15S_TTL_DAYS`** - TTL in days for the `metrics_15s` rollup table (default: the database's samples TTL). This sets when rollup rows are dropped; any move-to-disk rules from the samples retention policy still apply to the table unchanged, so a longer rollup TTL keeps rows past the point where the policy has already moved them to colder storage.
  - LogQL `rate` and `count_over_time` with no parser, `keep`, `drop`, `unwrap` or non-empty line filter read `metrics_15s` when every evaluation time, the range and the offset are multiples of 15 seconds, so that the window at `T` counts Loki's `(T-R, T]`; otherwise they read every raw line.
  - The schema upgrade records a cutoff: a 15-second cell boundary at least a minute after the later of the upgrade and the newest stored cell (on a fresh install, the start of time). For lines before the cutoff, these queries also read the raw lines exactly on a window edge, so those windows need the raw retention too. Keep both TTLs equal while such windows are queried: where one table holds lines the other lacks (past either TTL, or before `metrics_15s` was enabled), these counts are wrong. From the cutoff on, `metrics_15s` alone serves them.
  - Unless the query is a `sum` over them, these read `metrics_15s` per stream and hold one value per stream and evaluation time while they run (`max`, `min`, `count`, `avg`, `stddev` or `stdvar` by, `topk`, or no aggregation). Operators cap that in the ClickHouse server profile: `max_memory_usage` limits a query, and `max_bytes_before_external_group_by` and `max_bytes_before_external_sort` let its aggregations and sorts spill to disk.

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

## TraceQL

- **`GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS`** - Number of rounds used to resolve span ancestry for TraceQL structural operators, an integer from `1` to `32` (default: `20`). With `R` rounds, traces up to `2^R` spans deep resolve. Spans of deeper traces, and of traces whose parent links form a cycle, are treated as unreachable, and a warning names those traces. Each round adds work to every query using `>>` or `<<`. An invalid value stops Gigapipe at start-up. See [Depth limit](#depth-limit).
- **`GIGAPIPE_TRACEQL_PORTION_SPANS`** - Most spans a TraceQL search or tag autocomplete reads per shard in one portion, a positive integer (default: `2500000`). A request runs in `ceil(estimate / setting)` portions. Queries with a structural operator estimate every span in range on the fullest shard, a single node being one shard; a lone `{}` estimates the search limit; others estimate the spans their selectors match across the cluster. Memory grows with spans per portion, and every portion rescans the range, so a lower value costs time. Metrics queries are not portioned. Set ClickHouse `max_memory_usage` as the backstop. An invalid value stops Gigapipe at start-up. See [Large queries](#large-queries).
- **`GIGAPIPE_TRACEQL_SEARCH_MAX_DURATION`** - Longest time range of a TraceQL search or tag autocomplete, a Go duration such as `168h` (default: `168h`). A longer request gets HTTP 400 with the plain-text body `range specified by start and end exceeds 168h0m0s. received start=<s> end=<e>`, as Tempo sends it. `0` turns the cap off. Applies to `/api/search` with `q`, and to `/api/v2/search/tags` and `/api/v2/search/tag/{tag}/values` with a non-zero `start`. Metrics queries, `/api/search` with `tags`, and tag lookups that read no time range are not capped. An invalid or negative value stops Gigapipe at start-up. See [Large queries](#large-queries).

Structural TraceQL queries need ClickHouse 24.9 or newer; see [Requirements](#requirements).

### Requirements

Queries that use a structural operator (`>`, `>>`, `<`, `<<`, `~` and their
`!`/`&` variants) need **ClickHouse 24.9 or newer**. On an older server they
fail with `TraceQL structural operators need ClickHouse 24.9 or newer`. All
other TraceQL queries keep working on older servers.

Structural queries need no schema migration, writer change or backfill: the
span tree is resolved at query time from the `parent_id` of the stored spans.
Clustered (Distributed) deployments return the same results as single-node
ones, provided **a trace's spans and attribute rows live on one shard**.
Writers inserting through the `_dist` tables place them so: the sharding key is
`sipHash64(oid, trace_id)`, and a trace has one `oid`.

The nested-set intrinsics read values that a background numberer stores, which
needs ClickHouse 24.9 or newer; see [The numberer](#the-numberer).

### Depth limit

The span tree is built at query time from stored `parent_id`s:

- A span takes part in a structural relation only when its parent chain
  reaches a root span (a span without a parent).
- A span that can't reach a root, such as one in an orphan subtree whose
  parent was never ingested or in a trace with no root, never matches a
  positive or `&` operator, and always matches a `!` operator.
- A span whose ancestor hasn't been ingested yet starts matching once that
  ancestor arrives.
- When a trace has several roots, they are all siblings under `~`.

Ancestry is resolved in a fixed number of rounds, set by
`GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS`. With `R` rounds, traces up to `2^R` spans
deep resolve. The default of 20 covers traces about a million spans deep.

A span whose ancestry is still unresolved after the last round is either in
a trace deeper than the limit or has parent links that form a cycle. The
query doesn't fail. Instead:

- Under `>>`, `!>>` and the one-hop operators (`>`, `<`, `~` and their
  variants), unresolved spans are treated like orphans. The rest of the trace
  is evaluated normally.
- Under `<<`, `!<<`, `&>>` and `&<<`, the whole trace is skipped and returns
  no spans from that operator, including under `!`.

Only traces that are candidates for the operator are affected: traces with
spans on both sides of a `>>`/`<<` operator or its variants, and, for the
one-hop operators, traces that have a root and also a span whose parent is
missing.

At most once a minute, gigapipe logs a warning that names the affected
traces:

```
TraceQL structural query: N trace(s) have unresolved ancestry (deeper than 2^R spans at GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS=R, or parent links form a cycle); their unresolved spans are treated as unreachable; trace_ids=[…]
```

The line lists up to 20 trace IDs. Raising the round limit helps only with
deep traces, not cycles.

### The numberer

The nested-set numberer computes `nestedSetLeft`, `nestedSetRight` and `nestedSetParent` for each span after ingestion and stores them in the `tempo_traces_nested_set` table; see [Nested-set intrinsics](../README.md#nested-set-intrinsics). There is nothing to enable. In `all` or `writer` mode an invalid value of the settings below stops Gigapipe at start-up; a `reader` ignores them.

- **`GIGAPIPE_NESTED_SET_NUMBERER_IDLE`** - How long a trace must receive no spans before it is numbered, as a Go duration (default: `10s`).
- **`GIGAPIPE_NESTED_SET_NUMBERER_INTERVAL`** - Time between numbering passes, as a Go duration (default: `5s`).
- **`GIGAPIPE_NESTED_SET_NUMBERER_MAX_SPANS`** - Maximum number of spans one pass numbers, a positive integer (default: `200000`). The oldest traces go first and the rest wait for the next pass; a single larger trace is numbered alone. On a cluster each shard numbers its own traces and the cap applies per shard.

- **Who takes part.** Every instance in `all` or `writer` mode takes part, and ClickHouse elects one of them to run the passes through a lease in the `tempo_traces_nested_set_lease` table. If the leader stops, or its passes keep failing, another takes over within 4 × `GIGAPIPE_NESTED_SET_NUMBERER_INTERVAL`, so values can lag that much longer. `reader` instances never take part.
- **Clocks.** Each instance judges the lease by the clock of the ClickHouse node it connects to, so on a cluster node clocks must agree to well within 3 × the interval. On a replicated cluster they must also agree within 2s and well within the idle period. NTP is enough.
- **ClickHouse version.** It needs ClickHouse 24.9 or newer. On an older server it logs an error and doesn't start, and every span reads `0`/`0`/`0`.
- **When values appear.** A trace is numbered once it has received no spans for the idle period. With the defaults, values appear about 15s after a trace's last span. A late span renumbers its whole trace.
- **What holds numbering back.** A pass waits for running synchronous inserts into `tempo_traces` and for spans a lagging replica has yet to fetch. A long insert, such as a backfill, holds back every shard's traces touched after it started until it ends. A lagging replica delays its shard by a few times the lag; a replica that can't catch up (a broken part, fetches stopped), or whose tables have yet to receive their first insert, stops its shard until it does. The other shards keep numbering, but each pass re-reads traces touched since that shard stopped, up to a day back. A pass doesn't wait for inserts on a node it can't reach, and a node too slow to answer fails the pass until it answers. Async inserts are assumed to commit within the idle period; a trace whose spans commit later may not be numbered until it receives another span.
- **Lease outages.** While the lease can't be read or written, numbering pauses and the instances log the start and end of the outage. Traces whose only spans arrived more than about a day before numbering resumes stay `0`/`0`/`0`, or more than 5 minutes before if no pass has succeeded yet.
- **No backfill.** A trace that receives no spans once the `tempo_traces_touched` table exists is never numbered, and one whose last span arrived more than 5 minutes before the first successful pass may not be.

Instances log start-up, failed passes, leadership changes and lease outages. To see who leads and since when (on a cluster, query `tempo_traces_nested_set_lease_dist`):

```sql
SELECT term,
       if(countIf(NOT claim) > 0, argMaxIf(holder, (expires_at, holder), NOT claim),
          argMinIf(holder, (expires_at, holder), claim)) AS leader,
       min(expires_at) - INTERVAL 15 SECOND AS since,
       max(expires_at) AS until,
       until > now64(3) AS live
FROM tempo_traces_nested_set_lease
GROUP BY term
ORDER BY term DESC
LIMIT 1
```

`leader` is the hostname, PID and a random suffix of the leading process. `since` subtracts the lease length, 3 × the interval: `15 SECOND` holds only at the default. Rows are kept for a day after they expire, so for a term led longer than that, `since` shows about a day ago.

### Large queries

A TraceQL search or tag autocomplete whose estimate exceeds
`GIGAPIPE_TRACEQL_PORTION_SPANS` runs in portions. Each portion reads one hash
bucket of traces over the whole requested range. For a search, each portion
returns its bucket's top `limit` traces and Gigapipe merges them by newest
matched span, so the result is the same at any portion count. Each portioned
request logs one info-level `traceql portioned` line with the query, range,
estimate, setting and portion count. For a structural query, the estimate is
the fullest shard's.

A search response reports its portions in a `metrics` block after the trace
array: `"metrics":{"totalJobs":N,"completedJobs":N}`, with `N` the portion count,
or `1` when the search was read at once. Autocomplete responses have no such
block.

`GIGAPIPE_TRACEQL_SEARCH_MAX_DURATION` caps the range at `168h` by default. A
longer request gets HTTP 400 instead of running.

Set ClickHouse `max_memory_usage` as the backstop: a query over it fails instead
of exhausting the node. The accepted risks are:

- A huge single trace isn't split. All its spans fall in one portion, whatever
  the setting.
- The budget is per query, and per shard for structural queries. Concurrent
  queries each read up to it.
- The default is sized for 7 GiB nodes. Lower it on smaller ones.
- Cost grows with range × portions, because every portion rescans the range.
- The reader holds up to `2 × limit` traces while it merges portions, which
  `max_memory_usage` doesn't cover.

### ClickHouse memory

Run the ClickHouse server with this variable in its environment (an environment variable, not a config key):

```
MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0
```

It makes ClickHouse return freed memory to the OS within about a second, so repeated large queries start from a low RSS instead of tripping the server memory limit (code 241).

Sizing for 24h structural TraceQL queries (search including Grafana Traces Drilldown, metrics, `compare()`, autocomplete) on a two-shard cluster, measured on aarch64: with the variable set, each node uses up to ~3.8 GB of tracked memory and ~5.1 GiB RSS, so 7 GiB per node is enough. Without it, plan about 2.5 GiB more per node.

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
CLICKHOUSE_READ_CLUSTER=cross_cluster_name
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
