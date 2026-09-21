# Table Ordering

Gigapipe creates its ClickHouse schema on startup. Every table's `ORDER BY` is
fixed by the migration scripts in `ctrl/qryn/sql/` with exactly one exception:
the raw samples table, whose sort key is settable via `ADVANCED_SAMPLES_ORDERING`.

This page covers what that variable does, when it takes effect, how to change the
ordering of an already-populated deployment, and why the other tables are not
exposed the same way.

## `ADVANCED_SAMPLES_ORDERING`

Sets the `ORDER BY` clause of the `samples_v3` table. Default: `timestamp_ns`.

```bash
ADVANCED_SAMPLES_ORDERING="fingerprint, timestamp_ns"
```

The value is substituted verbatim into the `CREATE TABLE` statement:

```sql
CREATE TABLE IF NOT EXISTS <db>.samples_v3 (
  fingerprint UInt64,
  timestamp_ns Int64 CODEC(DoubleDelta),
  value Float64 CODEC(Gorilla),
  string String
) ENGINE = MergeTree
PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
ORDER BY (<ADVANCED_SAMPLES_ORDERING>);
```

(A later migration adds a `type UInt8` column to this table, so a live deployment
has one more column than the original `CREATE` shown above. `type` is not part of
the default sort key.)

Three things to know before setting it:

- **It affects one table.** `samples_v3` only — the raw samples store shared by
  logs and metrics, distinguished by the `type` column. It does not touch
  `metrics_15s`, `time_series`, the traces tables, or the profiles tables.
- **It applies at table creation only.** The migration runner is version-gated:
  it records the number of applied scripts per schema file and skips everything
  below that watermark. Setting the variable on a deployment where `samples_v3`
  already exists has no effect. See [Changing the ordering of an existing
  deployment](#changing-the-ordering-of-an-existing-deployment).
- **It is not validated.** The string is interpolated into DDL as-is. A typo
  surfaces as a ClickHouse syntax error at startup; a valid-but-wrong expression
  is accepted silently.

## Two meanings of `ORDER BY`

ClickHouse reuses one clause for two jobs, and which job it is depends on the
table engine. This distinction is why only one gigapipe table exposes the knob.

**On `MergeTree`, `ORDER BY` is an index.** It determines the on-disk sort order
and the sparse primary index. A poor choice costs scan time. Data is unaffected —
every inserted row is still there, still correct.

**On `AggregatingMergeTree` and `ReplacingMergeTree`, `ORDER BY` is the merge
key.** It determines which rows are *collapsed into one* during background
merges. A poor choice costs rows. Once a merge has run, the originals are gone.

`samples_v3` is a plain `MergeTree`, so its sort key is purely an index — any
permutation is safe, and the worst case is a slow query. That is what makes it
safe to expose as a configuration knob. The aggregate and deduplicating tables
have no such property.

## Schema reference

| Table | Engine | `ORDER BY` | Configurable |
|---|---|---|---|
| `samples_v3` | `MergeTree` | `timestamp_ns` (default) | **yes** — `ADVANCED_SAMPLES_ORDERING` |
| `metrics_15s` | `AggregatingMergeTree` | `(fingerprint, timestamp_ns, type)` | no |
| `time_series` | `ReplacingMergeTree` | `(fingerprint, type)` | no |
| `time_series_gin` | `ReplacingMergeTree` | `(key, val, fingerprint, type)` | no |
| `settings` | `ReplacingMergeTree` | `fingerprint` | no |
| `patterns` | `MergeTree` | `(timestamp_10m, fingerprint)` | no |
| `tempo_traces` | `MergeTree` | `(oid, trace_id, timestamp_ns)` | no |
| `tempo_traces_attrs_gin` | `ReplacingMergeTree` | `(oid, date, key, val, timestamp_ns, trace_id, span_id)` | no |
| `tempo_traces_kv` | `ReplacingMergeTree` | `(oid, date, key, val_id)` | no |
| `profiles` | `MergeTree` | `(type_id, service_name, timestamp_ns)` | no |
| `profiles_series` | `ReplacingMergeTree` | `(date, type_id, fingerprint)` | no |
| `profiles_series_gin` | `ReplacingMergeTree` | `(date, key, val, type_id, fingerprint)` | no |
| `profiles_series_keys` | `ReplacingMergeTree` | `(date, key, val_id)` | no |

Definitions live in `ctrl/qryn/sql/log.sql`, `traces.sql`, and `profiles.sql`.

## Choosing a sort key for `samples_v3`

Both read paths filter `samples_v3` the same way — a fingerprint set intersected
with a timestamp range:

- **PromQL** filters `fingerprint IN (…)` plus a `timestamp_ns` range
  (`reader/promql/promql_transpiler/planner/values.go`).
- **LogQL** puts the `timestamp_ns` range in `PREWHERE`
  (`reader/logql/logql_transpiler/clickhouse_planner/planner_main_init.go`) and
  adds `samples.fingerprint IN (…)` whenever the query carries a stream selector
  (`planner_fingerprint_filter.go`).

Both also filter on `type`, which is **not** part of the default sort key.

So the question is which predicate is more selective for your data:

- **`timestamp_ns` (default)** suits workloads where queries sweep a narrow time
  window across many series — typical log search.
- **`(fingerprint, timestamp_ns)`** suits workloads where each query touches few
  series out of a large total — typical high-cardinality metrics. It also
  aligns `samples_v3` with the layout `metrics_15s` already uses, and it is the
  shape in which the `fingerprint` column compresses — a storage effect separate
  from the scan-selectivity argument, covered below.
- **Adding `type`** can help deployments that mix heavy log and metric volume in
  one database, since every query filters on it.

The sort key decides compression as well as scan cost, and the two do not always
point the same way. A codec only helps when adjacent rows are similar, and what
lands adjacent is exactly what the sort key says. Under the default
`timestamp_ns` ordering consecutive rows belong to different series, so
`fingerprint` is interleaved noise and stays near-incompressible whatever codec
it carries — PR #976 measured this directly and reported that the column only
compresses once `ADVANCED_SAMPLES_ORDERING` groups by it. PR #977 chose the
shipped per-column codecs on the same basis: `DoubleDelta` for timestamps in
tables that are sorted by time, `Delta` for the ones that are not.

Note that `PARTITION BY` is per-day regardless of sort key, so time-range pruning
happens at the partition level either way. Fingerprint-first ordering does not
turn a narrow time query into a full-table scan — it scans whole days' granules
for the matched fingerprints instead of a contiguous time slice.

The compression side of this tradeoff has been measured — see PR #976 and
PR #977. The query-latency side has not: gigapipe ships no benchmark comparing
scan performance across these layouts, so the guidance above about which
predicate is more selective is directional rather than measured. Test it against
your own data and query mix before committing, since the choice is effectively
permanent (see below).

## Changing the ordering of an existing deployment

Two constraints make this a manual, deliberate operation.

**`ALTER TABLE … MODIFY ORDER BY` cannot help.** ClickHouse only allows
*appending* columns to an existing sort key. Going from `timestamp_ns` to
`(fingerprint, timestamp_ns)` prepends, which the statement does not support.

**Dropping the table does not make gigapipe recreate it.** The migration runner
reads the number of already-applied scripts from the `ver` table and only runs
scripts past that watermark. Since `samples_v3`'s `CREATE` is an early script in
`log.sql`, a dropped table is never rebuilt — you get a running gigapipe with a
missing table.

The safe shape is create-copy-swap, never drop-and-restart:

1. Optionally set `OMIT_CREATE_TABLES=true` on gigapipe instances to keep the
   schema maintenance and rotation passes from touching tables mid-migration.
2. Create `samples_v3_new` with the same columns, codecs, and `PARTITION BY`,
   changing only `ORDER BY`. Take the current definition from
   `SHOW CREATE TABLE samples_v3` so you inherit the per-column codecs added by
   PR #977 along with any TTL and storage policy. Hand-writing the column list is
   the easy way to silently drop them.
3. Copy the data, partition by partition, so a failure is resumable:
   `INSERT INTO samples_v3_new SELECT * FROM samples_v3 WHERE …`
4. Swap atomically: `EXCHANGE TABLES samples_v3 AND samples_v3_new`.
5. Verify row counts, then drop the old table.

On a cluster, repeat per shard; `samples_v3_dist` points at the local table name
and needs no change. Ingestion during the copy will write rows the copy has
already passed, so either pause writes or reconcile the tail partition after the
swap.

Given the cost, the common case is to decide at first deployment.

## Why `metrics_15s` is not configurable

`metrics_15s` is an `AggregatingMergeTree` whose sort key is its merge key, so
the failure mode is data loss rather than slow queries. Setting it to
`timestamp_ns` — mirroring the `samples_v3` default — would collapse every
fingerprint sharing a 15-second bucket into a single row, silently blending
`argMaxMerge(last)`, `sum`, and `count` across unrelated series. The damage
appears only after a background merge, by which point the source rows are gone.

The populating materialized view `metrics_15s_mv` also has `GROUP BY fingerprint,
timestamp_ns, type` hardcoded to match the table's key. Making the table's key
configurable without the view's would guarantee a mismatch; making both
configurable would hand operators two free-text variables that must agree, with
corruption as the failure mode.

There is also nothing to gain. Every reader of the table queries it the same way —
a fingerprint set intersected with a `timestamp_ns` range, which is exactly the
access pattern `(fingerprint, timestamp_ns, type)` already serves:

- `reader/promql/promql_transpiler/planner/bucket_producer.go`
- `reader/promql/promql_transpiler/planner/downsample_values.go`
- `reader/logql/logql_transpiler/clickhouse_planner/planner_metrics15s_shortcut.go`
  — the LogQL `rate` / `count_over_time` shortcut, wrapped in
  `FingerprintFilterPlanner` by `planner.go`, which supplies the fingerprint
  predicate

If a different key is ever needed for `metrics_15s`, the right mechanism is a
versioned migration appended to `log.sql` together with a matching view rewrite —
the pattern used when the `type` column was introduced — so every deployment
converges on the same reviewed schema.

## See also

- [Configuration](configuration.md) — full environment variable reference
