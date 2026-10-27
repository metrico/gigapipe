## The file is for the metric stack tables and views
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens beyond those of log.sql:
##   {{.SAMPLES_DAYS}} - the raw tier's lifetime in days (METRICS_RAW_DAYS)
##   {{.METRICS_5M_DAYS}} - the 5m tier's lifetime in days
##   {{.METRICS_1H_DAYS}} - the 1h tier's lifetime in days
##   {{.SERIES_DAYS}} - the series index lifetime in days, the 1h tier's

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_samples {{.OnCluster}} (
  fingerprint UInt64,
  timestamp   DateTime64(3) CODEC(Delta, ZSTD(1)),
  value       Float64       CODEC(ZSTD(1))
) ENGINE = {{.ReplacingMergeTree}}
PARTITION BY toDate(timestamp)
ORDER BY (fingerprint, timestamp)
TTL toDateTime(timestamp) + INTERVAL {{.SAMPLES_DAYS}} DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_exemplars {{.OnCluster}} (
  fingerprint UInt64,
  timestamp   DateTime64(3) CODEC(Delta, ZSTD(1)),
  value       Float64       CODEC(ZSTD(1)),
  trace_id    String        CODEC(ZSTD(1)),
  labels      String        CODEC(ZSTD(1))
) ENGINE = {{.ReplacingMergeTree}}
PARTITION BY toDate(timestamp)
ORDER BY (fingerprint, timestamp, trace_id)
TTL toDateTime(timestamp) + INTERVAL {{.SAMPLES_DAYS}} DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_series {{.OnCluster}} (
  name        LowCardinality(String),
  fingerprint UInt64,
  labels      Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  first_seen  SimpleAggregateFunction(min, DateTime64(3)),
  last_seen   SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = {{.AggregatingMergeTree}}
ORDER BY (name, fingerprint)
TTL toDateTime(last_seen) + INTERVAL {{.SERIES_DAYS}} DAY;

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_metadata {{.OnCluster}} (
  name       LowCardinality(String),
  type       LowCardinality(String),
  help       String CODEC(ZSTD(1)),
  unit       LowCardinality(String),
  updated_at DateTime64(3)
) ENGINE = {{.ReplacingMergeTree}}(updated_at)
ORDER BY name;

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_samples_in {{.OnCluster}} (
  fingerprint    UInt64,
  timestamp      DateTime64(3),
  value          Float64,
  prev_timestamp DateTime64(3),
  prev_value     Float64,
  aggregate      UInt8
) ENGINE = Null;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metric_samples_mv {{.OnCluster}} TO {{.DB}}.metric_samples AS
SELECT fingerprint, timestamp, value FROM {{.DB}}.metric_samples_in;

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_5m {{.OnCluster}} (
  fingerprint UInt64,
  bucket      DateTime64(3) CODEC(Delta, ZSTD(1)),
  first       AggregateFunction(minIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  last        AggregateFunction(maxIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  count       SimpleAggregateFunction(sum, UInt64),
  sum         SimpleAggregateFunction(sum, Float64),
  sum_sq      SimpleAggregateFunction(sum, Float64),
  min         AggregateFunction(minIf, Float64, UInt8),
  max         AggregateFunction(maxIf, Float64, UInt8),
  resets      SimpleAggregateFunction(sum, UInt64),
  reset_drop  SimpleAggregateFunction(sum, Float64),
  changes     SimpleAggregateFunction(sum, UInt64),
  stale_at    SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = {{.AggregatingMergeTree}}
PARTITION BY toDate(bucket)
ORDER BY (fingerprint, bucket)
TTL toDateTime(bucket) + INTERVAL {{.METRICS_5M_DAYS}} DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_5m_mv {{.OnCluster}} TO {{.DB}}.metrics_5m AS
WITH
  reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale,
  reinterpretAsUInt64(prev_value) = 0x7ff0000000000002 AS prev_stale,
  toStartOfInterval(timestamp - INTERVAL 1 MILLISECOND, INTERVAL 5 MINUTE) + INTERVAL 5 MINUTE AS bucket_end,
  prev_timestamp < timestamp
    AND toStartOfInterval(prev_timestamp - INTERVAL 1 MILLISECOND, INTERVAL 5 MINUTE) + INTERVAL 5 MINUTE = bucket_end
    AND NOT stale AND NOT prev_stale AS paired
SELECT
  fingerprint,
  toDateTime64(bucket_end, 3) AS bucket,
  minIfState((timestamp, value), NOT stale) AS first,
  maxIfState((timestamp, value), NOT stale) AS last,
  countIf(NOT stale) AS count,
  sumIf(value, NOT stale) AS sum,
  sumIf(value * value, NOT stale) AS sum_sq,
  minIfState(value, NOT stale) AS min,
  maxIfState(value, NOT stale) AS max,
  countIf(paired AND value < prev_value) AS resets,
  sumIf(prev_value, paired AND value < prev_value) AS reset_drop,
  countIf(paired AND value != prev_value AND NOT (isNaN(value) AND isNaN(prev_value))) AS changes,
  maxIf(timestamp, stale) AS stale_at
FROM {{.DB}}.metric_samples_in
WHERE aggregate
GROUP BY fingerprint, bucket;

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_1h {{.OnCluster}} (
  fingerprint UInt64,
  bucket      DateTime64(3) CODEC(Delta, ZSTD(1)),
  first       AggregateFunction(minIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  last        AggregateFunction(maxIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  count       SimpleAggregateFunction(sum, UInt64),
  sum         SimpleAggregateFunction(sum, Float64),
  sum_sq      SimpleAggregateFunction(sum, Float64),
  min         AggregateFunction(minIf, Float64, UInt8),
  max         AggregateFunction(maxIf, Float64, UInt8),
  resets      SimpleAggregateFunction(sum, UInt64),
  reset_drop  SimpleAggregateFunction(sum, Float64),
  changes     SimpleAggregateFunction(sum, UInt64),
  stale_at    SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = {{.AggregatingMergeTree}}
PARTITION BY toMonday(bucket)
ORDER BY (fingerprint, bucket)
TTL toDateTime(bucket) + INTERVAL {{.METRICS_1H_DAYS}} DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_1h_mv {{.OnCluster}} TO {{.DB}}.metrics_1h AS
WITH
  reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale,
  reinterpretAsUInt64(prev_value) = 0x7ff0000000000002 AS prev_stale,
  toStartOfInterval(timestamp - INTERVAL 1 MILLISECOND, INTERVAL 1 HOUR) + INTERVAL 1 HOUR AS bucket_end,
  prev_timestamp < timestamp
    AND toStartOfInterval(prev_timestamp - INTERVAL 1 MILLISECOND, INTERVAL 1 HOUR) + INTERVAL 1 HOUR = bucket_end
    AND NOT stale AND NOT prev_stale AS paired
SELECT
  fingerprint,
  toDateTime64(bucket_end, 3) AS bucket,
  minIfState((timestamp, value), NOT stale) AS first,
  maxIfState((timestamp, value), NOT stale) AS last,
  countIf(NOT stale) AS count,
  sumIf(value, NOT stale) AS sum,
  sumIf(value * value, NOT stale) AS sum_sq,
  minIfState(value, NOT stale) AS min,
  maxIfState(value, NOT stale) AS max,
  countIf(paired AND value < prev_value) AS resets,
  sumIf(prev_value, paired AND value < prev_value) AS reset_drop,
  countIf(paired AND value != prev_value AND NOT (isNaN(value) AND isNaN(prev_value))) AS changes,
  maxIf(timestamp, stale) AS stale_at
FROM {{.DB}}.metric_samples_in
WHERE aggregate
GROUP BY fingerprint, bucket;

INSERT INTO {{.DB}}.settings (fingerprint, type, name, value, inserted_at)
SELECT cityHash64('update_metric_stack'), 'update', 'metric_stack', toString(toUnixTimestamp(NOW())), NOW()
WHERE (SELECT count() FROM {{.DB}}.settings WHERE type = 'update' AND name = 'metric_stack') = 0;
