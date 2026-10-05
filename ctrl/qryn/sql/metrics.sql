## The file is for the metric stack tables and views
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_samples {{.OnCluster}} (
  fingerprint UInt64,
  timestamp   DateTime64(3) CODEC(Delta, ZSTD(1)),
  value       Float64       CODEC(ZSTD(1))
) ENGINE = {{.ReplacingMergeTree}}
PARTITION BY toDate(timestamp)
ORDER BY (fingerprint, timestamp) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_exemplars {{.OnCluster}} (
  fingerprint UInt64,
  timestamp   DateTime64(3) CODEC(Delta, ZSTD(1)),
  value       Float64       CODEC(ZSTD(1)),
  trace_id    String        CODEC(ZSTD(1)),
  labels      String        CODEC(ZSTD(1))
) ENGINE = {{.ReplacingMergeTree}}
PARTITION BY toDate(timestamp)
ORDER BY (fingerprint, timestamp, trace_id) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_series {{.OnCluster}} (
  name        LowCardinality(String),
  fingerprint UInt64,
  labels      Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  first_seen  SimpleAggregateFunction(min, DateTime64(3)),
  last_seen   SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = {{.AggregatingMergeTree}}
ORDER BY (name, fingerprint) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_metadata {{.OnCluster}} (
  name       LowCardinality(String),
  type       LowCardinality(String),
  help       String CODEC(ZSTD(1)),
  unit       LowCardinality(String),
  updated_at DateTime64(3)
) ENGINE = {{.ReplacingMergeTree}}(updated_at)
ORDER BY name {{.CREATE_SETTINGS}};

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
  var         AggregateFunction(varPopStableIf, Float64, UInt8),
  min         AggregateFunction(minIf, Float64, UInt8),
  max         AggregateFunction(maxIf, Float64, UInt8),
  resets      SimpleAggregateFunction(sum, UInt64),
  reset_drop  SimpleAggregateFunction(sum, Float64),
  changes     SimpleAggregateFunction(sum, UInt64),
  stale_at    SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = {{.AggregatingMergeTree}}
PARTITION BY toDate(bucket)
ORDER BY (fingerprint, bucket) {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_5m_mv {{.OnCluster}} TO {{.DB}}.metrics_5m AS
{{.METRICS_5M_SELECT}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_1h {{.OnCluster}} (
  fingerprint UInt64,
  bucket      DateTime64(3) CODEC(Delta, ZSTD(1)),
  first       AggregateFunction(minIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  last        AggregateFunction(maxIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  count       SimpleAggregateFunction(sum, UInt64),
  sum         SimpleAggregateFunction(sum, Float64),
  var         AggregateFunction(varPopStableIf, Float64, UInt8),
  min         AggregateFunction(minIf, Float64, UInt8),
  max         AggregateFunction(maxIf, Float64, UInt8),
  resets      SimpleAggregateFunction(sum, UInt64),
  reset_drop  SimpleAggregateFunction(sum, Float64),
  changes     SimpleAggregateFunction(sum, UInt64),
  stale_at    SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = {{.AggregatingMergeTree}}
PARTITION BY toMonday(bucket)
ORDER BY (fingerprint, bucket) {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_1h_mv {{.OnCluster}} TO {{.DB}}.metrics_1h AS
{{.METRICS_1H_SELECT}};

INSERT INTO {{.DB}}.settings (fingerprint, type, name, value, inserted_at)
SELECT cityHash64('update_metric_stack'), 'update', 'metric_stack', toString(toUnixTimestamp(NOW())), NOW()
WHERE (SELECT count() FROM {{.DB}}.settings WHERE type = 'update' AND name = 'metric_stack') = 0;

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_label_names {{.OnCluster}} (
  label      String,
  first_seen SimpleAggregateFunction(min, DateTime64(3)),
  last_seen  SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = {{.AggregatingMergeTree}}
ORDER BY label {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metric_label_names_mv {{.OnCluster}} TO {{.DB}}.metric_label_names AS
SELECT label, min(first_seen) AS first_seen, max(last_seen) AS last_seen
FROM {{.DB}}.metric_series
ARRAY JOIN mapKeys(labels) AS label
GROUP BY label;

INSERT INTO {{.DB}}.metric_label_names (label, first_seen, last_seen)
SELECT label, min(first_seen) AS first_seen, max(last_seen) AS last_seen
FROM {{.DB}}.metric_series
ARRAY JOIN mapKeys(labels) AS label
GROUP BY label;
