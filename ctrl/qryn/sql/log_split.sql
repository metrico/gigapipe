## Split-by-signal tables: logs and metrics stored apart.
## Executed only when SAMPLES_SPLIT_BY_SIGNAL is on.
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql, plus
##   {{.METRICS_ORDER_RUL}} - ordering rule for samples_metrics

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_logs {{.OnCluster}} (
  type UInt8,
  fingerprint UInt64 CODEC(Delta, ZSTD(1)),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
  value Float64 CODEC(Gorilla, ZSTD(1)),
  string String CODEC(ZSTD(1))
) ENGINE = {{.MergeTree}}
PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
ORDER BY ({{.SAMPLES_ORDER_RUL}}) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_metrics {{.OnCluster}} (
  type UInt8,
  fingerprint UInt64 CODEC(Delta, ZSTD(1)),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
  value Float64 CODEC(Gorilla, ZSTD(1))
) ENGINE = {{.MergeTree}}
PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
ORDER BY ({{.METRICS_ORDER_RUL}}) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.logs_aggr {{.OnCluster}} (
  fingerprint UInt64 CODEC(Delta, ZSTD(1)),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
  last AggregateFunction(argMax, Float64, Int64) CODEC(ZSTD(1)),
  max SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
  min SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
  count AggregateFunction(count) CODEC(ZSTD(1)),
  sum SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
  bytes SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
  type UInt8
) ENGINE = {{.AggregatingMergeTree}}
PARTITION BY toDate(toDateTime(intDiv(timestamp_ns, 1000000000)))
ORDER BY (fingerprint, timestamp_ns, type) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_aggr {{.OnCluster}} (
  fingerprint UInt64 CODEC(Delta, ZSTD(1)),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)),
  last AggregateFunction(argMax, Float64, Int64) CODEC(ZSTD(1)),
  max SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
  min SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
  count AggregateFunction(count) CODEC(ZSTD(1)),
  sum SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
  type UInt8
) ENGINE = {{.AggregatingMergeTree}}
PARTITION BY toDate(toDateTime(intDiv(timestamp_ns, 1000000000)))
ORDER BY (fingerprint, timestamp_ns, type) {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.logs_aggr_mv {{.OnCluster}} TO {{.DB}}.logs_aggr
AS SELECT
    fingerprint,
    intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
    argMaxState(value, samples.timestamp_ns) as last,
    maxSimpleState(value) as max,
    minSimpleState(value) as min,
    countState() as count,
    sumSimpleState(value) as sum,
    sumSimpleState(length(string)) as bytes,
    type
FROM {{.DB}}.samples_logs as samples
GROUP BY fingerprint, timestamp_ns, type;
