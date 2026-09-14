## Distributed wrappers for the split-by-signal tables.
## Executed only when SAMPLES_SPLIT_BY_SIGNAL is on and a cluster is configured.
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_logs_dist {{.OnCluster}} (
  `type` UInt8,
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `value` Float64 CODEC(Gorilla),
  `string` String
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'samples_logs', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_metrics_dist {{.OnCluster}} (
  `type` UInt8,
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `value` Float64 CODEC(Gorilla)
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'samples_metrics', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.logs_aggr_dist {{.OnCluster}} (
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `last` AggregateFunction(argMax, Float64, Int64),
  `max` SimpleAggregateFunction(max, Float64),
  `min` SimpleAggregateFunction(min, Float64),
  `count` AggregateFunction(count),
  `sum` SimpleAggregateFunction(sum, Float64),
  `bytes` SimpleAggregateFunction(sum, Float64),
  `type` UInt8
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'logs_aggr', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_aggr_dist {{.OnCluster}} (
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `last` AggregateFunction(argMax, Float64, Int64),
  `max` SimpleAggregateFunction(max, Float64),
  `min` SimpleAggregateFunction(min, Float64),
  `count` AggregateFunction(count),
  `sum` SimpleAggregateFunction(sum, Float64),
  `type` UInt8
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metrics_aggr', fingerprint) {{.DIST_CREATE_SETTINGS}};
