## Cross-cluster read-path wrappers for the split-by-signal tables.
## Used when CLICKHOUSE_READ_CLUSTER is set and SAMPLES_SPLIT_BY_SIGNAL is on.
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_logs{{.READ_SUFFIX}} {{.OnCluster}} (
  `type` UInt8,
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `value` Float64 CODEC(Gorilla),
  `string` String
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'samples_logs', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_metrics{{.READ_SUFFIX}} {{.OnCluster}} (
  `type` UInt8,
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `value` Float64 CODEC(Gorilla)
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'samples_metrics', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.logs_aggr{{.READ_SUFFIX}} {{.OnCluster}} (
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `last` AggregateFunction(argMax, Float64, Int64),
  `max` SimpleAggregateFunction(max, Float64),
  `min` SimpleAggregateFunction(min, Float64),
  `count` AggregateFunction(count),
  `sum` SimpleAggregateFunction(sum, Float64),
  `bytes` SimpleAggregateFunction(sum, Float64),
  `type` UInt8
) ENGINE = Distributed('{{.READ_CLUSTER}}', '{{.DB}}', 'logs_aggr', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_aggr{{.READ_SUFFIX}} {{.OnCluster}} (
  `fingerprint` UInt64,
  `timestamp_ns` Int64 CODEC(DoubleDelta),
  `last` AggregateFunction(argMax, Float64, Int64),
  `max` SimpleAggregateFunction(max, Float64),
  `min` SimpleAggregateFunction(min, Float64),
  `count` AggregateFunction(count),
  `sum` SimpleAggregateFunction(sum, Float64),
  `type` UInt8
) ENGINE = Distributed('{{.READ_CLUSTER}}', '{{.DB}}', 'metrics_aggr', fingerprint) SETTINGS skip_unavailable_shards = 1;
