## Cross-cluster read-path distributed tables for logs/metrics
## Used when CLICKHOUSE_READ_CLUSTER is set (multi-cluster query aggregation)
## APPEND ONLY!!!!!

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_15s{{.READ_SUFFIX}} {{.OnCluster}} (
    `fingerprint` UInt64,
    `timestamp_ns` Int64 CODEC(DoubleDelta),
    `last` AggregateFunction(argMax, Float64, Int64),
    `max` SimpleAggregateFunction(max, Float64),
    `min` SimpleAggregateFunction(min, Float64),
    `count` AggregateFunction(count),
    `sum` SimpleAggregateFunction(sum, Float64),
    `bytes` SimpleAggregateFunction(sum, Float64)
) ENGINE = Distributed('{{.READ_CLUSTER}}', '{{.DB}}', 'metrics_15s', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_v3{{.READ_SUFFIX}} {{.OnCluster}} (
    `fingerprint` UInt64,
    `timestamp_ns` Int64 CODEC(DoubleDelta),
    `value` Float64 CODEC(Gorilla),
    `string` String
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'samples_v3', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.time_series{{.READ_SUFFIX}} {{.OnCluster}} (
    `date` Date,
    `fingerprint` UInt64,
    `labels` String,
    `name` String
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'time_series', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.settings{{.READ_SUFFIX}} {{.OnCluster}} (
    `fingerprint` UInt64,
    `type` String,
    `name` String,
    `value` String,
    `inserted_at` DateTime64(9, 'UTC')
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'settings', rand()) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.time_series_gin{{.READ_SUFFIX}} {{.OnCluster}} (
    date Date,
    key String,
    val String,
    fingerprint UInt64
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'time_series_gin', rand()) SETTINGS skip_unavailable_shards = 1;

ALTER TABLE {{.DB}}.metrics_15s{{.READ_SUFFIX}} {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

ALTER TABLE {{.DB}}.samples_v3{{.READ_SUFFIX}} {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

ALTER TABLE {{.DB}}.time_series{{.READ_SUFFIX}} {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

ALTER TABLE {{.DB}}.time_series_gin{{.READ_SUFFIX}} {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

CREATE TABLE IF NOT EXISTS {{.DB}}.patterns{{.READ_SUFFIX}} {{.OnCluster}}(
    timestamp_10m UInt32,
    fingerprint UInt64,
    timestamp_s UInt32,
    tokens Array(String),
    classes Array(UInt32),
    overall_cost UInt32,
    generalized_cost UInt32,
    samples_count UInt32,
    pattern_id UInt64,
    iteration_id UInt64
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'patterns', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.ver{{.READ_SUFFIX}} {{.OnCluster}} (
    k UInt64,
    ver UInt64
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'ver', rand()) SETTINGS skip_unavailable_shards = 1;

ALTER TABLE {{.DB}}.time_series{{.READ_SUFFIX}} {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS metadata String DEFAULT '';

ALTER TABLE {{.DB}}.time_series{{.READ_SUFFIX}} {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS updated_at_ns Int64 DEFAULT toUnixTimestamp64Nano(now64(9));
