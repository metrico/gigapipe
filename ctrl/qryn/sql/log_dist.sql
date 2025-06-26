## These are comments
## The file is for log distributed tables
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_15s_dist {{.OnCluster}} (
    `fingerprint` UInt64,
    `timestamp_ns` Int64 CODEC(DoubleDelta),
    `last` AggregateFunction(argMax, Float64, Int64),
    `max` SimpleAggregateFunction(max, Float64),
    `min` SimpleAggregateFunction(min, Float64),
    `count` AggregateFunction(count),
    `sum` SimpleAggregateFunction(sum, Float64),
    `bytes` SimpleAggregateFunction(sum, Float64)
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metrics_15s', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_v3_dist {{.OnCluster}} (
    `fingerprint` UInt64,
    `timestamp_ns` Int64 CODEC(DoubleDelta),
    `value` Float64 CODEC(Gorilla),
    `string` String
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'samples_v3', fingerprint);

CREATE TABLE IF NOT EXISTS {{.DB}}.time_series_dist {{.OnCluster}} (
    `date` Date,
    `fingerprint` UInt64,
    `labels` String,
    `name` String
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'time_series', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.settings_dist {{.OnCluster}} (
    `fingerprint` UInt64,
    `type` String,
    `name` String,
    `value` String,
    `inserted_at` DateTime64(9, 'UTC')
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'settings', rand()) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.time_series_gin_dist {{.OnCluster}} (
    date Date,
    key String,
    val String,
    fingerprint UInt64
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'time_series_gin', rand()) {{.DIST_CREATE_SETTINGS}};

ALTER TABLE {{.DB}}.metrics_15s_dist {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

ALTER TABLE {{.DB}}.samples_v3_dist {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

ALTER TABLE {{.DB}}.time_series_dist {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

ALTER TABLE {{.DB}}.time_series_gin_dist {{.OnCluster}} ADD COLUMN IF NOT EXISTS `type` UInt8;

ALTER TABLE time_series_dist
    (ADD COLUMN `type_v2` UInt8 ALIAS type);

ALTER TABLE time_series_gin_dist
    (ADD COLUMN `type_v2` UInt8 ALIAS type);

ALTER TABLE samples_v3_dist
    (ADD COLUMN `type_v2` UInt8 ALIAS type);

ALTER TABLE metrics_15s_dist
    (ADD COLUMN `type_v2` UInt8 ALIAS type);

CREATE TABLE IF NOT EXISTS {{.DB}}.patterns_dist {{.OnCluster}}(
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
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'patterns', fingerprint) {{.DIST_CREATE_SETTINGS}};
