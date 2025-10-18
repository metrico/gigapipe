## Comments are started with `##`
## The file is for log replicated tables
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens:
##   templating is done by "text/template" go lib
##   {{.OnCluster}} - is replaced by "ON CLUSTER `<clustername>`" or an empty string
##   {{.ReplacingMergeTree}} - is replaced by ReplacingMergeTree or ReplicatedReplacingMergeTree
##   {{.MergeTree}} - is replaced by MergeTree or ReplicatedMergeTree
##   {{.AggregatingMergeTree}} - is replaced by AggregatingMergeTree or ReplicatedAggregatingMergeTree
##   {{.CLUSTER}} - is replaced by cluster name if needed
##   {{.DB}} - is replaced by the db name
##   {{.CREATE_SETTINGS}} - extra create settings for tables //TODO
##   {{.SAMPLES_ORDER_RUL}} - samples ordering rule configurable //TODO

CREATE TABLE IF NOT EXISTS {{.DB}}.time_series {{.OnCluster}} (
    date Date,
    fingerprint UInt64,
    labels String,
    name String
) ENGINE = {{.ReplacingMergeTree}}(date)
PARTITION BY date
ORDER BY fingerprint {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_v3 {{.OnCluster}} (
  fingerprint UInt64,
  timestamp_ns Int64 CODEC(DoubleDelta),
  value Float64 CODEC(Gorilla),
  string String
) ENGINE = {{.MergeTree}}
PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
ORDER BY ({{.SAMPLES_ORDER_RUL}}) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.settings {{.OnCluster}} (
  fingerprint UInt64, 
  type String, 
  name String, 
  value String, 
  inserted_at DateTime64(9, 'UTC')
) ENGINE = {{.ReplacingMergeTree}}(inserted_at) 
ORDER BY fingerprint {{.CREATE_SETTINGS}};

DROP TABLE IF EXISTS {{.DB}}.samples_read {{.OnCluster}};

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_read {{.OnCluster}} (
    fingerprint UInt64,
    timestamp_ms Int64,
    value Float64,
    string String
) ENGINE=Merge('{{.DB}}', '^(samples|samples_v2)$');

CREATE VIEW IF NOT EXISTS {{.DB}}.samples_read_v2_1 {{.OnCluster}} AS
SELECT fingerprint, timestamp_ms * 1000000 as timestamp_ns, value, string FROM samples_read;

CREATE TABLE IF NOT EXISTS {{.DB}}.samples_read_v2_2 {{.OnCluster}} (
    fingerprint UInt64,
    timestamp_ns Int64,
    value Float64,
    string String
) ENGINE=Merge('{{.DB}}', '^(samples_read_v2_1|samples_v3)$');

CREATE TABLE IF NOT EXISTS {{.DB}}.time_series_gin {{.OnCluster}} (
    date Date,
    key String,
    val String,
    fingerprint UInt64
) ENGINE = {{.ReplacingMergeTree}}()
PARTITION BY date
ORDER BY (key, val, fingerprint) {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.time_series_gin_view {{.OnCluster}} TO time_series_gin
AS SELECT
    date,
    pairs.1 as key,
    pairs.2 as val,
    fingerprint
FROM time_series
ARRAY JOIN JSONExtractKeysAndValues(time_series.labels, 'String') as pairs;

INSERT INTO {{.DB}}.settings (fingerprint, type, name, value, inserted_at) 
VALUES (cityHash64('update_v3_5'), 'update', 'v3_1', toString(toUnixTimestamp(NOW())), NOW());

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_15s {{.OnCluster}} (
    fingerprint UInt64,
    timestamp_ns Int64 CODEC(DoubleDelta),
    last AggregateFunction(argMax, Float64, Int64),
    max SimpleAggregateFunction(max, Float64),
    min SimpleAggregateFunction(min, Float64),
    count AggregateFunction(count),
    sum SimpleAggregateFunction(sum, Float64),
    bytes SimpleAggregateFunction(sum, Float64)
) ENGINE = {{.AggregatingMergeTree}}
PARTITION BY toDate(toDateTime(intDiv(timestamp_ns, 1000000000)))
ORDER BY (fingerprint, timestamp_ns) {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_15s_mv {{.OnCluster}} TO metrics_15s
AS SELECT
    fingerprint,
    intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
    argMaxState(value, samples.timestamp_ns) as last,
    maxSimpleState(value) as max,
    minSimpleState(value) as min,
    countState() as count,
    sumSimpleState(value) as sum,
    sumSimpleState(length(string)) as bytes
FROM {{.DB}}.samples_v3 as samples
GROUP BY fingerprint, timestamp_ns;

INSERT INTO {{.DB}}.settings (fingerprint, type, name, value, inserted_at)
VALUES (cityHash64('update_v3_2'), 'update', 'v3_2', toString(toUnixTimestamp(NOW())), NOW());

INSERT INTO {{.DB}}.settings (fingerprint, type, name, value, inserted_at)
VALUES (cityHash64('update_v3_2'), 'update', 'v3_2', toString(toUnixTimestamp(NOW())), NOW());

ALTER TABLE {{.DB}}.time_series {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS type UInt8,
    MODIFY ORDER BY (fingerprint, type);

ALTER TABLE {{.DB}}.samples_v3 {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS type UInt8;

ALTER TABLE {{.DB}}.time_series_gin {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS type UInt8,
    MODIFY ORDER BY (key, val, fingerprint, type);

ALTER TABLE {{.DB}}.metrics_15s {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS type UInt8,
    MODIFY ORDER BY (fingerprint, timestamp_ns, type);

RENAME TABLE {{.DB}}.time_series_gin_view TO time_series_gin_view_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.time_series_gin_view {{.OnCluster}} TO time_series_gin
AS SELECT
    date,
    pairs.1 as key,
    pairs.2 as val,
    fingerprint,
    type
FROM time_series
ARRAY JOIN JSONExtractKeysAndValues(time_series.labels, 'String') as pairs;

DROP TABLE IF EXISTS {{.DB}}.time_series_gin_view_bak {{.OnCluster}};

RENAME TABLE {{.DB}}.metrics_15s_mv TO metrics_15s_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_15s_mv {{.OnCluster}} TO metrics_15s
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
FROM samples_v3 as samples
GROUP BY fingerprint, timestamp_ns, type;

DROP TABLE IF EXISTS {{.DB}}.metrics_15s_mv_bak {{.OnCluster}};

ALTER TABLE time_series
    ADD COLUMN `type_v2` UInt8 ALIAS type;

ALTER TABLE time_series_gin
    ADD COLUMN `type_v2` UInt8 ALIAS type;

ALTER TABLE samples_v3
    ADD COLUMN `type_v2` UInt8 ALIAS type;

ALTER TABLE metrics_15s
    ADD COLUMN `type_v2` UInt8 ALIAS type;

CREATE TABLE IF NOT EXISTS {{.DB}}.patterns {{.OnCluster}}(
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
) ENGINE = {{.MergeTree}}
PARTITION BY toDate(fromUnixTimestamp(timestamp_10m*600))
ORDER BY (timestamp_10m, fingerprint) {{.CREATE_SETTINGS}};
