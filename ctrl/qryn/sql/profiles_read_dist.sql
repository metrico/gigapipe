## Cross-cluster read-path distributed tables for profiles
## Used when CLICKHOUSE_READ_CLUSTER is set (multi-cluster query aggregation)
## APPEND ONLY!!!!!

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles{{.READ_SUFFIX}} {{.OnCluster}} (
    timestamp_ns UInt64,
    fingerprint UInt64,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    duration_ns UInt64,
    payload_type LowCardinality(String),
    payload String,
    values_agg Array(Tuple(String, Int64, Int32))
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}','profiles', fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series{{.READ_SUFFIX}} {{.OnCluster}} (
    date Date,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
    tags Array(Tuple(String, String)) CODEC(ZSTD(1))
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}','profiles_series',fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series_gin{{.READ_SUFFIX}} {{.OnCluster}} (
    date Date,
    key String,
    val String,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1))
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}','profiles_series_gin',fingerprint) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series_keys{{.READ_SUFFIX}} {{.OnCluster}} (
    date Date,
    key String,
    val String,
    val_id UInt64
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}','profiles_series_keys', rand()) SETTINGS skip_unavailable_shards = 1;

ALTER TABLE {{.DB}}.profiles{{.READ_SUFFIX}} {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `tree` Array(Tuple(UInt64, UInt64, UInt64, Array(Tuple(String, Int64, Int64)))),
    ADD COLUMN IF NOT EXISTS `functions` Array(Tuple(UInt64, String));

ALTER TABLE {{.DB}}.profiles{{.READ_SUFFIX}} {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `sample_types_units` Array(Tuple(String, String));

ALTER TABLE {{.DB}}.profiles_series{{.READ_SUFFIX}} {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `sample_types_units` Array(Tuple(String, String));

ALTER TABLE {{.DB}}.profiles_series_gin{{.READ_SUFFIX}} {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `sample_types_units` Array(Tuple(String, String));
