## Scripts for the distributed profiles support
## APPEND ONLY!!!
## Please check log.sql file to get the main rules and template substarctions

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_dist {{.OnCluster}} (
    timestamp_ns UInt64,
    fingerprint UInt64,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    duration_ns UInt64,
    payload_type LowCardinality(String),
    payload String,
    values_agg Array(Tuple(String, Int64, Int32))
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}','profiles', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series_dist {{.OnCluster}} (
    date Date,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
    tags Array(Tuple(String, String)) CODEC(ZSTD(1))
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}','profiles_series',fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series_gin_dist {{.OnCluster}} (
    date Date,
    key String,
    val String,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1))
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}','profiles_series_gin',fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series_keys_dist {{.OnCluster}} (
    date Date,
    key String,
    val String,
    val_id UInt64
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}','profiles_series_keys', rand()) {{.DIST_CREATE_SETTINGS}};

ALTER TABLE {{.DB}}.profiles_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `tree` Array(Tuple(UInt64, UInt64, UInt64, Array(Tuple(String, Int64, Int64)))),
    ADD COLUMN IF NOT EXISTS `functions` Array(Tuple(UInt64, String));

ALTER TABLE {{.DB}}.profiles_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `sample_types_units` Array(Tuple(String, String));

ALTER TABLE {{.DB}}.profiles_series_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `sample_types_units` Array(Tuple(String, String));

ALTER TABLE {{.DB}}.profiles_series_gin_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `sample_types_units` Array(Tuple(String, String));

ALTER TABLE {{.DB}}.profiles_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16;

ALTER TABLE {{.DB}}.profiles_series_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16;

ALTER TABLE {{.DB}}.profiles_series_gin_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16;

ALTER TABLE {{.DB}}.profiles_series_keys_dist {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16;