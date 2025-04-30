## Scripts for the profiles support
## APPEND ONLY!!!
## Please check log.sql file to get the main rules and template substarctions

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_input {{.OnCluster}} (
    timestamp_ns UInt64,
    type LowCardinality(String),
    service_name LowCardinality(String),
    sample_types_units Array(Tuple(String, String)),
    period_type LowCardinality(String),
    period_unit LowCardinality(String),
    tags Array(Tuple(String, String)),
    duration_ns UInt64,
    payload_type LowCardinality(String),
    payload String,
    values_agg Array(Tuple(String, Int64, Int32)) CODEC(ZSTD(1))
) Engine=Null;

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles {{.OnCluster}} (
    timestamp_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    sample_types_units Array(Tuple(String, String)) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    duration_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    payload_type LowCardinality(String) CODEC(ZSTD(1)),
    payload String CODEC(ZSTD(1)),
    values_agg Array(Tuple(String, Int64, Int32)) CODEC(ZSTD(1))
) Engine {{.MergeTree}}()
ORDER BY (type_id, service_name, timestamp_ns)
PARTITION BY toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000))) {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_mv {{.OnCluster}} TO profiles AS
SELECT
    timestamp_ns,
    cityHash64(arraySort(arrayConcat(
    profiles_input.tags, [
      ('__type__', concatWithSeparator(':', type, period_type, period_unit) as _type_id),
      ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
      ('service_name', service_name)
    ])) as _tags) as fingerprint,
    _type_id as type_id,
    sample_types_units,
    service_name,
    duration_ns,
    payload_type,
    payload,
    values_agg
FROM profiles_input;

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series {{.OnCluster}} (
    date Date CODEC(ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    sample_types_units Array(Tuple(String, String)) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
    tags Array(Tuple(String, String)) CODEC(ZSTD(1))
) Engine {{.ReplacingMergeTree}}()
ORDER BY (date, type_id, fingerprint)
PARTITION BY date {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_series_mv {{.OnCluster}} TO profiles_series AS
SELECT
  toDate(intDiv(timestamp_ns, 1000000000)) as date,
  concatWithSeparator(':', type, period_type, period_unit) as type_id,
  sample_types_units,
  service_name,
  cityHash64(arraySort(arrayConcat(
    profiles_input.tags, [
    ('__type__', type_id),
    ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
    ('service_name', service_name)
  ])) as _tags) as fingerprint,
  arrayConcat(profiles_input.tags, [('service_name', service_name)]) as tags
FROM profiles_input;

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series_gin {{.OnCluster}} (
    date Date CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1)),
    val String CODEC(ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    sample_types_units Array(Tuple(String, String)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1))
) Engine {{.ReplacingMergeTree}}()
ORDER BY (date, key, val, type_id, fingerprint)
PARTITION BY date {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_series_gin_mv {{.OnCluster}} TO profiles_series_gin AS
SELECT
    date,
    kv.1 as key,
    kv.2 as val,
    type_id,
    sample_types_units,
    service_name,
    fingerprint
FROM profiles_series ARRAY JOIN tags as kv;

CREATE TABLE IF NOT EXISTS {{.DB}}.profiles_series_keys {{.OnCluster}} (
    date Date,
    key String,
    val String,
    val_id UInt64
) Engine {{.ReplacingMergeTree}}()
ORDER BY (date, key, val_id)
PARTITION BY date {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_series_keys_mv {{.OnCluster}} TO profiles_series_keys AS
SELECT
    date,
    key,
    val,
    cityHash64(val) % 50000 as val_id
FROM profiles_series_gin;

ALTER TABLE {{.DB}}.profiles_input {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `tree` Array(Tuple(UInt64, UInt64, UInt64, Array(Tuple(String, Int64, Int64)))),
    ADD COLUMN IF NOT EXISTS `functions` Array(Tuple(UInt64, String));

ALTER TABLE {{.DB}}.profiles {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `tree` Array(Tuple(UInt64, UInt64, UInt64, Array(Tuple(String, Int64, Int64)))),
    ADD COLUMN IF NOT EXISTS `functions` Array(Tuple(UInt64, String));

RENAME TABLE IF EXISTS {{.DB}}.profiles_mv TO profiles_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_mv {{.OnCluster}} TO profiles AS
SELECT
    timestamp_ns,
    cityHash64(arraySort(arrayConcat(
      profiles_input.tags, [
        ('__type__', concatWithSeparator(':', type, period_type, period_unit) as _type_id),
        ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
        ('service_name', service_name)
    ])) as _tags) as fingerprint,
    _type_id as type_id,
    sample_types_units,
    service_name,
    duration_ns,
    payload_type,
    payload,
    values_agg,
    tree,
    functions
FROM profiles_input;

DROP TABLE IF EXISTS {{.DB}}.profiles_mv_bak {{.OnCluster}};

INSERT INTO {{.DB}}.settings (fingerprint, type, name, value, inserted_at)
VALUES (cityHash64('profiles_v2'), 'update', 'profiles_v2', toString(toUnixTimestamp(NOW())), NOW());

## TTL
ALTER TABLE {{.DB}}.profiles_input {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS `ttl_days` UInt16;

ALTER TABLE {{.DB}}.profiles
    ADD COLUMN IF NOT EXISTS `ttl_days` UInt16;

ALTER TABLE {{.DB}}.profiles_series {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16,
    MODIFY ORDER BY (date, type_id, fingerprint, ttl_days);

ALTER TABLE {{.DB}}.profiles_series_gin {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16,
    MODIFY ORDER BY (date, key, val, type_id, fingerprint, ttl_days);

ALTER TABLE {{.DB}}.profiles_series_keys {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16,
    MODIFY ORDER BY (date, key, val_id, ttl_days);

RENAME TABLE {{.DB}}.profiles_mv TO profiles_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_mv {{.OnCluster}} TO profiles AS
SELECT
    timestamp_ns,
    cityHash64(arraySort(arrayConcat(
            profiles_input.tags, [
      ('__type__', concatWithSeparator(':', type, period_type, period_unit) as _type_id),
            ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
            ('service_name', service_name)
                ])) as _tags) as fingerprint,
    _type_id as type_id,
    sample_types_units,
    service_name,
    duration_ns,
    payload_type,
    payload,
    values_agg,
    ttl_days
FROM profiles_input;

DROP TABLE IF EXISTS {{.DB}}.profiles_mv_bak {{.OnCluster}};

RENAME TABLE {{.DB}}.profiles_series_keys_mv TO profiles_series_keys_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_series_keys_mv {{.OnCluster}} TO profiles_series_keys AS
SELECT
    date,
    key,
    val,
    cityHash64(val) % 50000 as val_id,
    ttl_days
FROM profiles_series_gin;

DROP TABLE IF EXISTS {{.DB}}.profiles_series_keys_mv_bak {{.OnCluster}};

RENAME TABLE {{.DB}}.profiles_series_mv TO profiles_series_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_series_mv {{.OnCluster}} TO profiles_series AS
SELECT
  toDate(intDiv(timestamp_ns, 1000000000)) as date,
  concatWithSeparator(':', type, period_type, period_unit) as type_id,
  sample_types_units,
  service_name,
  cityHash64(arraySort(arrayConcat(
    profiles_input.tags, [
    ('__type__', type_id),
    ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
    ('service_name', service_name)
  ])) as _tags) as fingerprint,
  arrayConcat(profiles_input.tags, [('service_name', service_name)]) as tags,
  ttl_days
FROM profiles_input;

DROP TABLE IF EXISTS {{.DB}}.profiles_series_mv_bak {{.OnCluster}};

RENAME TABLE {{.DB}}.profiles_series_gin_mv TO profiles_series_gin_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.profiles_series_gin_mv {{.OnCluster}} TO profiles_series_gin AS
SELECT
    date,
    kv.1 as key,
    kv.2 as val,
    type_id,
    sample_types_units,
    service_name,
    fingerprint,
    ttl_days
FROM profiles_series ARRAY JOIN tags as kv;

DROP TABLE IF EXISTS {{.DB}}.profiles_series_gin_mv_bak {{.OnCluster}};

