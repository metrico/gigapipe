## These are comments
## The file for traces
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces {{.OnCluster}} (
    oid String DEFAULT '0',
    trace_id FixedString(16),
    span_id FixedString(8),
    parent_id String,
    name String,
    timestamp_ns Int64 CODEC(DoubleDelta),
    duration_ns Int64,
    service_name String,
    payload_type Int8,
    payload String
) Engine = {{.MergeTree}}() ORDER BY (oid, trace_id, timestamp_ns)
PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000)))) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_attrs_gin {{.OnCluster}} (
    oid String,
    date Date,
    key String,
    val String,
    trace_id FixedString(16),
    span_id FixedString(8),
    timestamp_ns Int64,
    duration Int64
) Engine = {{.ReplacingMergeTree}}()
PARTITION BY date
ORDER BY (oid, date, key, val, timestamp_ns, trace_id, span_id) {{.CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_kv {{.OnCluster}} (
    oid String,
    date Date,
    key String,
    val_id UInt64,
    val String
) Engine = {{.ReplacingMergeTree}}()
PARTITION BY (oid, date)
ORDER BY (oid, date, key, val_id) {{.CREATE_SETTINGS}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.tempo_traces_kv_mv {{.OnCluster}} TO tempo_traces_kv AS
SELECT
    oid,
    date,
    key,
    cityHash64(val) % 10000 as val_id,
    val
FROM tempo_traces_attrs_gin;

CREATE TABLE IF NOT EXISTS {{.DB}}.traces_input {{.OnCluster}} (
    oid String DEFAULT '0',
    trace_id String,
    span_id String,
    parent_id String,
    name String,
    timestamp_ns Int64 CODEC(DoubleDelta),
    duration_ns Int64,
    service_name String,
    payload_type Int8,
    payload String,
    tags Array(Tuple(String, String))
) Engine=Null;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.traces_input_traces_mv {{.OnCluster}} TO tempo_traces AS
SELECT  oid,
    unhex(trace_id)::FixedString(16) as trace_id,
    unhex(span_id)::FixedString(8) as span_id,
    unhex(parent_id) as parent_id,
    name,
    timestamp_ns,
    duration_ns,
    service_name,
    payload_type,
    payload
FROM traces_input;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.traces_input_tags_mv {{.OnCluster}} TO tempo_traces_attrs_gin AS
SELECT  oid,
    toDate(intDiv(timestamp_ns, 1000000000)) as date,
    tags.1 as key, 
    tags.2 as val,
    unhex(trace_id)::FixedString(16) as trace_id, 
    unhex(span_id)::FixedString(8) as span_id, 
    timestamp_ns,      
    duration_ns as duration
FROM traces_input ARRAY JOIN tags;

INSERT INTO {{.DB}}.settings (fingerprint, type, name, value, inserted_at)
VALUES (cityHash64('tempo_traces_v1'), 'update', 'tempo_traces_v2', toString(toUnixTimestamp(NOW())), NOW());

## TTL
ALTER TABLE {{.DB}}.tempo_traces {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16;

ALTER TABLE {{.DB}}.tempo_traces_attrs_gin {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16,
    MODIFY ORDER BY (oid, date, key, val, timestamp_ns, trace_id, span_id, ttl_days);

ALTER TABLE {{.DB}}.tempo_traces_kv {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16,
    MODIFY ORDER BY (oid, date, key, val_id, ttl_days);

ALTER TABLE {{.DB}}.traces_input {{.OnCluster}}
    ADD COLUMN IF NOT EXISTS ttl_days UInt16;

RENAME TABLE {{.DB}}.traces_input_traces_mv TO traces_input_traces_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.traces_input_traces_mv {{.OnCluster}} TO tempo_traces AS
SELECT  oid,
    unhex(trace_id)::FixedString(16) as trace_id,
    unhex(span_id)::FixedString(8) as span_id,
    unhex(parent_id) as parent_id,
    name,
    timestamp_ns,
    duration_ns,
    service_name,
    payload_type,
    payload,
    ttl_days
FROM traces_input;

DROP TABLE IF EXISTS {{.DB}}.traces_input_traces_mv_bak {{.OnCluster}};

RENAME TABLE {{.DB}}.traces_input_tags_mv TO traces_input_tags_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.traces_input_tags_mv {{.OnCluster}} TO tempo_traces_attrs_gin AS
SELECT  oid,
    toDate(intDiv(timestamp_ns, 1000000000)) as date,
    tags.1 as key,
    tags.2 as val,
    unhex(trace_id)::FixedString(16) as trace_id,
    unhex(span_id)::FixedString(8) as span_id,
    timestamp_ns,
    duration_ns as duration,
    ttl_days
FROM traces_input ARRAY JOIN tags;

DROP TABLE IF EXISTS {{.DB}}.traces_input_tags_mv_bak {{.OnCluster}};

RENAME TABLE {{.DB}}.tempo_traces_kv_mv TO tempo_traces_kv_mv_bak {{.OnCluster}};

CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.tempo_traces_kv_mv {{.OnCluster}} TO tempo_traces_kv AS
SELECT
    oid,
    date,
    key,
    cityHash64(val) % 10000 as val_id,
    val,
    ttl_days
FROM tempo_traces_attrs_gin;

DROP TABLE IF EXISTS {{.DB}}.tempo_traces_kv_mv_bak {{.OnCluster}};
