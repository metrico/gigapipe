## These are comments
## The file for traces is Distributed tables
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_kv_dist {{.OnCluster}} (
    oid String,
    date Date,
    key String,
    val_id String,
    val String
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'tempo_traces_kv', sipHash64(oid, key)) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_dist {{.OnCluster}} (
    oid String,
    trace_id FixedString(16),
    span_id FixedString(8),
    parent_id String,
    name String,
    timestamp_ns Int64 CODEC(DoubleDelta),
    duration_ns Int64,
    service_name String,
    payload_type Int8,
    payload String
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'tempo_traces', sipHash64(oid, trace_id)) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_attrs_gin_dist {{.OnCluster}} (
    oid String,
    date Date,
    key String,
    val String,
    trace_id FixedString(16),
    span_id FixedString(8),
    timestamp_ns Int64,
    duration Int64
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'tempo_traces_attrs_gin', sipHash64(oid, trace_id)) {{.DIST_CREATE_SETTINGS}};

## Same placement as tempo_traces_dist for rows with an empty oid; the numbering pass writes shard-locally.
CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_nested_set_dist {{.OnCluster}} (
    trace_id FixedString(16),
    span_id FixedString(8),
    date Date,
    left Int32,
    right Int32,
    parent Int32,
    version UInt64
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'tempo_traces_nested_set', sipHash64('', trace_id)) {{.DIST_CREATE_SETTINGS}};

## Same placement as tempo_traces_dist for rows with an empty oid; the numbering pass writes shard-locally.
CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_nested_set_batch_dist {{.OnCluster}} (
    pass UUID,
    trace_id FixedString(16),
    touched DateTime64(3) DEFAULT 0
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'tempo_traces_nested_set_batch', sipHash64('', trace_id)) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_nested_set_lease_dist {{.OnCluster}} (
    term UInt64,
    holder String,
    claim Bool,
    expires_at DateTime64(3),
    cursor DateTime64(3) DEFAULT 0
) ENGINE = Distributed('{{.CLUSTER}}','{{.DB}}', 'tempo_traces_nested_set_lease', 0) {{.DIST_CREATE_SETTINGS}};
