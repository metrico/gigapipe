## Cross-cluster read-path distributed tables for traces
## Used when CLICKHOUSE_READ_CLUSTER is set (multi-cluster query aggregation)
## APPEND ONLY!!!!!

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_kv{{.READ_SUFFIX}} {{.OnCluster}} (
    oid String,
    date Date,
    key String,
    val_id String,
    val String
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'tempo_traces_kv', sipHash64(oid, key)) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces{{.READ_SUFFIX}} {{.OnCluster}} (
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
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'tempo_traces', sipHash64(oid, trace_id)) SETTINGS skip_unavailable_shards = 1;

CREATE TABLE IF NOT EXISTS {{.DB}}.tempo_traces_attrs_gin{{.READ_SUFFIX}} {{.OnCluster}} (
    oid String,
    date Date,
    key String,
    val String,
    trace_id FixedString(16),
    span_id FixedString(8),
    timestamp_ns Int64,
    duration Int64
) ENGINE = Distributed('{{.READ_CLUSTER}}','{{.DB}}', 'tempo_traces_attrs_gin', sipHash64(oid, trace_id)) SETTINGS skip_unavailable_shards = 1;
