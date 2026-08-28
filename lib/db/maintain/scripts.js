module.exports.overall = [
  `CREATE TABLE IF NOT EXISTS time_series (date Date,fingerprint UInt64,labels String, name String)
    ENGINE = ReplicatedReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint`,

  `CREATE TABLE IF NOT EXISTS samples_v3
    (
      fingerprint UInt64,
      timestamp_ns Int64 CODEC(DoubleDelta),
      value Float64 CODEC(Gorilla),
      string String
    ) ENGINE = ReplicatedMergeTree
    PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
    ORDER BY ({{SAMPLES_ORDER_RUL}})`,

  `CREATE TABLE IF NOT EXISTS settings
    (fingerprint UInt64, type String, name String, value String, inserted_at DateTime64(9, 'UTC'))
    ENGINE = ReplicatedReplacingMergeTree(inserted_at) ORDER BY fingerprint`,

  'DROP TABLE IF EXISTS samples_read',

  `CREATE TABLE IF NOT EXISTS samples_read
   (fingerprint UInt64,timestamp_ms Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples|samples_v2)$')`,

  `CREATE VIEW IF NOT EXISTS samples_read_v2_1 AS 
    SELECT fingerprint, timestamp_ms * 1000000 as timestamp_ns, value, string FROM samples_read`,

  `CREATE TABLE IF NOT EXISTS samples_read_v2_2
   (fingerprint UInt64,timestamp_ns Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples_read_v2_1|samples_v3)$')`,

  `CREATE TABLE IF NOT EXISTS time_series_gin (
    date Date,
    key String,
    val String,
    fingerprint UInt64
   ) ENGINE = ReplicatedReplacingMergeTree() PARTITION BY date ORDER BY (key, val, fingerprint)`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS time_series_gin_view TO time_series_gin
   AS SELECT date, pairs.1 as key, pairs.2 as val, fingerprint
   FROM time_series ARRAY JOIN JSONExtractKeysAndValues(time_series.labels, 'String') as pairs`,

  `INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_5'), 'update',
     'v3_1', toString(toUnixTimestamp(NOW())), NOW())`
]

module.exports.traces = [
  `CREATE TABLE IF NOT EXISTS tempo_traces (
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
  ) Engine ReplicatedMergeTree() ORDER BY (oid, trace_id, timestamp_ns)
  PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000))));`,

  `CREATE TABLE IF NOT EXISTS tempo_traces_attrs_gin (
    oid String,
    date Date,
    key String,
    val String,
    trace_id FixedString(16),
    span_id FixedString(8),
    timestamp_ns Int64,
    duration Int64
  ) Engine = ReplicatedReplacingMergeTree()
  PARTITION BY date
  ORDER BY (oid, date, key, val, timestamp_ns, trace_id, span_id);`,

  `CREATE TABLE IF NOT EXISTS tempo_traces_kv (
    oid String,
    date Date,
    key String,
    val_id UInt64,
    val String
  ) Engine = ReplicatedReplacingMergeTree()
  PARTITION BY (oid, date)
  ORDER BY (oid, date, key, val_id)`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS tempo_traces_kv_mv TO tempo_traces_kv AS 
    SELECT oid, date, key, cityHash64(val) % 10000 as val_id, val FROM tempo_traces_attrs_gin`,

  `CREATE TABLE IF NOT EXISTS traces_input (
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
   ) Engine=Null`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS traces_input_traces_mv TO tempo_traces AS
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
    FROM traces_input`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS traces_input_tags_mv TO tempo_traces_attrs_gin AS
    SELECT  oid,
      toDate(intDiv(timestamp_ns, 1000000000)) as date,
      tags.1 as key, 
      tags.2 as val,
      unhex(trace_id)::FixedString(16) as trace_id, 
      unhex(span_id)::FixedString(8) as span_id, 
      timestamp_ns,      
      duration_ns as duration
    FROM traces_input ARRAY JOIN tags`,

  `INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('tempo_traces_v1'), 'update',
     'tempo_traces_v2', toString(toUnixTimestamp(NOW())), NOW())`
]
