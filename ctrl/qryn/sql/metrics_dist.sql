## The file is for the metric stack distributed tables
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_samples_in_dist {{.OnCluster}} (
  fingerprint    UInt64,
  timestamp      DateTime64(3),
  value          Float64,
  prev_timestamp DateTime64(3),
  prev_value     Float64,
  aggregate      UInt8
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metric_samples_in', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_samples_dist {{.OnCluster}} (
  fingerprint UInt64,
  timestamp   DateTime64(3),
  value       Float64
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metric_samples', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_exemplars_dist {{.OnCluster}} (
  fingerprint UInt64,
  timestamp   DateTime64(3),
  value       Float64,
  trace_id    String,
  labels      String
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metric_exemplars', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_series_dist {{.OnCluster}} (
  name        LowCardinality(String),
  fingerprint UInt64,
  labels      Map(LowCardinality(String), String),
  first_seen  SimpleAggregateFunction(min, DateTime64(3)),
  last_seen   SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metric_series', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metric_metadata_dist {{.OnCluster}} (
  name       LowCardinality(String),
  type       LowCardinality(String),
  help       String,
  unit       LowCardinality(String),
  updated_at DateTime64(3)
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metric_metadata', cityHash64(name)) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_5m_dist {{.OnCluster}} (
  fingerprint UInt64,
  bucket      DateTime64(3),
  first       AggregateFunction(minIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  last        AggregateFunction(maxIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  count       SimpleAggregateFunction(sum, UInt64),
  sum         SimpleAggregateFunction(sum, Float64),
  var         AggregateFunction(varPopStableIf, Float64, UInt8),
  min         AggregateFunction(minIf, Float64, UInt8),
  max         AggregateFunction(maxIf, Float64, UInt8),
  resets      SimpleAggregateFunction(sum, UInt64),
  reset_drop  SimpleAggregateFunction(sum, Float64),
  changes     SimpleAggregateFunction(sum, UInt64),
  stale_at    SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metrics_5m', fingerprint) {{.DIST_CREATE_SETTINGS}};

CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_1h_dist {{.OnCluster}} (
  fingerprint UInt64,
  bucket      DateTime64(3),
  first       AggregateFunction(minIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  last        AggregateFunction(maxIf, Tuple(timestamp DateTime64(3), value Float64), UInt8),
  count       SimpleAggregateFunction(sum, UInt64),
  sum         SimpleAggregateFunction(sum, Float64),
  var         AggregateFunction(varPopStableIf, Float64, UInt8),
  min         AggregateFunction(minIf, Float64, UInt8),
  max         AggregateFunction(maxIf, Float64, UInt8),
  resets      SimpleAggregateFunction(sum, UInt64),
  reset_drop  SimpleAggregateFunction(sum, Float64),
  changes     SimpleAggregateFunction(sum, UInt64),
  stale_at    SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'metrics_1h', fingerprint) {{.DIST_CREATE_SETTINGS}};
