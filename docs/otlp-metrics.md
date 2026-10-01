# OTLP Metrics

gigapipe accepts OpenTelemetry (OTLP) metrics on both transports:

- **OTLP/HTTP**: `POST /v1/metrics` with `Content-Type: application/x-protobuf`
  or `application/json`. Gzip request bodies are supported
  (`Content-Encoding: gzip`); the decompressed body is limited to 64 MiB
  (HTTP 413 beyond that). The response is an `ExportMetricsServiceResponse`
  encoded to match the request's content type; non-200 responses carry a
  `google.rpc.Status`.
- **OTLP/gRPC**: `opentelemetry.proto.collector.metrics.v1.MetricsService/Export`,
  multiplexed on the same port as HTTP (see [otlp-grpc.md](otlp-grpc.md)).

## Translation to Prometheus-style series

Incoming metrics are translated per the OpenTelemetry Prometheus
compatibility spec, matching what the Prometheus OTLP receiver does by
default:

- Metric names are mapped to the Prometheus charset (dots become
  underscores), UCUM units become word suffixes (`s` → `_seconds`,
  `By` → `_bytes`, `By/s` → `_bytes_per_second`), and monotonic sums get a
  `_total` suffix. `{annotation}` units and the dimensionless unit `1` add no
  suffix.
- `service.namespace`/`service.name` become the `job` label
  (`<namespace>/<name>`), `service.instance.id` becomes `instance`.
- Remaining resource attributes go to a `target_info` gauge (constant value
  1, one sample per resource per export, at the latest accepted data-point
  timestamp), not onto every series.
- Instrumentation scope becomes `otel_scope_name` / `otel_scope_version` /
  `otel_scope_schema_url` labels plus `otel_scope_`-prefixed scope
  attributes. A scope attribute whose sanitized name would collide with one
  of those three identity labels is dropped, per the OTel↔Prometheus
  compatibility spec.
- Metric `description` and unit are stored as time-series metadata, not as
  labels.

Per-type mapping:

| OTLP type             | Stored series |
|-----------------------|---------------|
| Gauge                 | one series per attribute set |
| Sum (cumulative)      | counter (`_total`) when monotonic, gauge otherwise |
| Histogram (cumulative)| `_bucket` (cumulative counts, `le` labels, `+Inf`), `_sum`, `_count` |
| ExponentialHistogram (cumulative) | converted to classic `_bucket`/`_sum`/`_count` series with exact boundaries (bucket `k`'s upper bound is `2^((k+1)·2^-scale)`, computed so power-of-two bounds are exact) |
| Summary               | quantile-labeled series plus `_sum`/`_count` |

## Rejected and dropped data

Per-data-point problems never fail the request: the response reports them in
`partial_success.rejected_data_points` with a human-readable
`error_message`, per the OTLP spec. Rejected (counted):

- Sum / Histogram / ExponentialHistogram data points with **delta** or
  unspecified aggregation temporality. gigapipe stores cumulative series
  only; storing raw delta values would silently break `rate()` and
  `increase()`. Use the OTel SDK default (cumulative) or the collector's
  `deltatocumulative` processor.
- Exponential histogram data points with negative buckets.
- Histogram data points whose `bucket_counts` length does not match
  `explicit_bounds` + 1.
- Data points with a zero `time_unix_nano` or no value.

Dropped silently (not counted): data points carrying the
`NO_RECORDED_VALUE` staleness flag — they have no value by definition and
gigapipe's query engine derives staleness from its own fill window.

## Exemplars

Every exemplar is stored in `metric_exemplars` with its series' fingerprint
(on plain series and `_bucket` series). The trace and span IDs are folded into
the exemplar's labels as `trace_id` and `span_id`, next to its filtered
attributes.

`/api/v1/query_exemplars` answers in Prometheus's shape: the exemplars in
`[start, end]` of every series the query's selectors pick, grouped per series
with the series' label set. A missing `start` or `end` leaves that side
unbounded. Exemplars live as long as raw samples (`METRICS_RAW_DAYS`), so the
answer is the same whether a chart was served from raw samples or from an
aggregate tier.
