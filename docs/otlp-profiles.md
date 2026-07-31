# OTLP Profiles Ingestion

gigapipe accepts the OpenTelemetry profiles signal (`profiles/v1development`)
over HTTP protobuf at:

    POST /v1development/profiles      Content-Type: application/x-protobuf

Only the protobuf encoding is supported. Requests with
`Content-Type: application/json` are rejected with `415 Unsupported Media Type`.
A successful request returns `200 OK`; a request that carries no profiles is a
no-op that still returns `200 OK`.

## Routing via an OpenTelemetry Collector

No collector code changes are required; a profiles-capable collector
(otlpreceiver + otlphttpexporter, v0.154+) forwards profiles to gigapipe:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:
exporters:
  otlphttp:
    endpoint: http://<gigapipe-writer-host>:<port>   # posts to /v1development/profiles
service:
  pipelines:
    profiles:
      receivers:  [otlp]
      exporters:  [otlphttp]
```

Do not route profiles through `pyroscopereceiver`: it emits `plog.Logs`, not the
profiles signal.

## Storage & query

Ingested OTLP profiles land in the same ClickHouse `profiles` tables as Pyroscope
profiles and are queryable through the existing Pyroscope-compatible reader API
(flamegraph, series, stacktraces). Ingested rows carry the `otel_v1development`
payload type internally so the reader can decode them alongside native Pyroscope
payloads. The `type` label is derived from the OTLP `sample_type` type string
(e.g. `cpu`, `samples`).
