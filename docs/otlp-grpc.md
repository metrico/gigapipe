# OTLP over gRPC

gigapipe accepts OpenTelemetry (OTLP) data over **gRPC** in addition to the
existing OTLP/HTTP endpoints. The gRPC receiver is disabled by default and is
enabled by setting the `OTLP_GRPC_ADDR` environment variable.

## Enabling the receiver

Set `OTLP_GRPC_ADDR` to a listen address. `4317` is the OTel-convention gRPC
port; OTLP/HTTP continues to be served on the existing HTTP port (`PORT`,
default `3100`).

```bash
OTLP_GRPC_ADDR=:4317
```

When `OTLP_GRPC_ADDR` is unset or empty the receiver is not started, no listener
is opened, and existing behavior is unchanged.

## Supported signals

| Signal    | OTLP/gRPC | OTLP/HTTP |
|-----------|-----------|-----------|
| Traces    | ✅        | ✅        |
| Logs      | ✅        | ✅        |
| Profiles  | ✅        | ✅ (`/v1development/profiles`) |

## Multitenancy and per-request options

The tenant and per-request options are selected via gRPC metadata, mirroring the
equivalent OTLP/HTTP headers:

| gRPC metadata key | HTTP header       | Purpose                                          |
|-------------------|-------------------|--------------------------------------------------|
| `x-ch-dsn`        | `X-CH-DSN`        | Selects the tenant (ClickHouse DSN)              |
| `x-scope-meta`    | `X-Scope-Meta`    | Scope metadata                                   |
| `x-ttl-days`      | `X-Ttl-Days`      | Per-request TTL in days                          |

## Routing via an OpenTelemetry Collector

A collector can export to the gigapipe gRPC receiver with `otlp` (gRPC) instead
of, or alongside, `otlphttp`:

```yaml
exporters:
  otlp:
    endpoint: <gigapipe-writer-host>:4317
    headers:
      x-ch-dsn: <tenant-dsn>
service:
  pipelines:
    traces:
      receivers:  [otlp]
      exporters:  [otlp]
    logs:
      receivers:  [otlp]
      exporters:  [otlp]
```

For profiles specifically, see [OTLP Profiles Ingestion](otlp-profiles.md).

## Graceful shutdown

On `SIGTERM`/`SIGINT` the gRPC receiver drains in-flight requests alongside the
HTTP server during graceful shutdown.
