# OTLP over gRPC

gigapipe accepts OpenTelemetry (OTLP) data over **gRPC** in addition to the
existing OTLP/HTTP endpoints. The gRPC receiver requires no configuration: it
is always on for nodes running in writer mode.

## Where it listens

There is no environment variable to set. On any node running with `MODE=all`,
`MODE=writer`, or `MODE` unset, the gRPC receiver is multiplexed onto the same
port as OTLP/HTTP — `PORT` (default `3100`). Requests are dispatched by
protocol: HTTP/2 requests with `Content-Type: application/grpc*` are routed to
the gRPC server, everything else is served as ordinary HTTP. Reader-only nodes
(`MODE=reader`) do not mount the gRPC receiver, since they have no write path.

The transport is cleartext HTTP/2 using prior-knowledge negotiation (no TLS,
no ALPN) — the same mode grpc-go clients use by default when configured for an
insecure channel. An HTTP/1.1-only proxy or load balancer placed in front of
gigapipe will break gRPC traffic even though OTLP/HTTP keeps working; put a
proxy capable of passing through HTTP/2 prior-knowledge (or terminate gRPC
before it, and forward OTLP/HTTP separately) if you need one in the path.

## Supported signals

| Signal    | OTLP/gRPC | OTLP/HTTP |
|-----------|-----------|-----------|
| Traces    | ✅        | ✅        |
| Logs      | ✅        | ✅        |
| Profiles  | ✅        | ✅ (`/v1development/profiles`) |

## Routing via an OpenTelemetry Collector

A collector can export to the gigapipe gRPC receiver with `otlp` (gRPC) instead
of, or alongside, `otlphttp`:

```yaml
exporters:
  otlp:
    endpoint: <gigapipe-host>:3100
    tls:
      insecure: true
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

On `SIGTERM`/`SIGINT` in-flight gRPC requests are drained by the shared HTTP
server's shutdown, alongside in-flight HTTP requests. See the second bullet
under [Limitations](#limitations) for why grpc-go's own `GracefulStop` plays no
part in this.

## Request compression and size limits

Requests compressed with **gzip** are supported and decompressed
transparently — this is what the OpenTelemetry Collector's OTLP gRPC
exporter sends by default, so no exporter configuration is needed to get
compression working.

The maximum accepted request size is **10 MB**, matching the limit on
OTLP/HTTP. Batches larger than that are rejected with a `ResourceExhausted`
error; if you hit it, tune the collector's `batch` processor to emit smaller
batches rather than raising the limit on the gigapipe side.

## Limitations

- **gRPC requests bypass the HTTP middleware chain.** OTLP/gRPC requests are
  dispatched straight to the gRPC server and never pass through the mux's
  logging, CORS, `AcceptEncodingMiddleware`, or `BasicAuthMiddleware`. If
  `AUTH_SETTINGS.BASIC` is configured, OTLP/HTTP requests are authenticated
  but OTLP/gRPC requests are not. This is a known gap; a gRPC auth
  interceptor that checks the `authorization` metadata key against the same
  credentials is planned follow-up work, not yet implemented.
- **`grpc.Server.GracefulStop` does not apply here.** Because the gRPC server
  is served through `ServeHTTP` on the shared `http.Server` rather than its
  own listener, grpc-go's own `GracefulStop` has no effect on it. Draining is
  handled entirely by the HTTP server's shutdown (`httpServer.Shutdown`),
  which closes idle connections and waits for in-flight requests, gRPC
  included, to finish.
- **Serving gRPC through `net/http`'s HTTP/2 stack is slower than grpc-go's
  native transport.** RPCs traverse `net/http`'s HTTP/2 implementation
  instead of grpc-go's own transport layer, which carries a measurable
  performance cost compared to a standalone grpc-go server. This is the
  accepted tradeoff for running OTLP/gRPC and OTLP/HTTP on a single port.
