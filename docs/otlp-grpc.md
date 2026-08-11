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

## Authentication

OTLP/gRPC requests bypass the HTTP mux's middleware chain, but two of its
middlewares are ported as gRPC unary interceptors: basic auth and access
logging. (CORS and response gzip are not — see [Limitations](#limitations).)

Auth is off by default. It turns on exactly when it does on the HTTP side:
when both `AUTH_SETTINGS.BASIC.Username` and `AUTH_SETTINGS.BASIC.Password`
are set. When it's on, every gRPC request must carry an `authorization`
metadata key with value `Basic <base64(user:pass)>`. A missing key, a
malformed value, or wrong credentials are all rejected with gRPC status code
`Unauthenticated` (16).

**Warning:** the gRPC port is cleartext HTTP/2 — there is no TLS on this
path (see [Where it listens](#where-it-listens)). Basic-auth credentials are
base64, not encryption, so they travel in the clear. Only enable
`AUTH_SETTINGS.BASIC` for gRPC over a trusted network, or put a TLS-terminating
proxy capable of HTTP/2 prior-knowledge in front of gigapipe.

An OpenTelemetry Collector authenticates with the `basicauth` extension,
wired to the `otlp` exporter via `auth:`:

```yaml
extensions:
  basicauth:
    client_auth:
      username: <user>
      password: <pass>

exporters:
  otlp:
    endpoint: <gigapipe-host>:3100
    tls:
      insecure: true
    auth:
      authenticator: basicauth

service:
  extensions: [basicauth]
  pipelines:
    traces:
      receivers:  [otlp]
      exporters:  [otlp]
    logs:
      receivers:  [otlp]
      exporters:  [otlp]
```

gRPC requests also now appear in the access log, in the same line format as
HTTP requests, with the gRPC status code and the full method name (e.g.
`/opentelemetry.proto.collector.trace.v1.TraceService/Export`) standing in
for the HTTP status and URL.

## Limitations

- **CORS and response gzip do not apply to gRPC.** CORS is a browser
  preflight mechanism — gRPC clients never send `Origin` and never issue
  `OPTIONS` preflights, and a browser cannot make an `application/grpc`
  HTTP/2 request at all (that's what grpc-web is for, and grpc-web isn't
  supported here). Response gzip compresses based on `Accept-Encoding`;
  gRPC negotiates per-message compression itself via `grpc-encoding` /
  `grpc-accept-encoding`, which already works, and OTLP export responses are
  empty messages anyway.
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
