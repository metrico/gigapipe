package grpc

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/model"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// NOTE: this file must NOT import google.golang.org/grpc/encoding/gzip, not
// even blank. Compressor registration in grpc-go is process-global via
// package init(); if this test binary imported the gzip codec anywhere, the
// server side would have it registered regardless of whether
// writer/grpc/server.go carries its own import, and TestGRPCTraces_EndToEnd_Gzip
// would pass even with the production import reverted. The client below uses
// grpc.UseCompressor("gzip") by name only — it relies entirely on
// server.go's blank import to make "gzip" resolvable process-wide.

// TestGRPCTraces_EndToEnd_Gzip is the same shape as TestGRPCTraces_EndToEnd,
// but the client compresses the request with gzip, as the OpenTelemetry
// Collector's OTLP gRPC exporter does by default. It exists to guard the
// blank gzip codec import in writer/grpc/server.go: without that import the
// server cannot decompress and this test fails with an "Unimplemented:
// Decompressor is not installed" error.
func TestGRPCTraces_EndToEnd_Gzip(t *testing.T) {
	spans := installFakeRegistry(t)
	installFPCache(t, "n")
	installConfig(t)

	lis := bufconn.Listen(1 << 20)
	sentinel := &sentinelHandler{}
	httpServer := &http.Server{Handler: Mux(sentinel, Options{}), Protocols: Protocols()}
	go func() { _ = httpServer.Serve(lis) }()
	defer httpServer.Close()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-ch-dsn", "test-dsn")
	client := coltracepb.NewTraceServiceClient(conn)
	if _, err := client.Export(ctx, sampleTraceRequest()); err != nil {
		t.Fatalf("gzip export failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(spans.reqs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := spans.reqs()
	if len(got) == 0 {
		t.Fatalf("spans insert service received no requests; expected at least one span row")
	}
	ts, ok := got[0].(*model.TempoSamples)
	if !ok {
		t.Fatalf("spans request had unexpected type %T", got[0])
	}
	if len(ts.MTraceId) == 0 || len(ts.MName) == 0 {
		t.Fatalf("spans request carried no span rows: %#v", ts)
	}
	if ts.MName[0] != "op" {
		t.Fatalf("span name did not flow through: got %q, want %q", ts.MName[0], "op")
	}
}

// largeTraceRequest builds an ExportTraceServiceRequest whose serialized size
// is comfortably above grpc-go's default 4 MiB receive cap (roughly 5-6 MB
// here), using a small number of spans that each carry one large attribute
// string value rather than many tiny spans, to keep the test fast.
func largeTraceRequest() *coltracepb.ExportTraceServiceRequest {
	const spanCount = 6
	const valueSize = 1 << 20 // 1 MB per attribute value
	bigValue := strings.Repeat("x", valueSize)

	spans := make([]*tracev1.Span, spanCount)
	for i := 0; i < spanCount; i++ {
		spans[i] = &tracev1.Span{
			Name:    "op",
			TraceId: []byte("0123456789abcdef"),
			SpanId:  []byte("01234567"),
			Attributes: []*commonv1.KeyValue{{
				Key:   "payload",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: bigValue}},
			}},
		}
	}

	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
				Key:   "service.name",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
			}}},
			ScopeSpans: []*tracev1.ScopeSpans{{Spans: spans}},
		}},
	}
}

// TestGRPCTraces_EndToEnd_LargeMessage proves a request larger than grpc-go's
// default 4 MiB receive cap is accepted, guarding the grpc.MaxRecvMsgSize
// option set in NewServer (see maxRecvMsgSize in writer/grpc/server.go).
// Without that option this test fails with a ResourceExhausted error.
func TestGRPCTraces_EndToEnd_LargeMessage(t *testing.T) {
	spans := installFakeRegistry(t)
	installFPCache(t, "n")
	installConfig(t)

	req := largeTraceRequest()

	lis := bufconn.Listen(1 << 20)
	sentinel := &sentinelHandler{}
	httpServer := &http.Server{Handler: Mux(sentinel, Options{}), Protocols: Protocols()}
	go func() { _ = httpServer.Serve(lis) }()
	defer httpServer.Close()

	// The client must also be willing to send/receive a message this size;
	// grpc-go's client-side defaults for send are effectively unbounded for
	// our purposes but the receive default on the client applies to
	// responses only (the Export response here is tiny), so no client-side
	// size option is required here beyond a generous max call recv size for
	// safety.
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16<<20)))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-ch-dsn", "test-dsn")
	client := coltracepb.NewTraceServiceClient(conn)
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("large export failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(spans.reqs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := spans.reqs()
	if len(got) == 0 {
		t.Fatalf("spans insert service received no requests; expected at least one span row")
	}
	ts, ok := got[0].(*model.TempoSamples)
	if !ok {
		t.Fatalf("spans request had unexpected type %T", got[0])
	}
	if len(ts.MTraceId) == 0 || len(ts.MName) == 0 {
		t.Fatalf("spans request carried no span rows: %#v", ts)
	}
}
