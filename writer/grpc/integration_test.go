package grpc

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
	"unsafe"

	clconfig "github.com/metrico/cloki-config"
	clokiconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	"github.com/metrico/qryn/v5/writer/utils/promise"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// recorderSvc is a minimal IInsertServiceV2 that records every request routed to
// it and returns an already-fulfilled promise so doPush completes at once. It
// mirrors the recorder in writer/controller/ingest_test.go (that file is package
// controller and cannot be imported here).
type recorderSvc struct {
	node     string
	mu       sync.Mutex
	received []helpers.SizeGetter
}

func (r *recorderSvc) Request(req helpers.SizeGetter, insertMode int) *promise.Promise[uint32] {
	r.mu.Lock()
	r.received = append(r.received, req)
	r.mu.Unlock()
	return promise.Fulfilled[uint32](nil, 0)
}

func (r *recorderSvc) reqs() []helpers.SizeGetter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]helpers.SizeGetter(nil), r.received...)
}

func (r *recorderSvc) Run()                     {}
func (r *recorderSvc) Stop()                    {}
func (r *recorderSvc) Ping() (time.Time, error) { return time.Time{}, nil }
func (r *recorderSvc) GetState(insertMode int) int {
	return service.INSERT_STATE_IDLE
}
func (r *recorderSvc) GetNodeName() string {
	if r.node != "" {
		return r.node
	}
	return "n"
}
func (r *recorderSvc) Init()      {}
func (r *recorderSvc) PlanFlush() {}

// fakeRegistry is an in-memory registry.ServiceRegistry test double. For traces,
// ResolveTraceServices reads GetSpansSeriesService (SpanAttrs slot) and
// GetSpansService (Spans slot); every other method exists only to satisfy the
// interface and returns nil.
type fakeRegistry struct {
	spans     *recorderSvc
	spanAttrs *recorderSvc
}

func (f *fakeRegistry) GetTimeSeriesService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *fakeRegistry) GetSamplesService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *fakeRegistry) GetMetricsService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *fakeRegistry) GetSpansService(id string) (service.IInsertServiceV2, error) {
	return f.spans, nil
}
func (f *fakeRegistry) GetSpansSeriesService(id string) (service.IInsertServiceV2, error) {
	return f.spanAttrs, nil
}
func (f *fakeRegistry) GetProfileInsertService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *fakeRegistry) GetPatternInsertService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *fakeRegistry) Run()  {}
func (f *fakeRegistry) Stop() {}

// installFakeRegistry installs a fakeRegistry (save/restore) whose trace insert
// services record pushed rows, and returns the spans recorder to assert on.
func installFakeRegistry(t *testing.T) *recorderSvc {
	t.Helper()
	spans := &recorderSvc{node: "n"}
	fr := &fakeRegistry{spans: spans, spanAttrs: &recorderSvc{node: "n"}}
	old := controller.Registry
	controller.Registry = fr
	t.Cleanup(func() { controller.Registry = old })
	return spans
}

// installFPCache registers a trivial per-node fingerprint cache for node "n" and
// restores the previous global on cleanup. IngestParsed resolves FPCache.DB(node)
// before running the parser, so a real (if trivial) cache must exist.
func installFPCache(t *testing.T, node string) {
	t.Helper()
	old := controller.FPCache
	controller.FPCache = numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{node: {}})
	t.Cleanup(func() {
		controller.FPCache.Stop()
		controller.FPCache = old
	})
}

// installConfig sets a minimal global config so doPush can read retry settings
// without a nil-pointer panic. One retry attempt with no delay keeps the push
// path fast and deterministic.
func installConfig(t *testing.T) {
	t.Helper()
	old := config.Cloki
	setting := &clokiconfig.ClokiBaseSettingServer{}
	setting.SYSTEM_SETTINGS.RetryAttempts = 1
	setting.SYSTEM_SETTINGS.RetryTimeoutS = 0
	config.Cloki = &clconfig.ClokiConfig{Setting: setting}
	t.Cleanup(func() { config.Cloki = old })
}

// sampleTraceRequest builds an ExportTraceServiceRequest carrying one span under
// one resource (service.name=svc) and one scope. TraceId is 16 bytes, SpanId 8.
func sampleTraceRequest() *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
				Key:   "service.name",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
			}}},
			ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{{
				Name:    "op",
				TraceId: []byte("0123456789abcdef"),
				SpanId:  []byte("01234567"),
			}}}},
		}},
	}
}

// sentinelHandler is a plain http.Handler that records whether it was
// invoked, used to prove gRPC requests never reach the non-gRPC fallback
// handler (and that non-gRPC requests do).
type sentinelHandler struct {
	mu     sync.Mutex
	called bool
}

func (s *sentinelHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.called = true
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (s *sentinelHandler) wasCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.called
}

// TestGRPCTraces_EndToEnd drives the OTLP gRPC TraceService over an in-memory
// bufconn connection, served through the same production stack used in
// production (Mux + Protocols on a shared http.Server, not a bare
// grpc.Server), and asserts a span row reaches the insert layer (the Spans
// recorder receives at least one pushed request). It also asserts the gRPC
// request never falls through to the sentinel non-gRPC handler.
func TestGRPCTraces_EndToEnd(t *testing.T) {
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
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-ch-dsn", "test-dsn")
	client := coltracepb.NewTraceServiceClient(conn)
	if _, err := client.Export(ctx, sampleTraceRequest()); err != nil {
		t.Fatalf("export failed: %v", err)
	}

	// IngestParsed pushes synchronously within Export, so the recorder should
	// already hold the request; a short bounded wait guards against any async.
	deadline := time.Now().Add(2 * time.Second)
	for len(spans.reqs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := spans.reqs()
	if len(got) == 0 {
		t.Fatalf("spans insert service received no requests; expected at least one span row")
	}

	// Count alone is vacuous: doParseSpans always emits a ParserResponse whose
	// SpansRequest is a non-nil &model.TempoSamples{} even for an empty span
	// list, so an empty span input would still record a request. Inspect the
	// payload to prove an actual span row — with THIS span's identity — reached
	// the insert layer. (Negative guard: an empty ScopeSpans.Spans slice would
	// leave MTraceId/MName empty and fail here.)
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

	if sentinel.wasCalled() {
		t.Fatalf("sentinel non-gRPC handler was called for a gRPC request; Mux dispatch is broken")
	}
}

// TestMux_HTTPPassthrough proves that plain HTTP/1.1 requests are routed to
// next unchanged — Mux's gRPC dispatch does not swallow ordinary routes.
func TestMux_HTTPPassthrough(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	sentinel := &sentinelHandler{}
	httpServer := &http.Server{Handler: Mux(sentinel, Options{}), Protocols: Protocols()}
	go func() { _ = httpServer.Serve(lis) }()
	defer httpServer.Close()

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return lis.Dial()
			},
		},
	}

	resp, err := client.Get("http://bufnet/")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if !sentinel.wasCalled() {
		t.Fatalf("sentinel handler was not called for a plain HTTP/1.1 request")
	}
}
