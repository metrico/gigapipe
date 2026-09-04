package grpc

import (
	"context"
	"encoding/base64"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/model"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// authTestOpts are the Options used by every test in this file: auth
// enabled with a fixed user/pass pair.
var authTestOpts = Options{BasicAuthUser: "user", BasicAuthPass: "pass"}

// startAuthTestServer boots the real Mux/http.Server stack (not a bare
// grpc.Server) over a bufconn listener with opts, and returns a dialed
// client connection plus the spans recorder to assert against.
func startAuthTestServer(t *testing.T, opts Options) (coltracepb.TraceServiceClient, *recorderSvc) {
	t.Helper()
	spans := installFakeRegistry(t)
	installFPCache(t, "n")
	installConfig(t)

	lis := bufconn.Listen(1 << 20)
	sentinel := &sentinelHandler{}
	httpServer := &http.Server{Handler: Mux(sentinel, opts), Protocols: Protocols()}
	go func() { _ = httpServer.Serve(lis) }()
	t.Cleanup(func() { httpServer.Close() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return coltracepb.NewTraceServiceClient(conn), spans
}

// basicAuthHeader base64-encodes "user:pass" the way a well-behaved client
// (or the OTel Collector's basicauth extension) would.
func basicAuthHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// assertUnauthenticatedAndEmpty exports req against client with the given
// outgoing context, asserting BOTH that the RPC failed with
// codes.Unauthenticated AND that the insert recorder received nothing --
// a status-code check alone would not prove the business handler was
// skipped.
func assertUnauthenticatedAndEmpty(t *testing.T, ctx context.Context, client coltracepb.TraceServiceClient, spans *recorderSvc) {
	t.Helper()
	_, err := client.Export(ctx, sampleTraceRequest())
	if err == nil {
		t.Fatalf("expected export to fail with Unauthenticated, got success")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("expected codes.Unauthenticated, got %v (err=%v)", got, err)
	}

	// Give any (incorrect) async path a moment to land, then assert nothing
	// arrived.
	time.Sleep(20 * time.Millisecond)
	if got := spans.reqs(); len(got) != 0 {
		t.Fatalf("expected insert recorder to receive nothing for an unauthenticated request, got %d requests", len(got))
	}
}

// TestAuthInterceptor_NoMetadata covers plan Task E case 1: auth enabled, no
// authorization metadata at all.
func TestAuthInterceptor_NoMetadata(t *testing.T) {
	client, spans := startAuthTestServer(t, authTestOpts)
	assertUnauthenticatedAndEmpty(t, context.Background(), client, spans)
}

// TestAuthInterceptor_WrongPassword covers plan Task E case 2.
func TestAuthInterceptor_WrongPassword(t *testing.T) {
	client, spans := startAuthTestServer(t, authTestOpts)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", basicAuthHeader("user", "wrong"))
	assertUnauthenticatedAndEmpty(t, ctx, client, spans)
}

// TestAuthInterceptor_Malformed covers plan Task E case 3: "Basic" alone (no
// payload), a Bearer-scheme header, and a non-base64 payload.
func TestAuthInterceptor_Malformed(t *testing.T) {
	cases := []struct {
		name string
		auth string
	}{
		{"BasicAlone", "Basic"},
		{"BearerScheme", "Bearer xyz"},
		{"NonBase64Payload", "Basic not-valid-base64!!!"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client, spans := startAuthTestServer(t, authTestOpts)
			ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", c.auth)
			assertUnauthenticatedAndEmpty(t, ctx, client, spans)
		})
	}
}

// TestAuthInterceptor_CorrectCredentials covers plan Task E case 4: correct
// credentials succeed, and a real span row reaches the insert layer.
func TestAuthInterceptor_CorrectCredentials(t *testing.T) {
	client, spans := startAuthTestServer(t, authTestOpts)
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", basicAuthHeader("user", "pass"),
		"x-ch-dsn", "test-dsn")

	if _, err := client.Export(ctx, sampleTraceRequest()); err != nil {
		t.Fatalf("export with correct credentials failed: %v", err)
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
	if len(ts.MName) == 0 || ts.MName[0] != "op" {
		t.Fatalf("span name did not flow through: %#v", ts)
	}
}

// TestAuthInterceptor_LowercaseScheme: the auth-scheme token is
// case-insensitive (RFC 7235 §2.1), so a "basic" header must authenticate.
// A successful Export is proof by itself that the interceptor passed the
// request through.
func TestAuthInterceptor_LowercaseScheme(t *testing.T) {
	client, _ := startAuthTestServer(t, authTestOpts)
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "basic "+base64.StdEncoding.EncodeToString([]byte("user:pass")),
		"x-ch-dsn", "test-dsn")

	if _, err := client.Export(ctx, sampleTraceRequest()); err != nil {
		t.Fatalf("export with lowercase scheme failed: %v", err)
	}
}

// TestAuthInterceptor_Disabled covers plan Task E case 5: with Options{}
// (empty user/pass), auth is disabled entirely and export succeeds with no
// metadata at all. TestGRPCTraces_EndToEnd in integration_test.go already
// exercises this path through Mux(sentinel, Options{}); this test exists
// alongside the negative-control discipline of this file to make the
// "disabled" contract explicit here too.
func TestAuthInterceptor_Disabled(t *testing.T) {
	client, spans := startAuthTestServer(t, Options{})
	if _, err := client.Export(context.Background(), sampleTraceRequest()); err != nil {
		t.Fatalf("export with auth disabled and no metadata failed: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(spans.reqs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(spans.reqs()) == 0 {
		t.Fatalf("expected export to reach the insert layer with auth disabled")
	}
}
