//go:build integration

// Package integration contains Docker-backed regression tests for the reflected
// XSS / Content-Type hardening in the global response middleware. They verify,
// against a real running gigapipe + ClickHouse, that:
//
//   - every response carries X-Content-Type-Options: nosniff and never declares
//     text/html (the go/reflected-xss guarantee), and
//   - the traces (Tempo protobuf) and profiles (Connect) endpoints still return
//     their correct negotiated Content-Type (no regression from the hardening).
//
// Run via test/integration/run.sh, or against an already-running stack:
//
//	GIGAPIPE_URL=http://localhost:3100 go test -tags integration ./test/integration/... -v
package integration

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	querierv1 "github.com/metrico/qryn/v4/reader/prof"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func baseURL() string {
	if u := os.Getenv("GIGAPIPE_URL"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://localhost:3100"
}

// assertSafeHeaders enforces the two invariants that defuse MIME-sniffing based
// reflected XSS on every response, regardless of endpoint.
func assertSafeHeaders(t *testing.T, resp *http.Response) {
	t.Helper()
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if ct := strings.ToLower(resp.Header.Get("Content-Type")); strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q declares HTML; reflected input would be executable", ct)
	}
}

func waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL() + "/api/v1/status/buildinfo")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("gigapipe at %s never became ready", baseURL())
}

func TestMain(m *testing.M) {
	// Give a brief grace period so `go test` invoked right after `up` still waits.
	os.Exit(m.Run())
}

// --- Profiles (Connect protocol) ------------------------------------------------

// TestProfiles_ProtoRequest_ReturnsProto guards the prof.go writeResponse fix:
// a Connect binary request (application/proto) must get application/proto back
// (not application/x-protobuf, which strict Connect clients reject) plus nosniff.
func TestProfiles_ProtoRequest_ReturnsProto(t *testing.T) {
	waitReady(t)
	nowMs := time.Now().UnixMilli()
	body, err := proto.Marshal(&querierv1.ProfileTypesRequest{Start: nowMs - 3_600_000, End: nowMs})
	if err != nil {
		t.Fatal(err)
	}
	resp := postProfileTypes(t, "application/proto", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %q", resp.StatusCode, raw)
	}
	assertSafeHeaders(t, resp)
	if ct := resp.Header.Get("Content-Type"); ct != "application/proto" {
		t.Errorf("Content-Type = %q, want application/proto", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out querierv1.ProfileTypesResponse
	if err := proto.Unmarshal(raw, &out); err != nil {
		t.Fatalf("response not decodable as protobuf: %v", err)
	}
}

// TestProfiles_JSONRequest_ReturnsJSON confirms the JSON negotiation path is
// untouched by the fix.
func TestProfiles_JSONRequest_ReturnsJSON(t *testing.T) {
	waitReady(t)
	nowMs := time.Now().UnixMilli()
	// The server parses JSON with encoding/json (numeric int64), not protojson
	// (which would string-encode the ints), so build the body the same way.
	body, err := json.Marshal(&querierv1.ProfileTypesRequest{Start: nowMs - 3_600_000, End: nowMs})
	if err != nil {
		t.Fatal(err)
	}
	resp := postProfileTypes(t, "application/json", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %q", resp.StatusCode, raw)
	}
	assertSafeHeaders(t, resp)
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func postProfileTypes(t *testing.T, contentType string, body []byte) *http.Response {
	t.Helper()
	url := baseURL() + "/querier.v1.QuerierService/ProfileTypes"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	// Disable transparent gzip so we can assert on the raw response headers.
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// --- Traces (Tempo protobuf) ----------------------------------------------------

// TestTraces_ProtobufRoundTrip guards the tempo.go fix: after ingesting a trace,
// GET /api/traces/{id} with Accept: application/protobuf must return
// application/protobuf (matching the negotiated Accept) plus nosniff, and the
// body must decode back to the ingested trace.
func TestTraces_ProtobufRoundTrip(t *testing.T) {
	waitReady(t)

	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
	spanID := []byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	nowNs := uint64(time.Now().UnixNano())

	td := &tracev1.TracesData{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "xss-itest"}},
		}}},
		ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              "itest-span",
			StartTimeUnixNano: nowNs,
			EndTimeUnixNano:   nowNs + 1_000_000,
		}}}},
	}}}
	payload, err := proto.Marshal(td)
	if err != nil {
		t.Fatal(err)
	}

	ingest, err := http.NewRequest(http.MethodPost, baseURL()+"/v1/traces", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	ingest.Header.Set("Content-Type", "application/x-protobuf")
	ir, err := http.DefaultClient.Do(ingest)
	if err != nil {
		t.Fatal(err)
	}
	ir.Body.Close()
	if ir.StatusCode/100 != 2 {
		t.Fatalf("ingest status = %d", ir.StatusCode)
	}

	hexID := hex.EncodeToString(traceID)
	startSec := time.Now().Add(-time.Hour).Unix()
	endSec := time.Now().Add(time.Hour).Unix()
	url := fmt.Sprintf("%s/api/traces/%s?start=%d&end=%d", baseURL(), hexID, startSec, endSec)

	// Poll: ingestion is asynchronous (bulk flush).
	deadline := time.Now().Add(30 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Accept", "application/protobuf")
		req.Header.Set("Accept-Encoding", "identity")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == http.StatusOK {
			assertSafeHeaders(t, resp)
			if ct := resp.Header.Get("Content-Type"); ct != "application/protobuf" {
				t.Errorf("Content-Type = %q, want application/protobuf", ct)
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var got tracev1.TracesData
			if err := proto.Unmarshal(raw, &got); err != nil {
				t.Fatalf("trace response not decodable as protobuf TracesData: %v", err)
			}
			if len(got.ResourceSpans) == 0 {
				t.Fatalf("trace round-trip returned no spans")
			}
			return
		}
		resp.Body.Close()
		if time.Now().After(deadline) {
			t.Fatalf("trace %s not queryable within timeout (last status %d)", hexID, resp.StatusCode)
		}
		time.Sleep(time.Second)
	}
}

// --- Reflected input, generic ---------------------------------------------------

// TestReflectedInput_NeverHTML sends an XSS payload in the request path and
// confirms the response can never be sniffed/rendered as HTML.
func TestReflectedInput_NeverHTML(t *testing.T) {
	waitReady(t)
	url := baseURL() + "/api/traces/%3Cscript%3Ealert(1)%3C/script%3E"
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	assertSafeHeaders(t, resp)
}
