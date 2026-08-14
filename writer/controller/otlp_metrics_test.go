package controller

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

// metricsFakeRegistry satisfies registry.ServiceRegistry with recorder-backed
// samples, time-series and profile services, which is the set
// withTSAndSampleService resolves.
type metricsFakeRegistry struct {
	samples    *recorderSvc
	timeSeries *recorderSvc
	profile    *recorderSvc
}

func (f *metricsFakeRegistry) GetTimeSeriesService(id string) (service.IInsertServiceV2, error) {
	return f.timeSeries, nil
}
func (f *metricsFakeRegistry) GetSamplesService(id string) (service.IInsertServiceV2, error) {
	return f.samples, nil
}
func (f *metricsFakeRegistry) GetMetricsService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *metricsFakeRegistry) GetSpansService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *metricsFakeRegistry) GetSpansSeriesService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *metricsFakeRegistry) GetProfileInsertService(id string) (service.IInsertServiceV2, error) {
	return f.profile, nil
}
func (f *metricsFakeRegistry) GetPatternInsertService(id string) (service.IInsertServiceV2, error) {
	return nil, nil
}
func (f *metricsFakeRegistry) Run()  {}
func (f *metricsFakeRegistry) Stop() {}

// newMetricsHandler wires OTLPMetricsV2 with the production middleware chain
// (WithOverallContextMiddleware for DSN/gzip handling) against fake insert
// services, returning the handler plus the samples recorder to assert on.
func newMetricsHandler(t *testing.T) (http.HandlerFunc, *recorderSvc) {
	t.Helper()
	installConfig(t)
	installFPCache(t, "n")
	samples := &recorderSvc{}
	old := Registry
	Registry = &metricsFakeRegistry{samples: samples, timeSeries: &recorderSvc{}, profile: &recorderSvc{}}
	t.Cleanup(func() { Registry = old })
	return OTLPMetricsV2(NewMiddlewareConfig(WithOverallContextMiddleware)), samples
}

func sampleMetricsRequest() *metricsv1.MetricsData {
	return &metricsv1.MetricsData{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
		}}},
		ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{{
			Name: "cpu.usage",
			Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
				TimeUnixNano: 1700000000_000000000,
				Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: 42.5},
			}}}},
		}}}},
	}}}
}

func TestOTLPMetricsHTTP_ProtoHappyPath(t *testing.T) {
	handler, samples := newMetricsHandler(t)

	body, err := proto.Marshal(sampleMetricsRequest())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/v1/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/x-protobuf" {
		t.Fatalf("expected protobuf response content type, got %q", ct)
	}
	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if err := proto.Unmarshal(w.Body.Bytes(), resp); err != nil {
		t.Fatalf("response is not an ExportMetricsServiceResponse: %v", err)
	}
	if resp.PartialSuccess != nil {
		t.Fatalf("expected unset partial_success on full success, got %+v", resp.PartialSuccess)
	}

	got := samples.reqs()
	if len(got) == 0 {
		t.Fatal("samples insert service received no requests")
	}
	spl := got[0].(*model.TimeSamplesData)
	if len(spl.MValue) != 1 || spl.MValue[0] != 42.5 {
		t.Fatalf("expected one sample with value 42.5, got %#v", spl.MValue)
	}
}

func TestOTLPMetricsHTTP_JSONWithPartialSuccess(t *testing.T) {
	handler, samples := newMetricsHandler(t)

	// One cumulative gauge (stored) plus one delta sum (rejected by policy).
	// Temporality 1 = DELTA, encoded as an integer per the OTLP JSON mapping.
	body := `{"resourceMetrics":[{"scopeMetrics":[{"metrics":[
		{"name":"ok.gauge","gauge":{"dataPoints":[
			{"timeUnixNano":"1700000000000000000","asDouble":1.5}]}},
		{"name":"bad.delta","sum":{"aggregationTemporality":1,"isMonotonic":true,"dataPoints":[
			{"timeUnixNano":"1700000000000000000","asInt":"3"}]}}
	]}]}]}`
	req := httptest.NewRequest("POST", "/v1/metrics", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected json response content type, got %q", ct)
	}
	respBody := w.Body.String()
	if !strings.Contains(respBody, `"rejectedDataPoints":"1"`) {
		t.Fatalf("expected partial success with 1 rejected point, got %s", respBody)
	}
	if !strings.Contains(respBody, "temporality") {
		t.Fatalf("expected temporality in error message, got %s", respBody)
	}

	got := samples.reqs()
	if len(got) == 0 {
		t.Fatal("samples insert service received no requests")
	}
	spl := got[0].(*model.TimeSamplesData)
	if len(spl.MValue) != 1 || spl.MValue[0] != 1.5 {
		t.Fatalf("expected only the gauge sample, got %#v", spl.MValue)
	}
}

func TestOTLPMetricsHTTP_BadPayloadReturns400Status(t *testing.T) {
	handler, _ := newMetricsHandler(t)

	req := httptest.NewRequest("POST", "/v1/metrics", strings.NewReader("\x01\x02 not a proto"))
	req.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	st := &spb.Status{}
	if err := proto.Unmarshal(w.Body.Bytes(), st); err != nil {
		t.Fatalf("error body is not a google.rpc.Status: %v", err)
	}
	if st.Message == "" {
		t.Fatal("expected a non-empty status message")
	}
}

func TestOTLPMetricsHTTP_UnsupportedContentType(t *testing.T) {
	handler, _ := newMetricsHandler(t)

	req := httptest.NewRequest("POST", "/v1/metrics", strings.NewReader("a,b,c"))
	req.Header.Set("Content-Type", "text/csv")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported content type, got %d", w.Code)
	}
}

func TestOTLPMetricsHTTP_GzipRequest(t *testing.T) {
	handler, samples := newMetricsHandler(t)

	raw, err := proto.Marshal(sampleMetricsRequest())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("POST", "/v1/metrics", &buf)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(samples.reqs()) == 0 {
		t.Fatal("gzip request did not reach the insert layer")
	}
}

func TestOTLPMetricsHTTP_OversizeBodyReturns413(t *testing.T) {
	handler, _ := newMetricsHandler(t)

	// A gzip body that decompresses beyond the 64 MiB limit proves the limit
	// applies to the decompressed stream, not the wire bytes.
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := io.CopyN(zw, zeroReader{}, otlpMetricsMaxBodySize+2); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("POST", "/v1/metrics", &buf)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
