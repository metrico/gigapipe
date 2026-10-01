package controller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/writer/model"
)

// installMetricRouteRegistry installs recording log and metric services with
// the metric caches, and returns the registry to assert on.
func installMetricRouteRegistry(t *testing.T) *metricsFakeRegistry {
	t.Helper()
	installConfig(t)
	installFPCache(t, "n")
	installMetricCaches(t)
	reg := &metricsFakeRegistry{samples: &recorderSvc{}, timeSeries: &recorderSvc{}, profile: &recorderSvc{},
		staging: &recorderSvc{}, series: &recorderSvc{}}
	old := Registry
	Registry = reg
	t.Cleanup(func() { Registry = old })
	return reg
}

func stagingValues(t *testing.T, svc *recorderSvc) []float64 {
	t.Helper()
	var res []float64
	for _, r := range svc.reqs() {
		res = append(res, r.(*model.MetricSamplesData).MValue...)
	}
	return res
}

func TestDatadogMetricsRouteUsesTheMetricServices(t *testing.T) {
	reg := installMetricRouteRegistry(t)
	handler := PushDatadogMetricsV2(NewMiddlewareConfig(WithOverallContextMiddleware))
	req := httptest.NewRequest("POST", "/api/v2/series",
		strings.NewReader(`{"series":[{"metric":"m","points":[{"timestamp":1700000000,"value":2}]}]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202: %s", w.Code, w.Body.String())
	}
	if got := stagingValues(t, reg.staging); len(got) != 1 || got[0] != 2 {
		t.Fatalf("staging values: got %v, want [2]", got)
	}
	if len(reg.samples.reqs()) != 0 || len(reg.timeSeries.reqs()) != 0 {
		t.Fatal("Datadog metrics reached a log insert service")
	}
}
