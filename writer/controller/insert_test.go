package controller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLokiJSONPushWithValueIs400(t *testing.T) {
	reg := installMetricRouteRegistry(t)
	handler := PushStreamV2(NewMiddlewareConfig(WithOverallContextMiddleware))
	body := `{"streams":[{"stream":{"job":"a"},"values":[["1700000000000000000","ok"]]},
		{"stream":{"job":"b"},"entries":[{"ts":"1700000000000000000","value":1}]}]}`
	req := httptest.NewRequest("POST", "/loki/api/v1/push", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `{job=\"b\"}: entry 0`) {
		t.Fatalf("body %s must name the stream and the entry", w.Body.String())
	}
	if len(reg.samples.reqs()) != 0 || len(reg.timeSeries.reqs()) != 0 {
		t.Fatal("a rejected push reached the log insert services")
	}
}
