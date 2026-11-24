package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
)

// TestTrimTagScope: Grafana asks for the values of "resource.service.name" and
// "span.http.status_code", the attribute tables store the names without the scope.
func TestTrimTagScope(t *testing.T) {
	cases := map[string]string{
		"resource.service.name": "service.name",
		"span.http.status_code": "http.status_code",
		".service.name":         "service.name",
		"service.name":          "service.name",
		"name":                  "name",
		"status":                "status",
		"resourceful.tag":       "resourceful.tag",
	}
	for in, want := range cases {
		if got := trimTagScope(in); got != want {
			t.Errorf("trimTagScope(%q) = %q, want %q", in, got, want)
		}
	}
}

type tagRecorder struct {
	model.ITempoService
	tag string
}

func (s *tagRecorder) ValuesV2(_ context.Context, key string, _ string, _ time.Time, _ time.Time,
	_ int) (chan string, error) {
	s.tag = key
	res := make(chan string)
	close(res)
	return res, nil
}

// TestValuesV2StatusScope: only the unscoped status is the intrinsic, span.status
// is an attribute that happens to be called status.
func TestValuesV2StatusScope(t *testing.T) {
	cases := map[string]string{
		"status":          "otel.status_code",
		"span.status":     "status",
		"resource.status": "status",
	}
	for tag, want := range cases {
		svc := &tagRecorder{}
		ctrl := &TempoController{Service: svc}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v2/search/tag/{tag}/values", ctrl.ValuesV2)
		mux.ServeHTTP(httptest.NewRecorder(),
			httptest.NewRequest("GET", "/api/v2/search/tag/"+tag+"/values?start=1&end=2", nil))
		if svc.tag != want {
			t.Errorf("%q reads key %q, want %q", tag, svc.tag, want)
		}
	}
}
