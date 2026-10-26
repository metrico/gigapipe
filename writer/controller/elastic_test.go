package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/writer/model"
)

// newElasticRouter mounts the elastic routes on fake insert services and hands
// back the time-series recorder. The assertions have to go through a real
// router: the defect these tests pin was the controller reading the route
// variables from a context key nobody writes, which every parser-level test
// would happily pass over.
func newElasticRouter(t *testing.T) (*http.ServeMux, *recorderSvc) {
	t.Helper()
	installConfig(t)
	installFPCache(t, "n")
	ts := &recorderSvc{}
	old := Registry
	Registry = &metricsFakeRegistry{samples: &recorderSvc{}, timeSeries: ts, profile: &recorderSvc{}}
	t.Cleanup(func() { Registry = old })

	cfg := NewMiddlewareConfig(WithOverallContextMiddleware)
	router := http.NewServeMux()
	router.HandleFunc("POST /{target}/_doc", TargetDocV2(cfg))
	router.HandleFunc("PUT /{target}/_doc/{id}", TargetDocV2(cfg))
	router.HandleFunc("POST /{target}/_bulk", TargetBulkV2(cfg))
	router.HandleFunc("POST /_bulk", TargetBulkV2(cfg))
	return router, ts
}

func ingest(t *testing.T, router *http.ServeMux, method, path, body string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("%s %s: status %d: %s", method, path, w.Code, w.Body.String())
	}
}

// labelSets returns the labels of every series the time-series service saw.
func labelSets(t *testing.T, ts *recorderSvc) []map[string]string {
	t.Helper()
	var out []map[string]string
	for _, req := range ts.reqs() {
		for _, l := range req.(*model.TimeSeriesData).MLabels {
			labels := map[string]string{}
			if err := json.Unmarshal([]byte(l), &labels); err != nil {
				t.Fatalf("labels %q: %v", l, err)
			}
			out = append(out, labels)
		}
	}
	return out
}

func TestElasticDocTakesTheIndexFromThePath(t *testing.T) {
	router, ts := newElasticRouter(t)

	ingest(t, router, http.MethodPost, "/idx1/_doc", `{"message":"hello"}`)

	got := labelSets(t, ts)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1: %v", len(got), got)
	}
	if got[0]["_index"] != "idx1" {
		t.Errorf("_index = %q, want %q", got[0]["_index"], "idx1")
	}
	// A route without {id} must not label the document with an empty one.
	if id, ok := got[0]["_id"]; ok {
		t.Errorf("_id = %q, want no _id label at all", id)
	}
}

func TestElasticDocTakesTheIDFromThePath(t *testing.T) {
	router, ts := newElasticRouter(t)

	ingest(t, router, http.MethodPut, "/idx1/_doc/42", `{"message":"hello"}`)

	got := labelSets(t, ts)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1: %v", len(got), got)
	}
	if got[0]["_index"] != "idx1" || got[0]["_id"] != "42" {
		t.Errorf("labels = %v, want _index=idx1 _id=42", got[0])
	}
}

// The path supplies the default index for a bulk request, and the action line
// overrides it, as in Elasticsearch.
func TestElasticBulkActionLineOverridesThePathIndex(t *testing.T) {
	router, ts := newElasticRouter(t)

	ingest(t, router, http.MethodPost, "/idx1/_bulk", strings.Join([]string{
		`{"index":{"_id":"1"}}`,
		`{"message":"one"}`,
		`{"create":{"_index":"other","_id":"2"}}`,
		`{"message":"two"}`,
	}, "\n"))

	byID := map[string]map[string]string{}
	for _, labels := range labelSets(t, ts) {
		byID[labels["_id"]] = labels
	}
	if len(byID) != 2 {
		t.Fatalf("got %d series, want 2: %v", len(byID), byID)
	}
	if got := byID["1"]["_index"]; got != "idx1" {
		t.Errorf("_index of the doc without one = %q, want the path's %q", got, "idx1")
	}
	if got := byID["2"]["_index"]; got != "other" {
		t.Errorf("_index of the doc with its own = %q, want %q", got, "other")
	}
}

// Without a target in the path an action line has to carry its own index:
// Elasticsearch rejects a document with no index at all.
func TestElasticBulkRejectsAMissingIndex(t *testing.T) {
	router, _ := newElasticRouter(t)

	req := httptest.NewRequest(http.MethodPost, "/_bulk", strings.NewReader(
		`{"index":{"_id":"1"}}`+"\n"+`{"message":"one"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
}
