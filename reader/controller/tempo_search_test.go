package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
)

type fakeTempoSearch struct {
	model.ITempoService
	traces    []model.TraceInfo
	responses []*model.TraceResponse
	totalJobs int64
	err       error
}

func (f *fakeTempoSearch) SearchTraceQL(context.Context, string, int, int, time.Time, time.Time) (chan []model.TraceInfo, int64, error) {
	if f.err != nil {
		return nil, 0, f.err
	}
	ch := make(chan []model.TraceInfo, 1)
	ch <- f.traces
	close(ch)
	return ch, f.totalJobs, nil
}

func (f *fakeTempoSearch) Search(context.Context, string, int64, int64, int, int64, int64) (chan *model.TraceResponse, error) {
	ch := make(chan *model.TraceResponse, len(f.responses))
	for _, r := range f.responses {
		ch <- r
	}
	close(ch)
	return ch, nil
}

func search(service *fakeTempoSearch, target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	(&TempoController{Service: service}).Search(w, httptest.NewRequest(http.MethodGet, target, nil))
	return w
}

func TestSearchResponseEndsWithJobCounts(t *testing.T) {
	const q = "/api/search?q=%7B%7D"
	const tags = "/api/search?tags=a%3Db"
	trace := []model.TraceInfo{{TraceID: "aa"}}
	for name, tc := range map[string]struct {
		service *fakeTempoSearch
		target  string
		traces  int
		tail    string
	}{
		"unportioned": {&fakeTempoSearch{traces: trace, totalJobs: 1}, q, 1,
			`],"metrics":{"totalJobs":1,"completedJobs":1}}`},
		"portioned": {&fakeTempoSearch{traces: trace, totalJobs: 3}, q, 1,
			`],"metrics":{"totalJobs":3,"completedJobs":3}}`},
		"no traces": {&fakeTempoSearch{totalJobs: 3}, q, 0,
			`{"traces": [],"metrics":{"totalJobs":3,"completedJobs":3}}`},
		"tag search": {&fakeTempoSearch{responses: []*model.TraceResponse{{TraceID: "aa"}}}, tags, 1,
			`],"metrics":{"totalJobs":1,"completedJobs":1}}`},
		"tag search without traces": {&fakeTempoSearch{}, tags, 0,
			`{"traces": [],"metrics":{"totalJobs":1,"completedJobs":1}}`},
	} {
		t.Run(name, func(t *testing.T) {
			w := search(tc.service, tc.target)
			body := w.Body.String()
			if w.Code != http.StatusOK || !strings.HasSuffix(body, tc.tail) {
				t.Fatalf("status %d, body %s, want tail %s", w.Code, body, tc.tail)
			}
			var parsed struct {
				Traces []json.RawMessage `json:"traces"`
			}
			if err := json.Unmarshal([]byte(body), &parsed); err != nil {
				t.Fatalf("%v: %s", err, body)
			}
			if len(parsed.Traces) != tc.traces {
				t.Errorf("got %d traces, want %d: %s", len(parsed.Traces), tc.traces, body)
			}
		})
	}
}

func TestSearchFailureHasNoJobCounts(t *testing.T) {
	w := search(&fakeTempoSearch{err: errors.New("boom")}, "/api/search?q=%7B%7D")
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status %d, want 500", w.Code)
	}
	if strings.Contains(w.Body.String(), "metrics") {
		t.Errorf("error body has job counts: %s", w.Body.String())
	}
}
