package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
)

type fakeTempoSearch struct {
	model.ITempoService
	traces    []model.TraceInfo
	responses []*model.TraceResponse
	totalJobs int64
	err       error
	calls     map[string]int
}

func (f *fakeTempoSearch) called(name string) chan string {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[name]++
	ch := make(chan string)
	close(ch)
	return ch
}

func (f *fakeTempoSearch) Tags(context.Context) (chan string, error) {
	return f.called("Tags"), nil
}

func (f *fakeTempoSearch) Values(context.Context, string) (chan string, error) {
	return f.called("Values"), nil
}

func (f *fakeTempoSearch) TagsV2(context.Context, string, time.Time, time.Time, int) (chan string, error) {
	return f.called("TagsV2"), nil
}

func (f *fakeTempoSearch) ValuesV2(context.Context, string, string, time.Time, time.Time, int) (chan string, error) {
	return f.called("ValuesV2"), nil
}

func (f *fakeTempoSearch) SearchTraceQL(context.Context, string, int, int, time.Time, time.Time) (chan []model.TraceInfo, int64, error) {
	f.called("SearchTraceQL")
	if f.err != nil {
		return nil, 0, f.err
	}
	ch := make(chan []model.TraceInfo, 1)
	ch <- f.traces
	close(ch)
	return ch, f.totalJobs, nil
}

func (f *fakeTempoSearch) Search(context.Context, string, int64, int64, int, int64, int64) (chan *model.TraceResponse, error) {
	f.called("Search")
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

// searchRangeEndpoints are the requests that read a time range through TraceQL.
var searchRangeEndpoints = map[string]struct {
	target  string
	serve   func(*TempoController, http.ResponseWriter, *http.Request)
	service string
}{
	"search":            {"/api/search?q=%7B%7D&", (*TempoController).Search, "SearchTraceQL"},
	"tags":              {"/api/v2/search/tags?", (*TempoController).TagsV2, "TagsV2"},
	"tags with q":       {"/api/v2/search/tags?q=%7B%7D&", (*TempoController).TagsV2, "TagsV2"},
	"tag values":        {"/api/v2/search/tag/a/values?", (*TempoController).ValuesV2, "ValuesV2"},
	"tag values with q": {"/api/v2/search/tag/a/values?q=%7B%7D&", (*TempoController).ValuesV2, "ValuesV2"},
}

func serveTempo(service *fakeTempoSearch, serve func(*TempoController, http.ResponseWriter, *http.Request), target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.SetPathValue("tag", "a")
	serve(&TempoController{Service: service}, w, r)
	return w
}

func setSearchMaxDuration(t *testing.T, v string) {
	t.Helper()
	t.Cleanup(func() {
		if err := traceqlconfig.Init(); err != nil {
			t.Error(err)
		}
	})
	t.Setenv(traceqlconfig.SearchMaxDurationEnv, v)
	if err := traceqlconfig.Init(); err != nil {
		t.Fatal(err)
	}
}

func TestSearchRangeAtTheCapPasses(t *testing.T) {
	for name, ep := range searchRangeEndpoints {
		t.Run(name, func(t *testing.T) {
			service := &fakeTempoSearch{}
			w := serveTempo(service, ep.serve, ep.target+"start=1700000000&end=1700604800")
			if w.Code != http.StatusOK || service.calls[ep.service] != 1 {
				t.Errorf("status %d, %s calls %d, body %s", w.Code, ep.service, service.calls[ep.service], w.Body.String())
			}
		})
	}
}

func TestSearchRangeOverTheCapIsRejected(t *testing.T) {
	const want = "range specified by start and end exceeds 168h0m0s. received start=1700000000 end=1700604801"
	for name, ep := range searchRangeEndpoints {
		t.Run(name, func(t *testing.T) {
			service := &fakeTempoSearch{}
			w := serveTempo(service, ep.serve, ep.target+"start=1700000000&end=1700604801")
			if w.Code != http.StatusBadRequest {
				t.Errorf("status %d, want 400", w.Code)
			}
			if got := w.Body.String(); got != want {
				t.Errorf("body %q, want %q", got, want)
			}
			if len(service.calls) != 0 {
				t.Errorf("service called: %v", service.calls)
			}
		})
	}
}

func TestSearchRangeCapOfZeroLets30DaysThrough(t *testing.T) {
	setSearchMaxDuration(t, "0")
	for name, ep := range searchRangeEndpoints {
		t.Run(name, func(t *testing.T) {
			service := &fakeTempoSearch{}
			w := serveTempo(service, ep.serve, ep.target+"start=1700000000&end=1702592000")
			if w.Code != http.StatusOK || service.calls[ep.service] != 1 {
				t.Errorf("status %d, %s calls %d, body %s", w.Code, ep.service, service.calls[ep.service], w.Body.String())
			}
		})
	}
}

func TestSearchRangeCapPrintsTheConfiguredDuration(t *testing.T) {
	setSearchMaxDuration(t, "24h")
	const want = "range specified by start and end exceeds 24h0m0s. received start=1700000000 end=1700086401"
	for name, ep := range searchRangeEndpoints {
		t.Run(name, func(t *testing.T) {
			w := serveTempo(&fakeTempoSearch{}, ep.serve, ep.target+"start=1700000000&end=1700086401")
			if w.Code != http.StatusBadRequest || w.Body.String() != want {
				t.Errorf("status %d, body %q, want 400 and %q", w.Code, w.Body.String(), want)
			}
		})
	}
}

func TestSearchRangeCapRejectsAnEndInMillisecondsOnV2(t *testing.T) {
	const want = "range specified by start and end exceeds 168h0m0s. received start=1700000000 end=1700604801000"
	w := serveTempo(&fakeTempoSearch{}, (*TempoController).TagsV2, "/api/v2/search/tags?start=1700000000&end=1700604801000")
	if w.Code != http.StatusBadRequest || w.Body.String() != want {
		t.Errorf("status %d, body %q, want 400 and %q", w.Code, w.Body.String(), want)
	}
}

func TestSearchRangeCapPrintsBoundsAsSent(t *testing.T) {
	const prefix = "range specified by start and end exceeds 168h0m0s. received "
	for name, query := range map[string]string{
		"milliseconds":                "start=1700000000000&end=1700604801000",
		"half a second over, in ms":   "start=1700000000000&end=1700604800500",
		"microseconds":                "start=1700000000000000&end=1700604801000000",
		"nanoseconds":                 "start=1700000000000000000&end=1700604801000000000",
		"mixed units":                 "start=1700000000&end=1700604801000",
		"start as zero, far-off end":  "start=0&end=4102444800",
		"explicit plus sign on start": "start=%2B1700000000&end=1700604801",
	} {
		t.Run(name, func(t *testing.T) {
			service := &fakeTempoSearch{}
			w := search(service, "/api/search?q=%7B%7D&"+query)
			values, err := url.ParseQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			want := prefix + "start=" + values.Get("start") + " end=" + values.Get("end")
			if w.Code != http.StatusBadRequest || w.Body.String() != want {
				t.Errorf("status %d, body %q, want 400 and %q", w.Code, w.Body.String(), want)
			}
			if len(service.calls) != 0 {
				t.Errorf("service called: %v", service.calls)
			}
		})
	}
}

func TestSearchRangeCapPrintsAnOmittedBoundInSeconds(t *testing.T) {
	const prefix = "range specified by start and end exceeds 168h0m0s. received "
	assertNearNow := func(t *testing.T, v string, offset time.Duration) {
		t.Helper()
		got, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("bound %q: %v", v, err)
		}
		if d := time.Since(time.Unix(got, 0).Add(-offset)); d < -time.Minute || d > time.Minute {
			t.Errorf("bound %q is %s from the effective value", v, d)
		}
	}

	t.Run("end", func(t *testing.T) {
		w := search(&fakeTempoSearch{}, "/api/search?q=%7B%7D&start=1700000000000")
		end, ok := strings.CutPrefix(w.Body.String(), prefix+"start=1700000000000 end=")
		if w.Code != http.StatusBadRequest || !ok {
			t.Fatalf("status %d, body %q", w.Code, w.Body.String())
		}
		assertNearNow(t, end, 0)
	})
	t.Run("start", func(t *testing.T) {
		w := search(&fakeTempoSearch{}, "/api/search?q=%7B%7D&end=4102444800000")
		start, ok := strings.CutPrefix(w.Body.String(), prefix+"start=")
		start, tailOk := strings.CutSuffix(start, " end=4102444800000")
		if w.Code != http.StatusBadRequest || !ok || !tailOk {
			t.Fatalf("status %d, body %q", w.Code, w.Body.String())
		}
		assertNearNow(t, start, -6*time.Hour)
	})
}

func TestSearchRangeCapSkipsRequestsThatDoNotRunTraceQL(t *testing.T) {
	const longRange = "start=1700000000&end=1702592000"
	for name, tc := range map[string]struct {
		target  string
		serve   func(*TempoController, http.ResponseWriter, *http.Request)
		service string
	}{
		"tag search":         {"/api/search?tags=a%3Db&" + longRange, (*TempoController).Search, "Search"},
		"v1 tags":            {"/api/search/tags?" + longRange, (*TempoController).Tags, "Tags"},
		"v1 tag values":      {"/api/search/tag/a/values?" + longRange, (*TempoController).Values, "Values"},
		"v2 tags no start":   {"/api/v2/search/tags?end=1702592000", (*TempoController).TagsV2, "Tags"},
		"v2 values no start": {"/api/v2/search/tag/a/values?end=1702592000", (*TempoController).ValuesV2, "Values"},
	} {
		t.Run(name, func(t *testing.T) {
			service := &fakeTempoSearch{}
			w := serveTempo(service, tc.serve, tc.target)
			if w.Code != http.StatusOK || service.calls[tc.service] != 1 {
				t.Errorf("status %d, %s calls %d, body %s", w.Code, tc.service, service.calls[tc.service], w.Body.String())
			}
		})
	}
}
