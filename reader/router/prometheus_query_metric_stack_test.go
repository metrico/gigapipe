package router

import (
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
)

// serveOneSeries routes the Prometheus query endpoints, reading raw samples, over a fake
// ClickHouse holding the series x{job="probe"} with value 7 at 00:01 and 8 at 00:02, which
// answers every pushdown with pushedDown.
func serveOneSeries(t *testing.T, pushedDown ...[]driver.Value) (*mux.Router, *fakeclickhouse.DB) {
	t.Helper()
	return serveOneSeriesFrom(t, "raw", pushedDown...)
}

// serveOneSeriesFrom is serveOneSeries with METRICS_READ_TIER set to tier.
func serveOneSeriesFrom(t *testing.T, tier string, pushedDown ...[]driver.Value) (*mux.Router, *fakeclickhouse.DB) {
	t.Helper()
	t.Setenv("METRICS_READ_TIER", tier)
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		if strings.Contains(query, "ARRAY JOIN") {
			return fakeclickhouse.Result{Columns: []string{"fingerprint", "labels", "t_ms", "value"}, Rows: pushedDown}, nil
		}
		if !strings.HasPrefix(query, "SELECT fingerprint, any(labels)") {
			return fakeclickhouse.Result{Columns: []string{"fingerprint", "timestamp", "value"}, Rows: [][]driver.Value{
				{uint64(1), time.UnixMilli(60000), 7.0},
				{uint64(1), time.UnixMilli(120000), 8.0},
			}}, nil
		}
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "label_set"}, Rows: [][]driver.Value{
			{uint64(1), map[string]string{"__name__": "x", "job": "probe"}},
		}}, nil
	})
	app := mux.NewRouter()
	RoutePrometheusQueryRange(app, db, false)
	return app, db
}

type promResult struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
			Values [][2]any          `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func get(t *testing.T, app *mux.Router, target string) promResult {
	t.Helper()
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var res promResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	return res
}

// onlyQuery returns the one query db received, failing unless there is exactly one.
func onlyQuery(t *testing.T, db *fakeclickhouse.DB) string {
	t.Helper()
	q := db.Queries()
	if len(q) != 1 {
		t.Fatalf("queries = %q, want one pushdown", q)
	}
	return q[0]
}

func TestInstantQueryWithNegativeOffset(t *testing.T) {
	app, _ := serveOneSeries(t)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET",
		"/api/v1/query?time=60&query="+url.QueryEscape("x offset -1m"), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var res struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Data.Result) != 1 || res.Data.Result[0].Value[1] != "8" || res.Data.Result[0].Metric["job"] != "probe" {
		t.Fatalf("result = %s", rec.Body)
	}
}

func TestRangeQueryEvaluatesAtTheRequestedTimestamps(t *testing.T) {
	app, db := serveOneSeries(t,
		[]driver.Value{uint64(1), map[string]string{"__name__": "x", "job": "probe"}, int64(61000), 7.0},
		[]driver.Value{uint64(1), map[string]string{"__name__": "x", "job": "probe"}, int64(121000), 8.0})
	res := get(t, app, "/api/v1/query_range?start=61&end=121&step=60&query=x")
	if len(res.Data.Result) != 1 {
		t.Fatalf("result = %+v", res)
	}
	want := [][2]any{{61.0, "7"}, {121.0, "8"}}
	got := res.Data.Result[0].Values
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("values = %v, want %v", got, want)
	}
	if q := onlyQuery(t, db); !strings.Contains(q, "WITH 61000 AS start_ms, 121000 AS end_ms, 60000 AS step_ms, 300000 AS range_ms") {
		t.Errorf("pushdown %s is not on the requested grid", q)
	}
}

func TestRangeQueryPushesDownRate(t *testing.T) {
	app, db := serveOneSeries(t,
		[]driver.Value{uint64(1), map[string]string{"job": "probe"}, int64(120000), 0.5},
		[]driver.Value{uint64(1), map[string]string{"job": "probe"}, int64(180000), 0.25})
	res := get(t, app, "/api/v1/query_range?start=60&end=300&step=60&query="+url.QueryEscape("rate(x[5m])"))
	if len(res.Data.Result) != 1 || len(res.Data.Result[0].Metric) != 1 || res.Data.Result[0].Metric["job"] != "probe" {
		t.Fatalf("result = %+v", res)
	}
	want := [][2]any{{120.0, "0.5"}, {180.0, "0.25"}}
	if got := res.Data.Result[0].Values; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("values = %v, want %v (the series ends at 180)", got, want)
	}
	q := onlyQuery(t, db)
	for _, part := range []string{"WITH 60000 AS start_ms, 300000 AS end_ms, 60000 AS step_ms, 300000 AS range_ms",
		"change * (factor / if(per_second, range_ms / 1000, 1))"} {
		if !strings.Contains(q, part) {
			t.Errorf("pushdown %s lacks %s", q, part)
		}
	}
}

func TestInstantQueryPushesDownTheAggregation(t *testing.T) {
	app, db := serveOneSeries(t, []driver.Value{uint64(9), map[string]string{"job": "probe"}, int64(120000), 8.0})
	res := get(t, app, "/api/v1/query?time=120&query="+url.QueryEscape("sum by (job) (x)"))
	if len(res.Data.Result) != 1 || res.Data.Result[0].Value[1] != "8" || res.Data.Result[0].Metric["job"] != "probe" {
		t.Fatalf("result = %+v", res)
	}
	q := onlyQuery(t, db)
	for _, part := range []string{"WITH 120000 AS start_ms, 120000 AS end_ms, 1 AS step_ms, 300000 AS range_ms",
		"has(['job'], x.1)", "if(isFinite(sum(value)), sumKahan(value), sum(value))"} {
		if !strings.Contains(q, part) {
			t.Errorf("pushdown %s lacks %s", q, part)
		}
	}
}

func TestRangeQueryRejectsAStepThePushdownCannotServe(t *testing.T) {
	for _, target := range []string{
		"/api/v1/query_range?start=0&end=600&step=0&query=" + url.QueryEscape("rate(x[5m])"),
		"/api/v1/query_range?start=0&end=600&step=0.001&query=" + url.QueryEscape("rate(x[5m])"),
	} {
		app, db := serveOneSeries(t)
		rec := httptest.NewRecorder()
		app.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400, body = %s", target, rec.Code, rec.Body)
		}
		if q := db.Queries(); len(q) != 0 {
			t.Errorf("%s: queries reached ClickHouse: %q", target, q)
		}
	}
}

func TestReadTierForcesTheTableEveryQueryReads(t *testing.T) {
	app, db := serveOneSeriesFrom(t, "5m")
	get(t, app, "/api/v1/query_range?start=60&end=300&step=60&query="+url.QueryEscape("rate(x[1m])"))
	if q := onlyQuery(t, db); !strings.Contains(q, " FROM metrics_5m ") || strings.Contains(q, "metric_samples") {
		t.Errorf("pushdown %s does not read the 5m tier only", q)
	}
}
