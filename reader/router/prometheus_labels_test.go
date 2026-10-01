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
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
)

// serveLabels routes the Prometheus label endpoints over a fake ClickHouse answering every
// query with res.
func serveLabels(t *testing.T, res fakeclickhouse.Result) (*mux.Router, *fakeclickhouse.DB) {
	t.Helper()
	db := fakeclickhouse.New(func(string) (fakeclickhouse.Result, error) { return res, nil })
	app := mux.NewRouter()
	RouteSelectPrometheusLabels(app, db)
	return app, db
}

func strings1(col string, vals ...string) fakeclickhouse.Result {
	res := fakeclickhouse.Result{Columns: []string{col}}
	for _, v := range vals {
		res.Rows = append(res.Rows, []driver.Value{v})
	}
	return res
}

type labelsResponse struct {
	Status   string          `json:"status"`
	Data     json.RawMessage `json:"data"`
	Warnings []string        `json:"warnings"`
}

func getLabels(t *testing.T, app *mux.Router, target string, wantCode int) labelsResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))
	if rec.Code != wantCode {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, wantCode, rec.Body)
	}
	var res labelsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("%v: %s", err, rec.Body)
	}
	return res
}

func TestLabelNamesWarnWhenTheLimitTruncates(t *testing.T) {
	app, db := serveLabels(t, strings1("label", "__name__", "job"))
	res := getLabels(t, app, "/api/v1/labels?limit=1&match[]=up", http.StatusOK)
	if string(res.Data) != `["__name__"]` {
		t.Fatalf("data = %s", res.Data)
	}
	if len(res.Warnings) != 1 || res.Warnings[0] != "results truncated due to limit" {
		t.Fatalf("warnings = %q", res.Warnings)
	}
	if q := onlyQuery(t, db); !strings.HasSuffix(q, "WHERE (name = 'up') ORDER BY label LIMIT 2") {
		t.Fatalf("query = %s", q)
	}
}

func TestLabelNamesWithoutParametersReadTheWholeIndex(t *testing.T) {
	app, db := serveLabels(t, strings1("label", "__name__", "job"))
	res := getLabels(t, app, "/api/v1/labels", http.StatusOK)
	if string(res.Data) != `["__name__","job"]` || res.Warnings != nil {
		t.Fatalf("data = %s, warnings = %q", res.Data, res.Warnings)
	}
	if q := onlyQuery(t, db); strings.Contains(q, "WHERE") || strings.Contains(q, "LIMIT") {
		t.Fatalf("query = %s", q)
	}
}

func TestLabelBoundsParseAsRFC3339OrFractionalSeconds(t *testing.T) {
	app, db := serveLabels(t, strings1("label"))
	res := getLabels(t, app, "/api/v1/labels?start=2026-10-01T00:00:00Z&end=1790856000.5", http.StatusOK)
	if string(res.Data) != `[]` {
		t.Fatalf("data = %s", res.Data)
	}
	want := "WHERE last_seen >= fromUnixTimestamp64Milli(1790811000000) " +
		"AND first_seen <= fromUnixTimestamp64Milli(1790856000500) ORDER BY label"
	if q := onlyQuery(t, db); !strings.HasSuffix(q, want) {
		t.Fatalf("query = %s", q)
	}
}

func TestLabelEndpointsRejectBadParameters(t *testing.T) {
	app, db := serveLabels(t, strings1("label"))
	for _, target := range []string{
		"/api/v1/labels?match[]=up{",
		"/api/v1/labels?match[]={job=~\".*\"}",
		"/api/v1/labels?limit=-1",
		"/api/v1/labels?limit=x",
		"/api/v1/labels?start=yesterday",
	} {
		if res := getLabels(t, app, target, http.StatusBadRequest); res.Status != "error" {
			t.Errorf("%s: status = %s", target, res.Status)
		}
	}
	if q := db.Queries(); len(q) != 0 {
		t.Fatalf("queries = %q", q)
	}
}

func TestLabelValuesOfANameReadTheNameColumn(t *testing.T) {
	app, db := serveLabels(t, strings1("value", "http_requests_total", "up"))
	res := getLabels(t, app, "/api/v1/label/__name__/values?limit=2", http.StatusOK)
	if string(res.Data) != `["http_requests_total","up"]` || res.Warnings != nil {
		t.Fatalf("data = %s, warnings = %q", res.Data, res.Warnings)
	}
	if q := onlyQuery(t, db); q != "SELECT DISTINCT name AS value FROM metric_series ORDER BY value LIMIT 3" {
		t.Fatalf("query = %s", q)
	}
}

func TestLabelValuesExcludeSeriesWithoutTheLabel(t *testing.T) {
	app, db := serveLabels(t, strings1("value", "/v1/x"))
	res := getLabels(t, app, "/api/v1/label/path/values?match[]=http_requests_total", http.StatusOK)
	if string(res.Data) != `["/v1/x"]` {
		t.Fatalf("data = %s", res.Data)
	}
	want := "SELECT DISTINCT labels['path'] AS value FROM metric_series " +
		"WHERE (name = 'http_requests_total') AND labels['path'] != '' ORDER BY value"
	if q := onlyQuery(t, db); q != want {
		t.Fatalf("query = %s", q)
	}
}

func TestSeriesReturnFullLabelSetsAndWarnAtTheLimit(t *testing.T) {
	app, db := serveLabels(t, fakeclickhouse.Result{Columns: []string{"label_set"}, Rows: [][]driver.Value{
		{map[string]string{"__name__": "http_requests_total", "job": "api", "status": "200"}},
		{map[string]string{"__name__": "http_requests_total", "job": "api", "status": "500"}},
	}})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/series", strings.NewReader("match[]=http_requests_total&limit=1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	app.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	want := `{"status":"success","data":[{"__name__":"http_requests_total","job":"api","status":"200"}],` +
		`"warnings":["results truncated due to limit"]}`
	if rec.Body.String() != want {
		t.Fatalf("body = %s", rec.Body)
	}
	wantQ := "SELECT any(labels) AS label_set FROM metric_series WHERE (name = 'http_requests_total') " +
		"GROUP BY name, fingerprint ORDER BY name, fingerprint LIMIT 2"
	if q := onlyQuery(t, db); q != wantQ {
		t.Fatalf("query = %s", q)
	}
}

func TestSeriesNeedASelector(t *testing.T) {
	app, db := serveLabels(t, fakeclickhouse.Result{Columns: []string{"label_set"}})
	getLabels(t, app, "/api/v1/series", http.StatusBadRequest)
	if q := db.Queries(); len(q) != 0 {
		t.Fatalf("queries = %q", q)
	}
}

func metadataRows(rows ...[]driver.Value) fakeclickhouse.Result {
	return fakeclickhouse.Result{Columns: []string{"name", "type", "help", "unit"}, Rows: rows}
}

func TestMetadataIsOneEntryPerFamily(t *testing.T) {
	app, db := serveLabels(t, metadataRows(
		[]driver.Value{"http_requests_total", "counter", "Requests served", ""},
		[]driver.Value{"up", "gauge", "Target up", ""},
	))
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/metadata?limit=2&limit_per_metric=1", nil))
	want := `{"status":"success","data":{` +
		`"http_requests_total":[{"type":"counter","help":"Requests served","unit":""}],` +
		`"up":[{"type":"gauge","help":"Target up","unit":""}]}}`
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if q := onlyQuery(t, db); q != "SELECT name, type, help, unit FROM metric_metadata FINAL ORDER BY name LIMIT 2" {
		t.Fatalf("query = %s", q)
	}
}

func TestMetadataOfOneMetric(t *testing.T) {
	app, db := serveLabels(t, metadataRows())
	res := getLabels(t, app, "/api/v1/metadata?metric=up", http.StatusOK)
	if string(res.Data) != `{}` {
		t.Fatalf("data = %s", res.Data)
	}
	if q := onlyQuery(t, db); q != "SELECT name, type, help, unit FROM metric_metadata FINAL WHERE name = 'up' ORDER BY name" {
		t.Fatalf("query = %s", q)
	}
}

func TestMetadataRejectsANonNumericLimit(t *testing.T) {
	app, _ := serveLabels(t, metadataRows())
	getLabels(t, app, "/api/v1/metadata?limit=x", http.StatusBadRequest)
	getLabels(t, app, "/api/v1/metadata?limit_per_metric=x", http.StatusBadRequest)
}

func TestQueryExemplarsGroupsExemplarsPerSeries(t *testing.T) {
	api := map[string]string{"__name__": "http_requests_total", "job": "api"}
	app, db := serveLabels(t, fakeclickhouse.Result{
		Columns: []string{"fingerprint", "label_set", "timestamp", "value", "labels"},
		Rows: [][]driver.Value{
			{uint64(1), api, time.UnixMilli(1790812815500), 2.0, `{"trace_id":"abc"}`},
			{uint64(1), api, time.UnixMilli(1790812830000), 0.25, `{"trace_id":"def","span_id":"01"}`},
			{uint64(2), map[string]string{"__name__": "up", "job": "api"}, time.UnixMilli(1790812800007), 1.0, `{}`},
		},
	})
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/query_exemplars?start=1790812800&end=2026-10-01T12:00:00Z&query="+
		url.QueryEscape(`rate(http_requests_total{job="api"}[5m]) or up`), nil))
	want := `{"status":"success","data":[` +
		`{"seriesLabels":{"__name__":"http_requests_total","job":"api"},"exemplars":[` +
		`{"labels":{"trace_id":"abc"},"value":"2","timestamp":1790812815.500},` +
		`{"labels":{"span_id":"01","trace_id":"def"},"value":"0.25","timestamp":1790812830}]},` +
		`{"seriesLabels":{"__name__":"up","job":"api"},"exemplars":[` +
		`{"labels":{},"value":"1","timestamp":1790812800.007}]}]}`
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("status = %d\nbody = %s\nwant = %s", rec.Code, rec.Body, want)
	}
	q := onlyQuery(t, db)
	for _, part := range []string{
		"AND ((labels['job'] = 'api' AND name = 'http_requests_total') OR (name = 'up')) GROUP BY fingerprint",
		"AND e.timestamp >= fromUnixTimestamp64Milli(1790812800000) AND e.timestamp <= fromUnixTimestamp64Milli(1790856000000)",
	} {
		if !strings.Contains(q, part) {
			t.Fatalf("query = %s\nwant part %s", q, part)
		}
	}
}

func TestQueryExemplarsWithoutASelectorIsEmpty(t *testing.T) {
	app, db := serveLabels(t, fakeclickhouse.Result{})
	if res := getLabels(t, app, "/api/v1/query_exemplars?query=1", http.StatusOK); string(res.Data) != `[]` {
		t.Fatalf("data = %s", res.Data)
	}
	if q := db.Queries(); len(q) != 0 {
		t.Fatalf("queries = %q", q)
	}
}

func TestQueryExemplarsRejectBadParameters(t *testing.T) {
	app, _ := serveLabels(t, fakeclickhouse.Result{})
	getLabels(t, app, "/api/v1/query_exemplars?query=up{", http.StatusBadRequest)
	getLabels(t, app, "/api/v1/query_exemplars?query=up&start=20&end=10", http.StatusBadRequest)
}
