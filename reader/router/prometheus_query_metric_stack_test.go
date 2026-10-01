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

// serveOneSeries routes the Prometheus query endpoints over a fake ClickHouse holding the
// series x{job="probe"} with value 7 at 00:01 and 8 at 00:02.
func serveOneSeries(t *testing.T) *mux.Router {
	t.Helper()
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		if strings.HasPrefix(query, "WITH fp AS") {
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
	return app
}

func TestInstantQueryWithNegativeOffset(t *testing.T) {
	app := serveOneSeries(t)
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
