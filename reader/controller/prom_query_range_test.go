package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/prometheus/prometheus/promql"
)

// noSettingsDB fails every query, so no version info resolves.
type noSettingsDB struct{}

func (noSettingsDB) GetName() string { return "no-settings" }
func (noSettingsDB) QueryCtx(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("no settings")
}
func (noSettingsDB) ExecCtx(context.Context, string, ...any) error { return nil }
func (noSettingsDB) Conn(context.Context) (*sql.Conn, error)       { return nil, nil }
func (noSettingsDB) Begin() (*sql.Tx, error)                       { return nil, nil }
func (noSettingsDB) Close()                                        {}

type noSettingsRegistry struct{}

func (noSettingsRegistry) GetDB(context.Context) (*model.DataDatabasesMap, error) {
	return &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{}, Session: noSettingsDB{}}, nil
}
func (noSettingsRegistry) Run()        {}
func (noSettingsRegistry) Stop()       {}
func (noSettingsRegistry) Ping() error { return nil }

// A range query evaluates at the caller's start + k*step, milliseconds
// included, and returns no point past end.
func TestQueryRangeEvaluatesOnTheRequestedGrid(t *testing.T) {
	c := &PromQueryRangeController{
		Engine:  promql.NewEngine(promql.EngineOpts{MaxSamples: 1 << 20, Timeout: time.Minute}),
		Storage: &service.CLokiQueriable{ServiceData: model.ServiceData{Session: noSettingsRegistry{}}},
	}
	for _, q := range []struct{ start, end, step string }{
		{"1700000007", "1700003607", "60"},
		{"1700000007.250", "1700003600.5", "420"},
		{"1700001234.567", "1700087999", "3600"},
		{"1700000000", "1700001007", "15"},
	} {
		rec := httptest.NewRecorder()
		c.QueryRange(rec, httptest.NewRequest("GET",
			"/api/v1/query_range?query=time()&start="+q.start+"&end="+q.end+"&step="+q.step, nil))
		var body struct {
			Status string
			Data   struct{ Result []struct{ Values [][2]any } }
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Status != "success" ||
			len(body.Data.Result) != 1 {
			t.Fatalf("%v: %d %s", q, rec.Code, rec.Body.String())
		}
		start, end, step := ms(t, q.start), ms(t, q.end), ms(t, q.step)
		values := body.Data.Result[0].Values
		if want := (end-start)/step + 1; int64(len(values)) != want {
			t.Errorf("%v: %d points, want %d", q, len(values), want)
		}
		for i, v := range values {
			ts := int64(v[0].(float64)*1000 + 0.5)
			if ts != start+int64(i)*step || ts > end {
				t.Fatalf("%v: point %d at %d, want %d and not past %d", q, i, ts, start+int64(i)*step, end)
			}
		}
	}
}

func ms(t *testing.T, s string) int64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatal(err)
	}
	return int64(f*1000 + 0.5)
}
