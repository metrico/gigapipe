package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// metricsStart is the start of every seeded span.
var metricsStart = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// testRegistry serves one database.
type testRegistry struct{ db *model.DataDatabasesMap }

func (r testRegistry) GetDB(context.Context) (*model.DataDatabasesMap, error) { return r.db, nil }
func (r testRegistry) Run()                                                   {}
func (r testRegistry) Stop()                                                  {}
func (r testRegistry) Ping() error                                            { return nil }

// openNestedSetTraces gives a service over trace 1, numbered: root span 1
// (1/4/-1, duration 10) and its child span 2 (2/3/1, duration 5); and trace
// 2, not numbered: root span 3.
func openNestedSetTraces(t *testing.T) *TempoService {
	t.Helper()
	conn, db := chtest.Open(t)
	if err := maintenance.Update(conn, db, "", maintenance.CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	ts := metricsStart.UnixNano()
	for _, q := range []string{
		fmt.Sprintf(`INSERT INTO traces_input (trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, tags)
			VALUES ('%032x', '%016x', '', 'root', %d, 10, [('name', 'root')]),
			('%032x', '%016x', '%016x', 'child', %d, 5, [('name', 'child')]),
			('%032x', '%016x', '', 'other', %d, 1, [('name', 'other')])`,
			1, 1, ts, 1, 2, 1, ts+1, 2, 3, ts+2),
		fmt.Sprintf(`INSERT INTO tempo_traces_nested_set (trace_id, span_id, date, left, right, parent, version)
			VALUES (unhex('%032x'), unhex('%016x'), '2026-10-01', 1, 4, -1, 1),
			(unhex('%032x'), unhex('%016x'), '2026-10-01', 2, 3, 1, 1)`, 1, 1, 1, 2),
	} {
		if err := conn.Exec(context.Background(), q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}
	sqlDB := sqlx.NewDb(clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{os.Getenv(chtest.AddrEnv)},
		Auth: clickhouse.Auth{Database: db},
	}), "clickhouse")
	t.Cleanup(func() { _ = sqlDB.Close() })
	session := &dsn.StableSqlxDBWrapper{DB: sqlDB, GetDB: func() *sqlx.DB { return sqlDB }, Name: db}
	return &TempoService{ServiceData: model.ServiceData{Session: testRegistry{&model.DataDatabasesMap{
		Config: &config.ClokiBaseDataBase{}, Session: session,
	}}}}
}

func metricsRequestOf(query string) *model.MetricsQueryRequest {
	return &model.MetricsQueryRequest{
		Query: query,
		From:  metricsStart.Add(-time.Minute),
		To:    metricsStart.Add(time.Minute),
		Step:  2 * time.Minute,
	}
}

// seriesOf lists series as their labels in protobuf JSON and their non-zero
// values, sorted.
func seriesOf(t *testing.T, labels [][]model.MetricsKeyValue, values [][]float64) []string {
	t.Helper()
	var res []string
	for i, l := range labels {
		js, err := json.Marshal(l)
		if err != nil {
			t.Fatal(err)
		}
		var vals []string
		for _, v := range values[i] {
			if v != 0 {
				vals = append(vals, fmt.Sprint(v))
			}
		}
		res = append(res, string(js)+" "+strings.Join(vals, ","))
	}
	sort.Strings(res)
	return res
}

func assertSeries(t *testing.T, query string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s:\ngot  %v\nwant %v", query, got, want)
	}
}

// Metrics group by stored nested-set values, labelled as ints, and take them
// as *_over_time() arguments; an unnumbered span reads 0.
func TestMetricsReadNestedSetValues(t *testing.T) {
	svc := openNestedSetTraces(t)
	ctx := context.Background()
	rangeSeries := func(query string) []string {
		res, err := svc.MetricsQueryRange(ctx, metricsRequestOf(query))
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		var labels [][]model.MetricsKeyValue
		var values [][]float64
		for _, s := range res.Series {
			labels = append(labels, s.Labels)
			var vals []float64
			for _, p := range s.Samples {
				vals = append(vals, p.Value)
			}
			values = append(values, vals)
		}
		return seriesOf(t, labels, values)
	}
	instantSeries := func(query string) []string {
		res, err := svc.MetricsQueryInstant(ctx, metricsRequestOf(query))
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		var labels [][]model.MetricsKeyValue
		var values [][]float64
		for _, s := range res.Series {
			labels = append(labels, s.Labels)
			values = append(values, []float64{s.Value})
		}
		return seriesOf(t, labels, values)
	}

	const byLeft = `{} | count_over_time() by (nestedSetLeft)`
	want := []string{
		`[{"key":"nestedSetLeft","value":{"intValue":"0"}}] 1`,
		`[{"key":"nestedSetLeft","value":{"intValue":"1"}}] 1`,
		`[{"key":"nestedSetLeft","value":{"intValue":"2"}}] 1`,
	}
	assertSeries(t, byLeft, rangeSeries(byLeft), want...)
	assertSeries(t, byLeft, instantSeries(byLeft), want...)

	const avgRight = `{} | avg_over_time(nestedSetRight)`
	assertSeries(t, avgRight, rangeSeries(avgRight), `[{"key":"__name__","value":{"stringValue":"avg_over_time"}}] 2.3333333333333335`)
	const maxRight = `{nestedSetParent < 0} | max_over_time(nestedSetRight) by (nestedSetParent, name)`
	assertSeries(t, maxRight, rangeSeries(maxRight),
		`[{"key":"nestedSetParent","value":{"intValue":"-1"}},{"key":"name","value":{"stringValue":"root"}}] 4`,
		`[{"key":"nestedSetParent","value":{"intValue":"0"}},{"key":"name","value":{"stringValue":"other"}}] `)
}
