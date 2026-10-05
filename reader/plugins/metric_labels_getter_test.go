package plugins_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// pluginSeries selects series labels from plugin_series, naming the window it was asked for.
type pluginSeries struct{}

func (pluginSeries) GetMetricLabelsQuery(_ context.Context, _ *model.DataDatabasesMap,
	matchers []*labels.Matcher, from time.Time, to time.Time) string {
	return fmt.Sprintf("SELECT fingerprint, label_set FROM plugin_series WHERE %d %d %d",
		len(matchers), from.UnixMilli(), to.UnixMilli())
}

// selectPoints runs a querier's Select over db and returns each series' points as
// "labels: t=v ...".
func selectPoints(t *testing.T, db *fakeclickhouse.DB, expr *promql_parser.Expr, hints *storage.SelectHints,
	matchers ...*labels.Matcher) []string {
	t.Helper()
	queryable := (&service.CLokiQueriable{ServiceData: model.ServiceData{Session: db}}).
		SetOidAndDB(context.Background(), expr)
	querier, err := queryable.Querier(hints.Start, hints.End)
	if err != nil {
		t.Fatal(err)
	}
	set := querier.Select(context.Background(), true, hints, matchers...)
	var res []string
	for set.Next() {
		s := set.At()
		line := s.Labels().String() + ":"
		it := s.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			line += fmt.Sprintf(" %d=%g", ts, v)
		}
		res = append(res, line)
	}
	if err := set.Err(); err != nil {
		t.Fatal(err)
	}
	return res
}

func parse(t *testing.T, query string) *promql_parser.Expr {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	return expr
}

func TestSelectNamesRawSeriesFromTheLabelsGetterPlugin(t *testing.T) {
	plugins.RegisterMetricLabelsGetterPlugin(pluginSeries{})
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		if strings.HasPrefix(query, "SELECT fingerprint, label_set FROM plugin_series WHERE 1 999 60000") {
			return fakeclickhouse.Result{Columns: []string{"fingerprint", "label_set"},
				Rows: [][]driver.Value{{uint64(1), map[string]string{"__name__": "x", "from": "plugin"}}}}, nil
		}
		if strings.Contains(query, "any(labels)") {
			return fakeclickhouse.Result{}, fmt.Errorf("series index read: %s", query)
		}
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "timestamp", "value"},
			Rows: [][]driver.Value{{uint64(1), time.UnixMilli(15000).UTC(), 1.0}}}, nil
	})
	got := selectPoints(t, db, parse(t, "x"), &storage.SelectHints{Start: 1000, End: 60000, Step: 15000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "x"))
	if want := `{__name__="x", from="plugin"}: 15000=1`; len(got) != 1 || got[0] != want {
		t.Fatalf("got %q, want [%q]", got, want)
	}
}

func TestSelectNamesSubstituteSeriesFromTheLabelsGetterPlugin(t *testing.T) {
	plugins.RegisterMetricLabelsGetterPlugin(pluginSeries{})
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		if strings.Contains(query, "ARRAY JOIN") {
			return fakeclickhouse.Result{Columns: []string{"fingerprint", "t_ms", "value"},
				Rows: [][]driver.Value{{uint64(7), int64(60000), 1.0}}}, nil
		}
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "labels"},
			Rows: [][]driver.Value{{uint64(7), map[string]string{"job": "a"}}}}, nil
	})
	expr := parse(t, "__metric_subst__1")
	expr.Substitutes["__metric_subst__1"] = &promql_parser.Substitute{MetricName: "__metric_subst__1",
		Pushdown: metricread.Pushdown{Grid: metricread.Grid{StartMs: 60000, EndMs: 300000, StepMs: 60000},
			Func: "rate", RangeMs: 300000,
			Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", "a|b")}}}
	selectPoints(t, db, expr, &storage.SelectHints{Start: -239999, End: 300000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))
	for _, q := range db.Queries() {
		if strings.Contains(q, "any(labels)") {
			t.Errorf("query reads labels from the series index: %s", q)
		}
		if !strings.Contains(q, "ARRAY JOIN") &&
			!strings.Contains(q, "FROM (SELECT fingerprint, label_set FROM plugin_series WHERE 1 -240000 300000)") {
			t.Errorf("labels read %q does not select from the plugin's query", q)
		}
	}
}
