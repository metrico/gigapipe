package service

import (
	"context"
	"database/sql/driver"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const staleMarkerBits = 0x7ff0000000000002

type point struct {
	ts   int64
	bits uint64
}

// metricStack answers the series read with series and the raw read with samples.
func metricStack(series, samples [][]driver.Value) fakeclickhouse.Handler {
	return func(query string) (fakeclickhouse.Result, error) {
		if strings.HasPrefix(query, "WITH fp AS") {
			return fakeclickhouse.Result{Columns: []string{"fingerprint", "timestamp", "value"}, Rows: samples}, nil
		}
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "label_set"}, Rows: series}, nil
	}
}

func parse(t *testing.T, query string) *promql_parser.Expr {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	return expr
}

// selectSeries runs the querier's Select and returns each series' points by label set.
func selectSeries(t *testing.T, db *fakeclickhouse.DB, expr *promql_parser.Expr, hints *storage.SelectHints,
	matchers ...*labels.Matcher) map[string][]point {
	t.Helper()
	queryable := (&CLokiQueriable{ServiceData: model.ServiceData{Session: db}}).SetOidAndDB(context.Background(), expr)
	querier, err := queryable.Querier(hints.Start, hints.End)
	if err != nil {
		t.Fatal(err)
	}
	set := querier.Select(context.Background(), true, hints, matchers...)
	res := map[string][]point{}
	for set.Next() {
		s := set.At()
		var pts []point
		it := s.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			pts = append(pts, point{ts, math.Float64bits(v)})
		}
		res[s.Labels().String()] = pts
	}
	if err := set.Err(); err != nil {
		t.Fatal(err)
	}
	return res
}

func ms(v int64) time.Time { return time.UnixMilli(v).UTC() }

func TestSelectReadsRawSamplesOfTheSelectedSeries(t *testing.T) {
	stale := math.Float64frombits(staleMarkerBits)
	db := fakeclickhouse.New(metricStack(
		[][]driver.Value{
			{uint64(1), map[string]string{"__name__": "x", "job": "probe"}},
			{uint64(2), map[string]string{"__name__": "x", "job": "other"}},
		},
		[][]driver.Value{
			{uint64(1), ms(15000), 1.0},
			{uint64(1), ms(30000), stale},
			{uint64(1), ms(45000), 2.0},
			{uint64(1), ms(45000), 3.0},
			{uint64(2), ms(15000), 7.0},
		}))
	got := selectSeries(t, db, parse(t, "x"), &storage.SelectHints{Start: 1000, End: 60000, Step: 15000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "x"))

	want := map[string][]point{
		`{__name__="x", job="probe"}`: {{15000, math.Float64bits(1)}, {30000, staleMarkerBits}, {45000, math.Float64bits(2)}},
		`{__name__="x", job="other"}`: {{15000, math.Float64bits(7)}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d series %v, want %d", len(got), got, len(want))
	}
	for lbls, pts := range want {
		if g := got[lbls]; !equalPoints(g, pts) {
			t.Errorf("%s: got %v, want %v", lbls, g, pts)
		}
	}
	queries := db.Queries()
	if len(queries) != 2 {
		t.Fatalf("queries = %q", queries)
	}
	for _, want := range []string{"WHERE (name = 'x')", "timestamp > fromUnixTimestamp64Milli(999)",
		"timestamp <= fromUnixTimestamp64Milli(60000)"} {
		if !strings.Contains(queries[1], want) {
			t.Errorf("raw read %q lacks %q", queries[1], want)
		}
	}
}

func equalPoints(a, b []point) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSelectEndsASubstituteSeriesOneStepAfterItsLastPoint(t *testing.T) {
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "labels", "t_ms", "value"},
			Rows: [][]driver.Value{
				{uint64(1), map[string]string{"job": "a"}, int64(60000), 1.0},
				{uint64(1), map[string]string{"job": "a"}, int64(120000), 2.0},
				{uint64(2), map[string]string{"job": "b"}, int64(240000), 3.0},
				{uint64(3), map[string]string{"job": "c"}, int64(60000), 4.0},
				{uint64(3), map[string]string{"job": "c"}, int64(180000), 5.0},
				{uint64(3), map[string]string{"job": "c"}, int64(240000), 6.0},
			}}, nil
	})
	pushdown := metricread.Pushdown{
		Grid:     metricread.Grid{StartMs: 60000, EndMs: 300000, StepMs: 60000},
		Func:     "rate",
		RangeMs:  300000,
		Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")},
	}
	expr := parse(t, "__metric_subst__1")
	expr.Substitutes["__metric_subst__1"] = &promql_parser.Substitute{MetricName: "__metric_subst__1", Pushdown: pushdown}
	// The engine's hints for the substitute selector reach back by its lookback.
	got := selectSeries(t, db, expr, &storage.SelectHints{Start: -239999, End: 300000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))

	want := map[string][]point{
		`{job="a"}`: {{60000, math.Float64bits(1)}, {120000, math.Float64bits(2)}, {180000, staleMarkerBits}},
		`{job="b"}`: {{240000, math.Float64bits(3)}, {300000, staleMarkerBits}},
		`{job="c"}`: {{60000, math.Float64bits(4)}, {120000, staleMarkerBits}, {180000, math.Float64bits(5)},
			{240000, math.Float64bits(6)}, {300000, staleMarkerBits}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d series %v, want %d", len(got), got, len(want))
	}
	for lbls, pts := range want {
		if g := got[lbls]; !equalPoints(g, pts) {
			t.Errorf("%s: got %v, want %v", lbls, g, pts)
		}
	}
	if q := db.Queries(); len(q) != 1 || q[0] != metricread.PushdownSQL(pushdown) {
		t.Errorf("queries = %q, want the substitute's pushdown only", q)
	}
}

func TestSelectLeavesASubstituteSeriesReachingTheQueryEndUnmarked(t *testing.T) {
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "labels", "t_ms", "value"},
			Rows: [][]driver.Value{
				{uint64(1), map[string]string{"__name__": "x"}, int64(240000), 1.0},
				{uint64(1), map[string]string{"__name__": "x"}, int64(300000), 2.0},
			}}, nil
	})
	expr := parse(t, "__metric_subst__1")
	expr.Substitutes["__metric_subst__1"] = &promql_parser.Substitute{MetricName: "__metric_subst__1",
		Pushdown: metricread.Pushdown{Grid: metricread.Grid{StartMs: 60000, EndMs: 330000, StepMs: 60000},
			Func: "last_over_time", RangeMs: 60000,
			Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")}}}
	got := selectSeries(t, db, expr, &storage.SelectHints{Start: -239999, End: 330000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))

	want := []point{{240000, math.Float64bits(1)}, {300000, math.Float64bits(2)}}
	if g := got[`{__name__="x"}`]; !equalPoints(g, want) {
		t.Errorf("got %v, want %v", g, want)
	}
}

func TestSelectStopsWhenTheEngineCancelsTheQuery(t *testing.T) {
	db := fakeclickhouse.New(metricStack(
		[][]driver.Value{{uint64(1), map[string]string{"__name__": "x"}}},
		[][]driver.Value{{uint64(1), ms(15000), 1.0}}))
	queryable := (&CLokiQueriable{ServiceData: model.ServiceData{Session: db}}).
		SetOidAndDB(context.Background(), parse(t, "x"))
	querier, err := queryable.Querier(0, 60000)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	set := querier.Select(ctx, true, &storage.SelectHints{Start: 0, End: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "x"))
	if set.Next() || !errors.Is(set.Err(), context.Canceled) {
		t.Fatalf("Select under a cancelled context: err = %v", set.Err())
	}
	if q := db.Queries(); len(q) != 0 {
		t.Fatalf("queries reached ClickHouse: %q", q)
	}
}

// substituteRows answers every query with pushdown rows (fingerprint, labels, t_ms, value).
func substituteRows(rows ...[]driver.Value) *fakeclickhouse.DB {
	return fakeclickhouse.New(func(string) (fakeclickhouse.Result, error) {
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "labels", "t_ms", "value"}, Rows: rows}, nil
	})
}

func rateSubstitute(t *testing.T) *promql_parser.Expr {
	expr := parse(t, "__metric_subst__1")
	expr.Substitutes["__metric_subst__1"] = &promql_parser.Substitute{MetricName: "__metric_subst__1",
		Pushdown: metricread.Pushdown{Grid: metricread.Grid{StartMs: 60000, EndMs: 300000, StepMs: 60000},
			Func: "rate", RangeMs: 300000,
			Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", "a|b")}}}
	return expr
}

func TestSelectRejectsSubstituteSeriesSharingALabelSetAtOneTimestamp(t *testing.T) {
	db := substituteRows(
		[]driver.Value{uint64(1), map[string]string{"job": "x"}, int64(60000), 1.0},
		[]driver.Value{uint64(1), map[string]string{"job": "x"}, int64(120000), 2.0},
		[]driver.Value{uint64(2), map[string]string{"job": "x"}, int64(120000), 3.0})
	queryable := (&CLokiQueriable{ServiceData: model.ServiceData{Session: db}}).
		SetOidAndDB(context.Background(), rateSubstitute(t))
	querier, err := queryable.Querier(0, 300000)
	if err != nil {
		t.Fatal(err)
	}
	set := querier.Select(context.Background(), true, &storage.SelectHints{Start: -239999, End: 300000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))
	if set.Next() || set.Err() == nil || set.Err().Error() != "vector cannot contain metrics with the same labelset" {
		t.Fatalf("err = %v, want the same-labelset error", set.Err())
	}
}

func TestSelectJoinsSubstituteSeriesSharingALabelSetAtDisjointTimestamps(t *testing.T) {
	db := substituteRows(
		[]driver.Value{uint64(1), map[string]string{"job": "x"}, int64(60000), 1.0},
		[]driver.Value{uint64(1), map[string]string{"job": "x"}, int64(120000), 2.0},
		[]driver.Value{uint64(2), map[string]string{"job": "x"}, int64(180000), 3.0},
		[]driver.Value{uint64(2), map[string]string{"job": "x"}, int64(240000), 4.0})
	got := selectSeries(t, db, rateSubstitute(t), &storage.SelectHints{Start: -239999, End: 300000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))
	want := []point{{60000, math.Float64bits(1)}, {120000, math.Float64bits(2)}, {180000, math.Float64bits(3)},
		{240000, math.Float64bits(4)}, {300000, staleMarkerBits}}
	if len(got) != 1 || !equalPoints(got[`{job="x"}`], want) {
		t.Fatalf("got %v, want {job=\"x\"} %v", got, want)
	}
}
