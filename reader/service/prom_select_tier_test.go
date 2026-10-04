package service

import (
	"database/sql/driver"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
	"github.com/metrico/qryn/v5/shared/metricretention"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

var lifetimes = metricretention.Tiers{RawDays: 7, FiveMinuteDays: 30, HourDays: 365}

func TestSelectReadsASubstituteFromTheTierAndEndsItOnTheQueryGrid(t *testing.T) {
	// The tier SQL stamps each point at the query's own timestamp, one minute apart.
	db := substituteRows([][]driver.Value{{uint64(1), map[string]string{"job": "a"}}},
		[]driver.Value{uint64(1), int64(300000), 1.0},
		[]driver.Value{uint64(1), int64(360000), 1.0},
		[]driver.Value{uint64(1), int64(420000), 2.0})
	expr := parse(t, "__metric_subst__1")
	pushdown := metricread.Pushdown{Grid: metricread.Grid{StartMs: 60000, EndMs: 600000, StepMs: 60000},
		Func: "rate", RangeMs: 60000, Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")}}
	expr.Substitutes["__metric_subst__1"] = &promql_parser.Substitute{MetricName: "__metric_subst__1", Pushdown: pushdown}
	got := selectRouted(t, db, &TierRouting{Lifetimes: lifetimes, Forced: "5m"}, expr,
		&storage.SelectHints{Start: -239999, End: 600000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))

	want := []point{{300000, math.Float64bits(1)}, {360000, math.Float64bits(1)}, {420000, math.Float64bits(2)},
		{480000, staleMarkerBits}}
	if g := got[`{job="a"}`]; !equalPoints(g, want) {
		t.Errorf("got %v, want %v", g, want)
	}
	pushdown.Tier = metricread.Tier5m
	if q := db.Queries(); len(q) != 2 || q[0] != metricread.PushdownSQL(pushdown) || q[1] != metricread.PushdownLabelsSQL(pushdown) {
		t.Errorf("queries = %q, want the pushdown from the 5m tier and its labels", q)
	}
}

func TestSelectHandsTheEngineATiersLastSamplesAndStaleMarkers(t *testing.T) {
	stale := math.Float64frombits(staleMarkerBits)
	db := fakeclickhouse.New(metricStack(
		[][]driver.Value{{uint64(1), map[string]string{"__name__": "x", "job": "probe"}}},
		[][]driver.Value{
			{uint64(1), ms(300000), 17.0},
			{uint64(1), ms(540000), 12.0},
			{uint64(1), ms(560000), stale},
		}))
	got := selectRouted(t, db, &TierRouting{Lifetimes: lifetimes, Forced: "1h"}, parse(t, "x offset 1m"),
		&storage.SelectHints{Start: 1, End: 600000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "x"))

	want := []point{{300000, math.Float64bits(17)}, {540000, math.Float64bits(12)}, {560000, staleMarkerBits}}
	if g := got[`{__name__="x", job="probe"}`]; !equalPoints(g, want) {
		t.Errorf("got %v, want %v", g, want)
	}
	q := db.Queries()
	if len(q) != 2 || !strings.Contains(q[1], " FROM metrics_1h ") || strings.Contains(q[1], "metric_samples") {
		t.Errorf("queries = %q, want the samples read from the 1h tier", q)
	}
}

func TestSelectReadsTheTierTheQueryIsRoutedTo(t *testing.T) {
	now := time.Now()
	aligned := now.Add(-48 * time.Hour).Truncate(time.Hour).UnixMilli()
	fortyDaysBack := now.Add(-40 * 24 * time.Hour).Truncate(time.Hour).UnixMilli()
	read := func(startMs, stepMs int64) metricread.Read {
		return metricread.Read{Grid: metricread.Grid{StartMs: startMs, EndMs: startMs + 12*stepMs, StepMs: stepMs},
			EarliestMs: startMs - 600000, RangesMs: []int64{300000}}
	}
	for _, tc := range []struct {
		name    string
		routing *TierRouting
		read    metricread.Read
		want    string
	}{
		{"no routing reads raw", nil, read(aligned, 300000), "metric_samples"},
		{"aligned inside raw", &TierRouting{Lifetimes: lifetimes}, read(aligned, 300000), "metrics_5m"},
		{"unaligned inside raw", &TierRouting{Lifetimes: lifetimes}, read(aligned, 60000), "metric_samples"},
		{"past the 5m tier inside 1h", &TierRouting{Lifetimes: lifetimes}, read(fortyDaysBack, 60000), "metrics_1h"},
		{"past the 1h tier", &TierRouting{Lifetimes: lifetimes}, read(60000, 60000), "metrics_1h"},
		{"raw forced past raw", &TierRouting{Lifetimes: lifetimes, Forced: "raw"}, read(60000, 60000), "metric_samples"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := substituteRows(nil)
			expr := parse(t, "__metric_subst__1")
			expr.Substitutes["__metric_subst__1"] = &promql_parser.Substitute{MetricName: "__metric_subst__1",
				Pushdown: metricread.Pushdown{Grid: tc.read.Grid, Func: "rate", RangeMs: 300000,
					Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")}}}
			expr.Read = tc.read
			selectRouted(t, db, tc.routing, expr, &storage.SelectHints{Start: tc.read.Grid.StartMs, End: tc.read.Grid.EndMs},
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))
			if q := db.Queries(); len(q) != 1 || !strings.Contains(q[0], " FROM "+tc.want+" ") {
				t.Errorf("queries = %q, want a read of %s", q, tc.want)
			}
		})
	}
}
