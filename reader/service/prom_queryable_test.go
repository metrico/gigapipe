package service

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
)

// stubLabelsGetter is a minimal model.ILabelsGetter backed by a map, for
// testing ReshuffleSeries without needing a real ClickHouse-backed getter.
type stubLabelsGetter struct {
	byFp map[uint64]model.Labels
}

func (s *stubLabelsGetter) Get(fp uint64) model.Labels {
	return s.byFp[fp]
}

func (s *stubLabelsGetter) GetNative(fp uint64) labels.Labels {
	return labels.Labels{}
}

func mkLabels(pairs ...string) model.Labels {
	lbls := make(model.Labels, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		lbls = append(lbls, labels.Label{Name: pairs[i], Value: pairs[i+1]})
	}
	return lbls
}

func TestReshuffleSeries_SameFpSameLabels(t *testing.T) {
	getter := &stubLabelsGetter{byFp: map[uint64]model.Labels{
		1: mkLabels("__name__", "foo", "job", "bar"),
	}}
	s1 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 20, Value: 2},
			{TimestampMs: 40, Value: 4},
		},
	}
	s2 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 10, Value: 1},
			{TimestampMs: 30, Value: 3},
		},
	}
	c := &CLokiQuerier{}
	out := c.ReshuffleSeries([]*model.SeriesV2{s1, s2})

	if len(out) != 1 {
		t.Fatalf("expected 1 series, got %d", len(out))
	}
	samples := out[0].Samples
	if len(samples) != 4 {
		t.Fatalf("expected 4 merged samples, got %d", len(samples))
	}
	for i := 1; i < len(samples); i++ {
		if samples[i-1].TimestampMs > samples[i].TimestampMs {
			t.Fatalf("samples not sorted ascending by TimestampMs: %+v", samples)
		}
	}
	wantTs := []int64{10, 20, 30, 40}
	for i, ts := range wantTs {
		if samples[i].TimestampMs != ts {
			t.Fatalf("sample %d: expected ts %d, got %d", i, ts, samples[i].TimestampMs)
		}
	}
}

func TestReshuffleSeries_DifferentFpSameLabels(t *testing.T) {
	getter := &stubLabelsGetter{byFp: map[uint64]model.Labels{
		1: mkLabels("__name__", "foo", "job", "bar"),
		2: mkLabels("__name__", "foo", "job", "bar"),
	}}
	s1 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 10, Value: 1},
		},
	}
	s2 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           2,
		Samples: []model.Sample{
			{TimestampMs: 20, Value: 2},
		},
	}
	c := &CLokiQuerier{}
	out := c.ReshuffleSeries([]*model.SeriesV2{s1, s2})

	if len(out) != 1 {
		t.Fatalf("expected 1 series, got %d", len(out))
	}
	if len(out[0].Samples) != 2 {
		t.Fatalf("expected 2 merged samples, got %d", len(out[0].Samples))
	}
}

func TestReshuffleSeries_DistinctLabelSets(t *testing.T) {
	getter := &stubLabelsGetter{byFp: map[uint64]model.Labels{
		1: mkLabels("__name__", "foo"),
		2: mkLabels("__name__", "bar"),
	}}
	s1 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 10, Value: 1},
		},
	}
	s2 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           2,
		Samples: []model.Sample{
			{TimestampMs: 20, Value: 2},
		},
	}
	c := &CLokiQuerier{}
	in := []*model.SeriesV2{s1, s2}
	out := c.ReshuffleSeries(in)

	if len(out) != 2 {
		t.Fatalf("expected 2 series, got %d", len(out))
	}
	if out[0] != s1 || out[1] != s2 {
		t.Fatalf("expected input order preserved")
	}
	if len(out[0].Samples) != 1 || len(out[1].Samples) != 1 {
		t.Fatalf("samples should be untouched")
	}
	if out[0].Samples[0].TimestampMs != 10 || out[1].Samples[0].TimestampMs != 20 {
		t.Fatalf("samples should be untouched")
	}
}

// appendStaleMarker terminates a SQL-filled series with a stale marker one step
// past its last sample (issue #931). These tests pin its semantics; the
// sqlFilled gate makes it the single decision point for which series get a
// marker.

func lastIsStaleMarker(t *testing.T, samples []model.Sample, wantTs int64) {
	t.Helper()
	if len(samples) == 0 {
		t.Fatalf("expected samples, got none")
	}
	last := samples[len(samples)-1]
	if last.TimestampMs != wantTs {
		t.Errorf("stale marker timestamp: expected %d, got %d", wantTs, last.TimestampMs)
	}
	if !value.IsStaleNaN(last.Value) {
		t.Errorf("expected last sample to be a stale marker, got value=%v", last.Value)
	}
}

// TestAppendStaleMarker_NoMarkerAtQueryEdge verifies we do NOT emit a stale
// marker for a series that is still live at the query window edge, even when
// SQL-filled. Prometheus only stale-marks a series that actually stops.
func TestAppendStaleMarker_NoMarkerAtQueryEdge(t *testing.T) {
	const step = int64(15000)
	// Last sample is within one step of the query end -> still live.
	const queryEnd = int64(130000 + 5000)
	in := []model.Sample{
		{TimestampMs: 100000, Value: 1},
		{TimestampMs: 130000, Value: 3},
	}

	out := appendStaleMarker(in, true, step, queryEnd)

	if len(out) != len(in) {
		t.Fatalf("expected no marker at the query edge, got %d samples: %+v", len(out), out)
	}
	for _, s := range out {
		if value.IsStaleNaN(s.Value) {
			t.Fatalf("unexpected stale marker for a series live at the query edge: %+v", out)
		}
	}
}

// TestAppendStaleMarker_EmptySeries verifies no marker and no panic for empty
// input.
func TestAppendStaleMarker_EmptySeries(t *testing.T) {
	out := appendStaleMarker(nil, true, 15000, 1_000_000)
	if len(out) != 0 {
		t.Fatalf("expected empty output for empty input, got %+v", out)
	}
}

// TestAppendStaleMarker_ZeroStepNoop verifies that with an unknown/zero step we
// do not fabricate a marker at a bogus timestamp.
func TestAppendStaleMarker_ZeroStepNoop(t *testing.T) {
	in := []model.Sample{{TimestampMs: 100000, Value: 1}}
	out := appendStaleMarker(in, true, 0, 1_000_000)
	if len(out) != len(in) {
		t.Fatalf("expected no marker with zero step, got %+v", out)
	}
}

// isSQLFilled predicate tests: it must key off substitute presence only.

func newQuerierWithSubstitutes(names ...string) *CLokiQuerier {
	subs := make(map[string]*promql_parser.Substitute, len(names))
	for _, n := range names {
		subs[n] = &promql_parser.Substitute{MetricName: n}
	}
	return &CLokiQuerier{expr: &promql_parser.Expr{Substitutes: subs}}
}

func nameMatcher(v string) []*labels.Matcher {
	return []*labels.Matcher{{Type: labels.MatchEqual, Name: "__name__", Value: v}}
}

// transpiledQuerier parses and transpiles a real PromQL query the same way the
// request path does, and returns a CLokiQuerier holding the resulting expr - so
// isSQLFilled runs against the actual optimizer output, not a hand-built map.
func transpiledQuerier(t *testing.T, query string) *promql_parser.Expr {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	expr, err = promql_transpiler.TranspileExpressionV2(expr)
	if err != nil {
		t.Fatalf("transpile %q: %v", query, err)
	}
	return expr
}

// TestIsSQLFilled_FromRealTranspile connects the whole chain end to end: it
// transpiles real queries and drives isSQLFilled with the exact __name__
// matchers the engine passes to Select() for each.
//
//   - rate/sum/*_over_time are accelerated into a substitute; the engine queries
//     the synthetic substitute name, and isSQLFilled must be true (SQL-filled).
//   - abs/topk/... are NOT accelerated; no substitute is created, the engine
//     queries the real metric name, and isSQLFilled must be false so the
//     stale-marker cap is not applied (the #931 review case).
func TestIsSQLFilled_FromRealTranspile(t *testing.T) {
	sqlFilled := []string{
		`rate(test_counter{job="x"}[1m])`,
		`sum(test_counter{job="x"})`,
		`avg_over_time(test_metric{job="x"}[5m])`,
	}
	for _, q := range sqlFilled {
		expr := transpiledQuerier(t, q)
		if len(expr.Substitutes) == 0 {
			t.Fatalf("%q: expected a substitute (accelerated)", q)
		}
		c := &CLokiQuerier{expr: expr}
		// The engine issues one Select per substitute, keyed by the substitute
		// name (substituteSelector sets Name -> __name__).
		for name := range expr.Substitutes {
			if !c.isSQLFilled(nameMatcher(name)) {
				t.Errorf("%q: substitute %q must be reported SQL-filled", q, name)
			}
		}
	}

	// Non-substitute instant-vector functions: no substitute, so the engine
	// queries the real metric name, which isSQLFilled must report as NOT filled.
	notFilled := []struct{ query, metric string }{
		{`abs(test_counter{job="x"})`, "test_counter"},
		{`topk(3, test_counter{job="x"})`, "test_counter"},
		{`ceil(test_metric{job="x"})`, "test_metric"},
		{`histogram_quantile(0.9, test_metric{job="x"})`, "test_metric"},
		// A bare selector is not accelerated either: it is served by the prolong
		// iterator path, not the SQL fill, so it must not be reported SQL-filled.
		{`test_metric{job="x"}`, "test_metric"},
	}
	for _, tc := range notFilled {
		expr := transpiledQuerier(t, tc.query)
		if len(expr.Substitutes) != 0 {
			t.Fatalf("%q: expected NO substitute, got %d", tc.query, len(expr.Substitutes))
		}
		c := &CLokiQuerier{expr: expr}
		if c.isSQLFilled(nameMatcher(tc.metric)) {
			t.Errorf("%q: %q must NOT be reported SQL-filled (would truncate the engine's 5m lookback)",
				tc.query, tc.metric)
		}
	}
}

// applyStaleMarkers is the pure, DB-independent pass Select() runs over the
// built series (like ReshuffleSeries). These tests exercise that exact method
// with the sqlFilled gate computed from query matchers via isSQLFilled, so they
// assert WHICH series get a marker - the scenario the #931 review asked for.

func staleTestSeries() []*model.SeriesV2 {
	getter := &stubLabelsGetter{byFp: map[uint64]model.Labels{
		1: mkLabels("__name__", "s1"),
		2: mkLabels("__name__", "s2"),
	}}
	return []*model.SeriesV2{
		{LabelsGetter: getter, Fp: 1, Samples: []model.Sample{{TimestampMs: 100000, Value: 1}, {TimestampMs: 130000, Value: 3}}},
		{LabelsGetter: getter, Fp: 2, Samples: []model.Sample{{TimestampMs: 100000, Value: 9}, {TimestampMs: 130000, Value: 7}}},
	}
}

func countStaleMarkers(series []*model.SeriesV2) int {
	n := 0
	for _, s := range series {
		for _, smp := range s.Samples {
			if value.IsStaleNaN(smp.Value) {
				n++
			}
		}
	}
	return n
}

// TestApplyStaleMarkers_NonSubstituteInstantFn is the key #931-review case: for
// abs(...)/topk(...) (a non-substitute instant-vector function, proven to
// produce no substitute by promql_transpiler.TestSQLFilled_SubstitutePresence)
// isSQLFilled is false, so applyStaleMarkers - the method Select actually calls
// - must leave every series untouched, preserving the engine's own 5m lookback.
func TestApplyStaleMarkers_NonSubstituteInstantFn(t *testing.T) {
	const step, queryEnd = int64(15000), int64(1_000_000)
	c := newQuerierWithSubstitutes("__metric_subst__999")  // some other query's subst
	sqlFilled := c.isSQLFilled(nameMatcher("test_metric")) // abs(test_metric) -> false
	if sqlFilled {
		t.Fatal("precondition: abs(test_metric) must not be SQL-filled")
	}

	series := staleTestSeries()
	out := c.applyStaleMarkers(series, sqlFilled, step, queryEnd)

	if n := countStaleMarkers(out); n != 0 {
		t.Fatalf("non-substitute instant fn (abs/topk) must not be stale-marked, found %d markers", n)
	}
	for _, s := range out {
		if len(s.Samples) != 2 {
			t.Fatalf("real samples must be untouched, got %d: %+v", len(s.Samples), s.Samples)
		}
	}
}

// TestApplyStaleMarkers_SubstituteBacked is the complement: a substitute-backed
// query (rate/sum/*_over_time) is SQL-filled, so every series gets exactly one
// trailing stale marker at last+step.
func TestApplyStaleMarkers_SubstituteBacked(t *testing.T) {
	const step, queryEnd = int64(15000), int64(1_000_000)
	c := newQuerierWithSubstitutes("__metric_subst__123")
	sqlFilled := c.isSQLFilled(nameMatcher("__metric_subst__123"))
	if !sqlFilled {
		t.Fatal("precondition: substitute-backed query must be SQL-filled")
	}

	out := c.applyStaleMarkers(staleTestSeries(), sqlFilled, step, queryEnd)

	if n := countStaleMarkers(out); n != 2 {
		t.Fatalf("expected one trailing marker per series (2 total), got %d", n)
	}
	for _, s := range out {
		// One marker appended after the two real samples, which are left intact.
		if len(s.Samples) != 3 {
			t.Fatalf("expected 2 real samples + 1 marker, got %d: %+v", len(s.Samples), s.Samples)
		}
		for i := range 2 {
			if value.IsStaleNaN(s.Samples[i].Value) {
				t.Fatalf("real sample %d must be untouched, got a stale marker: %+v", i, s.Samples)
			}
		}
		lastIsStaleMarker(t, s.Samples, 130000+step)
	}
}
