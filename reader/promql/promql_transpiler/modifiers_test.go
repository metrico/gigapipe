package promql_transpiler

import (
	"context"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// modQueryStart/modQueryEnd describe a one hour range query at a 60s step. Every
// assertion in this file is written against these two instants.
var (
	modQueryEnd   = time.Unix(1700000000, 0)
	modQueryStart = modQueryEnd.Add(-time.Hour)
)

// modLookback is the engine lookback delta hintsForQuery runs with. Pinned
// rather than defaulted because the span assertions are written against it.
const modLookback = 5 * time.Minute

// recordingQuerier captures the SelectHints the prometheus engine asks storage
// for, and returns nothing. hints.End is what
// reader/service/prom_queryable.go turns into PlannerContext.To, so it is
// exactly the window the accelerated SQL will read.
type recordingQuerier struct{ hints *[]*storage.SelectHints }

func (r *recordingQuerier) Select(_ context.Context, _ bool, h *storage.SelectHints,
	_ ...*labels.Matcher) storage.SeriesSet {
	*r.hints = append(*r.hints, h)
	return storage.EmptySeriesSet()
}

func (r *recordingQuerier) LabelValues(context.Context, string, *storage.LabelHints,
	...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (r *recordingQuerier) LabelNames(context.Context, *storage.LabelHints,
	...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (r *recordingQuerier) Close() error { return nil }

type recordingQueryable struct{ hints *[]*storage.SelectHints }

func (r *recordingQueryable) Querier(int64, int64) (storage.Querier, error) {
	return &recordingQuerier{hints: r.hints}, nil
}

// hintsForQuery transpiles query and runs the residual expression through a real
// prometheus engine, returning the single SelectHints the engine produced.
//
// Asserting on hints rather than on generated SQL tests the actual contract --
// does the engine ask for the right window? -- and stays valid across planner
// refactors.
func hintsForQuery(t *testing.T, query string) *storage.SelectHints {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	expr, err = TranspileExpressionV2(expr)
	if err != nil {
		t.Fatalf("transpile %s: %v", query, err)
	}
	if len(expr.Substitutes) == 0 {
		t.Fatalf("%s: not accelerated, there is no pushdown window to assert on", query)
	}
	var hints []*storage.SelectHints
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:       1e6,
		Timeout:          time.Minute,
		EnableAtModifier: true,
		LookbackDelta:    modLookback,
	})
	q, err := engine.NewRangeQuery(context.Background(), &recordingQueryable{hints: &hints}, nil,
		expr.Expr.String(), modQueryStart, modQueryEnd, time.Minute)
	if err != nil {
		t.Fatalf("new range query %s: %v", query, err)
	}
	defer q.Close()
	res := q.Exec(context.Background())
	if res.Err != nil {
		t.Fatalf("exec %s: %v", query, res.Err)
	}
	if len(hints) != 1 {
		t.Fatalf("%s: expected exactly one Select call, got %d", query, len(hints))
	}
	return hints[0]
}

// TestRangeFnOffsetShiftsWindow guards the range-function path: an offset must
// translate the queried window back by exactly that duration. Before the fix the
// substitute selector was built bare, so the offset never reached the engine and
// rate(x[5m] offset 1h) silently returned the current rate.
func TestRangeFnOffsetShiftsWindow(t *testing.T) {
	for _, c := range []struct {
		query string
		shift time.Duration
	}{
		{`rate(http_requests_total{job="j"}[5m] offset 1h)`, time.Hour},
		{`last_over_time(http_requests_total{job="j"}[5m] offset 30m)`, 30 * time.Minute},
		{`increase(http_requests_total{job="j"}[5m] offset 2h)`, 2 * time.Hour},
	} {
		t.Run(c.query, func(t *testing.T) {
			got := hintsForQuery(t, c.query)
			want := modQueryEnd.UnixMilli() - c.shift.Milliseconds()
			if got.End != want {
				t.Errorf("offset must shift the queried window back:\n got End = %d\nwant End = %d (query end %d minus %v)",
					got.End, want, modQueryEnd.UnixMilli(), c.shift)
			}
		})
	}
}

// TestAggOffsetShiftsWindow guards the cross-series aggregation path, which
// builds its substitute in a second place (Aggregate.aggregate). The last case
// also covers folding: the inner range function is substituted first, and the
// outer aggregation clones that already-synthetic selector, so the offset has to
// survive two hops.
func TestAggOffsetShiftsWindow(t *testing.T) {
	for _, c := range []struct {
		query string
		shift time.Duration
	}{
		{`sum(http_requests_total{job="j"} offset 1h)`, time.Hour},
		{`count(http_requests_total{job="j"} offset 15m)`, 15 * time.Minute},
		{`sum by (job) (rate(http_requests_total{job="j"}[5m] offset 2h))`, 2 * time.Hour},
	} {
		t.Run(c.query, func(t *testing.T) {
			got := hintsForQuery(t, c.query)
			want := modQueryEnd.UnixMilli() - c.shift.Milliseconds()
			if got.End != want {
				t.Errorf("offset must shift the queried window back:\n got End = %d\nwant End = %d (query end %d minus %v)",
					got.End, want, modQueryEnd.UnixMilli(), c.shift)
			}
		})
	}
}

// TestAtModifierAnchorsWindow guards @: the window must collapse onto the given
// instant. @ was explicitly enabled for users in issue #769
// (EnableAtModifier: true in reader/router/prometheus_query_range.go); the v5.0.0
// pushdown silently undid that for every accelerated expression.
//
// Only the window is asserted. The accelerated path buckets samples on the step
// grid, so an @ instant that is not step aligned resolves to the enclosing
// bucket -- the value is accurate to within one step, exact only when the
// instant is step aligned. That deviation is deliberate and documented on
// substituteSelector.
func TestAtModifierAnchorsWindow(t *testing.T) {
	const atSeconds = 1699000000
	for _, q := range []string{
		`rate(http_requests_total{job="j"}[5m] @ 1699000000)`,
		`sum(http_requests_total{job="j"} @ 1699000000)`,
	} {
		t.Run(q, func(t *testing.T) {
			got := hintsForQuery(t, q)
			want := int64(atSeconds) * 1000
			if got.End != want {
				t.Errorf("@ must anchor the queried window to the given instant:\n got End = %d\nwant End = %d",
					got.End, want)
			}
		})
	}
}

// TestAtStartEndResolve guards the @ start() / @ end() forms, which reach the
// substitute as StartOrEnd rather than Timestamp and are resolved later by the
// engine's preprocessing. The last case combines both modifiers.
//
// Each subtest additionally asserts the selected span; that check is written
// once, inside the loop body, and runs for every case.
func TestAtStartEndResolve(t *testing.T) {
	for _, c := range []struct {
		query   string
		wantEnd int64
	}{
		{`rate(http_requests_total{job="j"}[5m] @ start())`, modQueryStart.UnixMilli()},
		{`rate(http_requests_total{job="j"}[5m] @ end())`, modQueryEnd.UnixMilli()},
		{`rate(http_requests_total{job="j"}[5m] @ end() offset 30m)`,
			modQueryEnd.UnixMilli() - (30 * time.Minute).Milliseconds()},
	} {
		t.Run(c.query, func(t *testing.T) {
			got := hintsForQuery(t, c.query)
			if got.End != c.wantEnd {
				t.Errorf("@ start()/end() must resolve against the query window:\n got End = %d\nwant End = %d",
					got.End, c.wantEnd)
			}
			// End alone cannot catch a dropped @ end(): end() resolves to the
			// query end, which is exactly what a dropped modifier falls back to.
			// The span does catch it.
			//
			// The span is bounded by the lookback, not by the [5m] in the query
			// text. The range function is folded into SQL, so what reaches the
			// engine is a bare instant selector with hints.Range == 0, and
			// getTimeRangesForSelector then takes its lookback branch rather
			// than its range branch (engine.go:997-1004). @ collapses the query
			// onto a single evaluation instant, so the engine reads one lookback
			// window instead of the whole query window plus a lookback.
			//
			// Asserted as an upper bound, not an exact width: that branch shaves
			// 1ms off the lower bound to exclude samples landing precisely a
			// lookback before the eval time, and that 1ms is an engine detail
			// this test has no reason to pin.
			if span := got.End - got.Start; span > modLookback.Milliseconds() {
				t.Errorf("@ must collapse the query onto one instant, so the selected span is one lookback:\n got span = %d ms (Start = %d, End = %d)\nwant span <= %d ms",
					span, got.Start, got.End, modLookback.Milliseconds())
			}
		})
	}
}
