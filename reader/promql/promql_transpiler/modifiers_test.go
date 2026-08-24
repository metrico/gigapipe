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

// recordingQuerier captures the SelectHints the engine asks storage for.
// hints.End is what prom_queryable turns into PlannerContext.To.
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

// hintsForQuery transpiles query, runs the residual through a real prometheus
// engine, and returns the single SelectHints produced. Asserting on hints rather
// than generated SQL tests the contract and survives planner refactors.
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

// TestRangeFnOffsetShiftsWindow: on the range-function path, an offset must
// translate the queried window back by exactly that duration.
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

// TestAggOffsetShiftsWindow: same, for the aggregation path's separate
// substitute site. The last case folds both sites, so the offset survives two hops.
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

// TestAtModifierAnchorsWindow: @ must collapse the window onto the given instant.
// Window only; value-side bucketing is documented on optimizer.substituteSelector.
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

// TestAtStartEndResolve: @ start() / @ end() reach the substitute as StartOrEnd
// rather than Timestamp. The last case combines both modifiers.
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
			// query end, which is what a dropped modifier falls back to anyway.
			// The span does catch it. Bounded by the lookback, not the [5m]:
			// the range is folded into SQL, so hints.Range == 0 and
			// getTimeRangesForSelector takes its lookback branch. Upper bound
			// because that branch shaves 1ms off the lower edge.
			if span := got.End - got.Start; span > modLookback.Milliseconds() {
				t.Errorf("@ must collapse the query onto one instant, so the selected span is one lookback:\n got span = %d ms (Start = %d, End = %d)\nwant span <= %d ms",
					span, got.Start, got.End, modLookback.Milliseconds())
			}
		})
	}
}
