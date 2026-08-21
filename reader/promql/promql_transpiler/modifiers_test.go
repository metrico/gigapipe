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
	})
	q, err := engine.NewRangeQuery(context.Background(), &recordingQueryable{hints: &hints}, nil,
		expr.Expr.String(), modQueryStart, modQueryEnd, time.Minute)
	if err != nil {
		t.Fatalf("new range query %s: %v", query, err)
	}
	q.Exec(context.Background())
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
