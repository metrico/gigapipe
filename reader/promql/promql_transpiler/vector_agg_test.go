package promql_transpiler

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
)

// TestCrossSeriesAggregatesAccelerate renders every accelerated cross-series
// aggregation over a bare selector and asserts the combine expression, plus the
// 5m staleness producer they all sit on: each series carried forward with
// last_over_time(5m) so out-of-phase series align at every step.
func TestCrossSeriesAggregatesAccelerate(t *testing.T) {
	for _, c := range []struct {
		fn   string
		want string
	}{
		{"sum", "sum(val) as val"},
		{"min", "min(val) as val"},
		{"max", "max(val) as val"},
		{"avg", "avg(val) as val"},
		{"count", "toFloat64(count(val)) as val"},
		{"group", "toFloat64(1) as val"},
		{"stddev", "stddevPop(val) as val"},
		{"stdvar", "varPop(val) as val"},
	} {
		t.Run(c.fn, func(t *testing.T) {
			got := transpileRange(t, c.fn+`(http_requests_total{job="myjob"})`)
			if !strings.Contains(got, c.want) {
				t.Errorf("missing combine %q in:\n%s", c.want, got)
			}
			// The staleness hold every cross-series aggregate is built on.
			for _, w := range []string{
				"argMaxIf(b_last, b_ts, source = 1)",
				"STALENESS 300000",
				"RANGE BETWEEN 299999 PRECEDING AND CURRENT ROW",
			} {
				if !strings.Contains(got, w) {
					t.Errorf("missing staleness producer piece %q in:\n%s", w, got)
				}
			}
		})
	}
}

// TestAggByKeepsOnlyListedLabels guards `by`: the new label set is filtered to
// the listed labels, which drops __name__ for free.
func TestAggByKeepsOnlyListedLabels(t *testing.T) {
	got := transpileRange(t, `sum by (dc, job) (http_requests_total{job="myjob"})`)
	if !strings.Contains(got, "x.1 IN ('dc','job')") {
		t.Errorf("by must keep only the listed labels:\n%s", got)
	}
}

// TestAggEmptyGroupingKeepsNoLabels guards the empty grouping set: a bare
// aggregation and `by ()` keep no labels, so the projection must be the constant
// empty label set. Rendering the filter instead would emit `x.1 IN ()`, which
// ClickHouse rejects with UNSUPPORTED_METHOD on every version.
func TestAggEmptyGroupingKeepsNoLabels(t *testing.T) {
	for _, q := range []string{
		`sum(http_requests_total{job="myjob"})`,
		`sum by () (http_requests_total{job="myjob"})`,
		`count(http_requests_total{job="myjob"})`,
		`sum(rate(http_requests_total{job="myjob"}[5m]))`,
	} {
		t.Run(q, func(t *testing.T) {
			got := transpileRange(t, q)
			if strings.Contains(got, "IN ()") {
				t.Errorf("empty IN list is not valid ClickHouse:\n%s", got)
			}
			if !strings.Contains(got, "'{}' as new_labels") {
				t.Errorf("an empty grouping set must project the empty label set:\n%s", got)
			}
		})
	}
}

// TestAggWithoutDropsNameLabel guards the __name__ fix: `without` must drop the
// listed labels and __name__, which prometheus removes from every aggregation.
func TestAggWithoutDropsNameLabel(t *testing.T) {
	got := transpileRange(t, `sum without (pod) (http_requests_total{job="myjob"})`)
	if !strings.Contains(got, "x.1 NOT IN ('pod','__name__')") {
		t.Errorf("without must drop the listed labels and __name__:\n%s", got)
	}
}

// TestAggOfRateFolds keeps the folding path working: sum(rate(x[5m])) is one
// substitute carrying both the rate machinery and the cross-series sum.
func TestAggOfRateFolds(t *testing.T) {
	got := transpileRange(t, `sum by (job) (rate(http_requests_total{job="myjob"}[5m]))`)
	if !strings.Contains(got, "sum(val) as val") {
		t.Errorf("outer sum missing:\n%s", got)
	}
	if !strings.Contains(got, "resets") || !strings.Contains(got, "c_change * c_reach / 300.000000 as val") {
		t.Errorf("inner rate machinery missing:\n%s", got)
	}
}

// TestAggOfAggNotAccelerated guards the nesting bail: a cross-series aggregation
// of another one cannot be expressed by AggPlanner, so the outer must be left for
// the engine. Only the inner is substituted.
func TestAggOfAggNotAccelerated(t *testing.T) {
	expr, err := promql_parser.Parse(`sum(sum by (a) (http_requests_total{job="j"}))`)
	if err != nil {
		t.Fatal(err)
	}
	expr, err = TranspileExpressionV2(expr)
	if err != nil {
		t.Fatal(err)
	}
	if len(expr.Substitutes) != 1 {
		t.Fatalf("expected only the inner aggregation substituted, got %d substitutes", len(expr.Substitutes))
	}
	// The outer node must survive as an aggregation for the engine to evaluate.
	if !strings.HasPrefix(expr.Expr.String(), "sum(") {
		t.Errorf("outer aggregation should remain for the engine, got: %s", expr.Expr.String())
	}
}

// TestCrossSeriesAggregateOldClickHouse renders a cross-series aggregate on a
// server without STALENESS: the producer must use the arrayJoin fallback and emit
// no STALENESS clause.
func TestCrossSeriesAggregateOldClickHouse(t *testing.T) {
	got := transpileRangeCtx(t, `sum(http_requests_total{job="myjob"})`, rangeTestCtxCap(false))
	if strings.Contains(got, "STALENESS") {
		t.Errorf("STALENESS must not appear on an incapable server:\n%s", got)
	}
	if !strings.Contains(got, "arrayJoin(range(bucket_ms") {
		t.Errorf("cross-series producer must use the arrayJoin fallback:\n%s", got)
	}
	if !strings.Contains(got, "sum(val) as val") {
		t.Errorf("outer sum missing:\n%s", got)
	}
}
