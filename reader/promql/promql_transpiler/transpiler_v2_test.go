package promql_transpiler

import (
	"fmt"
	"testing"

	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/prometheus/prometheus/model/labels"
)

var grid = metricread.Grid{StartMs: 1767225600000, EndMs: 1767226200000, StepMs: 60000}

func transpile(t *testing.T, query string) *promql_parser.Expr {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	if expr, err = TranspileExpressionV2(expr, grid); err != nil {
		t.Fatal(err)
	}
	return expr
}

func selector(name string, more ...*labels.Matcher) []*labels.Matcher {
	return append(more, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name))
}

func TestTranspilePushesDownRangeFunctionsInstantSelectorsAndTheirAggregation(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  string
		subs  []metricread.Pushdown
	}{
		{`rate(x{job="a"}[5m])`, "__metric_subst__1", []metricread.Pushdown{{Grid: grid, Func: "rate", RangeMs: 300000,
			Matchers: selector("x", labels.MustNewMatcher(labels.MatchEqual, "job", "a"))}}},
		{"x", "__metric_subst__1", []metricread.Pushdown{{Grid: grid, RangeMs: 300000, Matchers: selector("x")}}},
		{"sum by (job) (increase(x[10m]))", "__metric_subst__1", []metricread.Pushdown{{Grid: grid, Func: "increase",
			RangeMs: 600000, Matchers: selector("x"),
			Aggregation: &metricread.Aggregation{Op: "sum", Grouping: []string{"job"}}}}},
		{"max without (instance) ((x))", "__metric_subst__1", []metricread.Pushdown{{Grid: grid, RangeMs: 300000,
			Matchers:    selector("x"),
			Aggregation: &metricread.Aggregation{Op: "max", Grouping: []string{"instance"}, Without: true}}}},
		{"sum(rate(x[5m])) / count(last_over_time(y[1m]))", "__metric_subst__1 / __metric_subst__2",
			[]metricread.Pushdown{
				{Grid: grid, Func: "rate", RangeMs: 300000, Matchers: selector("x"),
					Aggregation: &metricread.Aggregation{Op: "sum"}},
				{Grid: grid, Func: "last_over_time", RangeMs: 60000, Matchers: selector("y"),
					Aggregation: &metricread.Aggregation{Op: "count"}},
			}},
		{"topk(3, rate(x[5m]))", "topk(3, __metric_subst__1)", []metricread.Pushdown{{Grid: grid, Func: "rate",
			RangeMs: 300000, Matchers: selector("x")}}},
		{"histogram_quantile(0.9, sum by (le) (rate(x[5m])))", "histogram_quantile(0.9, __metric_subst__1)",
			[]metricread.Pushdown{{Grid: grid, Func: "rate", RangeMs: 300000, Matchers: selector("x"),
				Aggregation: &metricread.Aggregation{Op: "sum", Grouping: []string{"le"}}}}},
		{`sum(rate({__name__=~"a|b"}[5m]))`, "sum(__metric_subst__1)", []metricread.Pushdown{{Grid: grid,
			Func: "rate", RangeMs: 300000, Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "a|b")}}}},
		{`sum by (job) (increase({job="x"}[5m]))`, "sum by (job) (__metric_subst__1)", []metricread.Pushdown{{Grid: grid,
			Func: "increase", RangeMs: 300000, Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "x")}}}},
		{`max(last_over_time({__name__=~"a|b"}[5m]))`, "__metric_subst__1", []metricread.Pushdown{{Grid: grid,
			Func: "last_over_time", RangeMs: 300000, Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "a|b")},
			Aggregation: &metricread.Aggregation{Op: "max"}}}},
		{`count({__name__=~"a|b"})`, "__metric_subst__1", []metricread.Pushdown{{Grid: grid, RangeMs: 300000,
			Matchers:    []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "a|b")},
			Aggregation: &metricread.Aggregation{Op: "count"}}}},
		{"sum(sum by (job) (x))", "sum(__metric_subst__1)", []metricread.Pushdown{{Grid: grid, RangeMs: 300000,
			Matchers: selector("x"), Aggregation: &metricread.Aggregation{Op: "sum", Grouping: []string{"job"}}}}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			expr := transpile(t, tc.query)
			if got := expr.Expr.String(); got != tc.want {
				t.Errorf("expression = %s, want %s", got, tc.want)
			}
			if len(expr.Substitutes) != len(tc.subs) {
				t.Fatalf("%d substitutes, want %d", len(expr.Substitutes), len(tc.subs))
			}
			for i, want := range tc.subs {
				name := "__metric_subst__" + string(rune('1'+i))
				sub, ok := expr.Substitutes[name]
				if !ok || sub.MetricName != name {
					t.Fatalf("no substitute %s in %v", name, expr.Substitutes)
				}
				if got, want := describe(sub.Pushdown), describe(want); got != want {
					t.Errorf("%s = %s, want %s", name, got, want)
				}
			}
		})
	}
}

// describe renders a pushdown with its matchers as PromQL text.
func describe(p metricread.Pushdown) string {
	agg := "none"
	if p.Aggregation != nil {
		agg = fmt.Sprintf("%+v", *p.Aggregation)
	}
	return fmt.Sprintf("%+v %q %d %v %s", p.Grid, p.Func, p.RangeMs, p.Matchers, agg)
}

func TestTranspileLeavesTheEngineWhatItCannotPushDown(t *testing.T) {
	for _, query := range []string{
		"rate(x[5m] offset 1m)",
		"x offset 1m",
		"x @ 100",
		"rate(x[5m] @ end())",
		"deriv(x[5m])",
		"quantile_over_time(0.5, x[5m])",
		"max_over_time(rate(x[5m])[30m:1m])",
		"rate(x[5m:1m])",
		"timestamp(x)",
		"absent(x)",
		"sum(rate(x[5m] offset 1m))",
	} {
		t.Run(query, func(t *testing.T) {
			expr := transpile(t, query)
			if len(expr.Substitutes) != 0 {
				t.Errorf("%s pushed down as %s", query, expr.Expr)
			}
		})
	}
}
