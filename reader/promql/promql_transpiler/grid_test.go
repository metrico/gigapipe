package promql_transpiler

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler/planner"
	"github.com/prometheus/prometheus/promql/parser"
)

// 1_700_000_000_000 ms is 20s past a minute and 800s past an hour.
const gridStartMs = int64(1_700_000_000_000)

// tagGrids tags query, renders it the way the controllers hand it to the
// engine, re-parses it, and returns the grid each selector carries by name.
func tagGrids(t *testing.T, query string, eval EvalGrid) map[string]planner.Grid {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	TagGrid(expr.Expr, eval)
	rendered := expr.Expr.String()
	reparsed, err := parser.NewParser(parser.Options{}).ParseExpr(rendered)
	if err != nil {
		t.Fatalf("%s: re-parse %q: %v", query, rendered, err)
	}
	grids := map[string]planner.Grid{}
	parser.Inspect(reparsed, func(node parser.Node, _ []parser.Node) error {
		vs, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		if g, _, ok := planner.GridFromMatchers(vs.LabelMatchers); ok {
			grids[vs.Name] = g
		}
		return nil
	})
	return grids
}

func TestTagGrid(t *testing.T) {
	rangeEval := EvalGrid{StartMs: gridStartMs, EndMs: gridStartMs + 86_400_000, StepMs: 60_000}
	hourEval := EvalGrid{StartMs: gridStartMs, EndMs: gridStartMs + 86_400_000, StepMs: 3_600_000,
		SubqueryStepMs: func(int64) int64 { return 120_000 }}
	instantEval := EvalGrid{StartMs: gridStartMs + 7_000, EndMs: gridStartMs + 7_000}
	cases := []struct {
		name  string
		query string
		eval  EvalGrid
		want  map[string]planner.Grid
	}{
		{"bare selector in a range query", "a", rangeEval,
			map[string]planner.Grid{"a": {PhaseMs: 20_000, StepMs: 60_000}}},
		{"offset shifts the phase", "a offset 7m + b offset 50s", hourEval,
			map[string]planner.Grid{
				"a": {PhaseMs: 380_000, StepMs: 3_600_000},
				"b": {PhaseMs: 750_000, StepMs: 3_600_000},
			}},
		{"instant query is a single point on R or LD", "rate(a[1h]) + b", instantEval,
			map[string]planner.Grid{
				"a": {PhaseMs: 807_000, StepMs: 3_600_000},
				"b": {PhaseMs: 207_000, StepMs: 300_000},
			}},
		{"@ pins a range query to a single point", "a @ 1700000123 + rate(b[10m] @ start()) + c @ end() offset 1m", hourEval,
			map[string]planner.Grid{
				"a": {PhaseMs: 23_000, StepMs: 300_000},
				"b": {PhaseMs: 200_000, StepMs: 600_000},
				"c": {PhaseMs: 140_000, StepMs: 300_000},
			}},
		{"subquery inner selectors run on epoch k*SS",
			"max_over_time(rate(a[5m])[1h:1m]) + max_over_time((b offset 20s)[1h:1m])" +
				" + max_over_time(c[1h:5m] offset 7m) + max_over_time(d[1h:] @ 1700000123)" +
				" + max_over_time(max_over_time(e[10m:30s])[1h:5m]) + max_over_time((f @ 1700000123)[1h:1m])",
			hourEval,
			map[string]planner.Grid{
				"a": {PhaseMs: 0, StepMs: 60_000},
				"b": {PhaseMs: 40_000, StepMs: 60_000},
				"c": {PhaseMs: 0, StepMs: 300_000},
				"d": {PhaseMs: 0, StepMs: 120_000},
				"e": {PhaseMs: 0, StepMs: 30_000},
				"f": {PhaseMs: 23_000, StepMs: 300_000},
			}},
		{"subquery in an instant query", "max_over_time(a[1h:1m])", instantEval,
			map[string]planner.Grid{"a": {PhaseMs: 0, StepMs: 60_000}}},
		{"subquery without a step or a default stays untagged", "max_over_time(a[1h:]) + b", rangeEval,
			map[string]planner.Grid{"b": {PhaseMs: 20_000, StepMs: 60_000}}},
		{"ruler instant eval leaves a step-less subquery untagged", "max_over_time(a[1h:]) + max_over_time(b[1h:1m]) + c",
			EvalGrid{StartMs: gridStartMs + 7_000, EndMs: gridStartMs + 7_000},
			map[string]planner.Grid{"b": {PhaseMs: 0, StepMs: 60_000}, "c": {PhaseMs: 207_000, StepMs: 300_000}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := tagGrids(t, c.query, c.eval)
			if len(got) != len(c.want) {
				t.Fatalf("%s: got %v, want %v", c.query, got, c.want)
			}
			for name, want := range c.want {
				if got[name] != want {
					t.Errorf("%s: %s: got %+v, want %+v", c.query, name, got[name], want)
				}
			}
		})
	}
}
