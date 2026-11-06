package clickhouse_transpiler

import (
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

func TestPortionFilterOnEveryOperandRead(t *testing.T) {
	const portionFilter = `((cityHash64(trace_id) % 3) == (1)) or (trace_id IN (unhex('aa'),unhex('bb')))`
	requestedBounds := []string{">86390", "<=86410"}
	for _, tc := range []struct {
		query    string
		operands int
	}{
		{`{.a} >> {.b}`, 2},
		{`{.a} && {.b}`, 2},
		{`{.a} !>> {.b}`, 2},
		{`{.a} | count() > 2`, 1},
		{`{.a = 1} >> {.b = 2}`, 2},
		{`{.a = 1} && {.b = 2}`, 2},
		{`{.a = 1} !>> {.b = 2}`, 2},
		{`{.a = 1} | count() > 2`, 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.RandomFilter = shared.RandomFilter{Max: 3, I: 1}
			ctx.CachedTraceIds = []string{"aa", "bb"}
			req, err := planSearch(t, tc.query).Process(ctx)
			if err != nil {
				t.Fatal(err)
			}
			operands := 0
			for _, w := range req.GetWith() {
				s := renderWith(t, w)
				bounds := spanStartBounds(s)
				if len(bounds) == 0 {
					continue
				}
				operands++
				if !slices.Equal(bounds, requestedBounds) {
					t.Errorf("%s: bounds %v, want %v", w.GetAlias(), bounds, requestedBounds)
				}
				if !strings.Contains(s, portionFilter) {
					t.Errorf("%s: no portion filter:\n%s", w.GetAlias(), s)
				}
			}
			if operands != tc.operands {
				t.Errorf("got %d operand reads, want %d", operands, tc.operands)
			}
		})
	}
}

// A query the attrless plan reads estimates like that plan, select() stages aside.
func TestEstimateFollowsAttrlessPlan(t *testing.T) {
	for _, tc := range []struct {
		query    string
		attrless bool
	}{
		{`{}`, true},
		{`({})`, true},
		{`{} | select(.x)`, true},
		{`({}) | select(.x)`, true},
		{`{} | select(.x) | select(.y)`, true},
		{`{.a} | select(.x)`, false},
		{`{} | count() > 2`, false},
		{`{} | {.a}`, false},
		{`{} >> {.a}`, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			q, err := traceql_ir.Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			eval, err := PlanEval(q)
			if err != nil {
				t.Fatal(err)
			}
			_, attrless := eval.(*EvalFinalizerPlanner).Main.(*AttrlessEvaluatorPlanner)
			if attrless != tc.attrless {
				t.Errorf("estimated by %T, want attrless %v", eval.(*EvalFinalizerPlanner).Main, tc.attrless)
			}
		})
	}
}

func estimateOf(t *testing.T, query string) shared.SQLRequestPlanner {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	eval, err := PlanEval(q)
	if err != nil {
		t.Fatal(err)
	}
	return eval.(*EvalFinalizerPlanner).Main
}

func TestStructuralQueriesEstimateEverySpanInRange(t *testing.T) {
	queries := []string{`{.a} && ({.b} >> {.c})`, `({.a} >> {.b}) | count() > 1`, `{} ~ {.a}`, `{} >> {.a}`}
	for _, op := range []string{">", "<", "~", "!>", "!<", "!~", "&>", "&<", "&~", ">>", "<<", "!>>", "!<<", "&>>", "&<<"} {
		queries = append(queries, `{.a = 1} `+op+` {.b = 1}`)
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			estimate := estimateOf(t, query)
			if _, ok := estimate.(*AllSpansEvaluatorPlanner); !ok {
				t.Fatalf("estimated by %T", estimate)
			}
			res := renderPlan(t, estimate, testPlannerContext(3))
			if !strings.Contains(res, "count()") || !strings.Contains(res, "tempo_traces") {
				t.Errorf("no count over the traces table:\n%s", res)
			}
			if bounds := spanStartBounds(res); !slices.Equal(bounds, []string{">86390", "<=86410"}) {
				t.Errorf("bounds %v:\n%s", bounds, res)
			}
		})
	}
}

func TestSpanOnlyQueriesKeepTheirEstimates(t *testing.T) {
	for _, query := range []string{`{.a = 1} && {.b = 2}`, `{.a = 1} || {.b = 2}`} {
		t.Run(query, func(t *testing.T) {
			estimate, ok := estimateOf(t, query).(*ComplexEvalOrPlanner)
			if !ok {
				t.Fatalf("estimated by %T", estimateOf(t, query))
			}
			for _, operand := range estimate.Operands {
				if _, all := operand.(*AllSpansEvaluatorPlanner); all {
					t.Errorf("operand estimates every span")
				}
			}
		})
	}
	for _, query := range []string{`{.a = 1}`, `{.a} | count() > 2`} {
		t.Run(query, func(t *testing.T) {
			if _, all := estimateOf(t, query).(*AllSpansEvaluatorPlanner); all {
				t.Errorf("estimates every span")
			}
		})
	}
	if _, attrless := estimateOf(t, `{}`).(*AttrlessEvaluatorPlanner); !attrless {
		t.Errorf("{} is estimated by %T", estimateOf(t, `{}`))
	}
}
