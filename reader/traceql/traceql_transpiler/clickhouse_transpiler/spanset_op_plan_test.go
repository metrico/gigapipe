package clickhouse_transpiler

import (
	"fmt"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// describePlan prints the planner tree: and(…), or(…), a structural op(…), a
// selector as {attr…} and a pipeline as its stages joined by |.
func describePlan(t *testing.T, p shared.SQLRequestPlanner) string {
	t.Helper()
	switch p := p.(type) {
	case *TraceAndPlanner:
		return fmt.Sprintf("and(%s,%s)", describePlan(t, p.LHS), describePlan(t, p.RHS))
	case *TraceOrPlanner:
		return fmt.Sprintf("or(%s,%s)", describePlan(t, p.LHS), describePlan(t, p.RHS))
	case *OneHopPlanner:
		return fmt.Sprintf("%s(%s,%s)", p.Op, describePlan(t, p.LHS), describePlan(t, p.RHS))
	case *TransitivePlanner:
		return fmt.Sprintf("%s(%s,%s)", p.Op, describePlan(t, p.LHS), describePlan(t, p.RHS))
	case *AllSpansPlanner:
		return "{}"
	case *AttrlessConditionPlanner:
		return "attrless"
	case *pipelinePlanner:
		stages := make([]string, len(p.Stages))
		for i, stage := range p.Stages {
			switch stage := stage.(type) {
			case *spansetStage:
				stages[i] = describePlan(t, stage.Expr)
			case *scalarFilterStage:
				stages[i] = fmt.Sprintf("%s %s %g", stage.agg, stage.op, stage.value)
			case *groupByStage:
				stages[i] = stage.by.String()
			case coalesceStage:
				stages[i] = "coalesce()"
			}
		}
		return strings.Join(stages, "|")
	case *FieldFilterPlanner:
		return "pivot{" + p.Filter.String() + "}"
	case *AttrConditionPlanner:
		attrs := make([]string, len(p.Terms))
		for i, term := range p.Terms {
			attrs[i] = term.Attr
		}
		return "{" + strings.Join(attrs, ",") + "}"
	}
	t.Fatalf("unexpected expression planner %T", p)
	return ""
}

func TestSearchPlansSpansetOperatorTree(t *testing.T) {
	for _, c := range []struct{ query, want string }{
		{`{.a = 1} && {.b = 2}`, `and({.a},{.b})`},
		{`{.a = 1} || {.b = 2}`, `or({.a},{.b})`},
		{`{.a = 1} && {.b = 2} && {.c = 3}`, `and(and({.a},{.b}),{.c})`},
		{`{.a = 1} && {.b = 2} || {.c = 3}`, `or(and({.a},{.b}),{.c})`},
		{`{.a = 1} || {.b = 2} && {.c = 3}`, `and(or({.a},{.b}),{.c})`},
		{`({.a = 1} || {.b = 2}) && {.c = 3}`, `and(or({.a},{.b}),{.c})`},
		{`{.a = 1} && ({.b = 2} || {.c = 3})`, `and({.a},or({.b},{.c}))`},
		{`({.a = 1}) && {.b = 2}`, `and({.a},{.b})`},
		{`(({.a = 1} && {.b = 2}))`, `and({.a},{.b})`},
		{`(({.a = 1}))`, `{.a}`},
		{`({.a = 1} | count() > 1) && {.b = 2}`, `and({.a}|count() > 1,{.b})`},
		{`{.a = 1} && {.b = 2} | count() > 1`, `and({.a},{.b})|count() > 1`},
		{`count() > 1`, `{}|count() > 1`},
		{`{.a = 1 && .x = 2} || {.b = 2} && ({.c = 3} || {.d = 4})`, `and(or({.a,.x},{.b}),or({.c},{.d}))`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); got != c.want {
				t.Errorf("plan = %s, want %s", got, c.want)
			}
		})
	}
}

func TestSearchSpansetOperatorsAreTraceLevel(t *testing.T) {
	for _, query := range []string{`{.a = 1} && {.b = 2}`, `{.a = 1} || {.b = 2}`} {
		res := renderSearch(t, planSearch(t, query), testPlannerContext(3))
		if strings.Contains(res, "INTERSECT") {
			t.Errorf("%s intersects spans:\n%s", query, res)
		}
	}
	res := renderSearch(t, planSearch(t, `{.a = 1} && {.b = 2}`), testPlannerContext(3))
	if !strings.Contains(res, "OVER (PARTITION BY trace_id)") {
		t.Errorf("&& is not evaluated per trace:\n%s", res)
	}
}

// Every operand keeps its own conditions however deep it is nested.
func TestSearchPlansEveryOperand(t *testing.T) {
	const query = `({.a = "w"} && {.b = "x"}) || ({.c = "y"} && ({.d = "z"} || {.e = "v"}))`
	res := renderSearch(t, planSearch(t, query), testPlannerContext(3))
	for _, v := range []string{"w", "x", "y", "z", "v"} {
		if !strings.Contains(res, fmt.Sprintf("'%s'", v)) {
			t.Errorf("operand %q missing:\n%s", v, res)
		}
	}
}

func TestSpansetOperandChecks(t *testing.T) {
	for _, c := range []struct {
		query string
		err   error
	}{
		{`{} && {.a = 1}`, nil},
		{`{.a = 1} || {}`, nil},
		{`({}) > {.a = 1}`, nil},
		{`{.a = 1} && ({.b = 2} || {})`, nil},
		{`({} | count() > 1) && {.a = 1}`, nil},
		{`{.a = 1} && ({.b = 2})`, nil},
		{`({.a = 1} || {.b = 2}) && {.c = 3}`, nil},
	} {
		t.Run(c.query, func(t *testing.T) {
			q, err := traceql_ir.Parse(c.query)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Plan(q); err != c.err {
				t.Errorf("Plan error = %v, want %v", err, c.err)
			}
			if _, err := PlanEval(q); err != c.err {
				t.Errorf("PlanEval error = %v, want %v", err, c.err)
			}
		})
	}
}

func TestComplexityEvaluatesEveryOperand(t *testing.T) {
	q, err := traceql_ir.Parse(`{.a = 1} && ({.b = 2} || {.c = 3}) && {.d = 4}`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanEval(q)
	if err != nil {
		t.Fatal(err)
	}
	final, ok := plan.(*EvalFinalizerPlanner)
	if !ok {
		t.Fatalf("eval plan is %T", plan)
	}
	operands, ok := final.Main.(*ComplexEvalOrPlanner)
	if !ok {
		t.Fatalf("eval operands planner is %T", final.Main)
	}
	if len(operands.Operands) != 4 {
		t.Errorf("%d operands evaluated, want 4", len(operands.Operands))
	}
	prefixes := map[string]bool{}
	for _, op := range operands.Operands {
		prefixes[op.(*AttrConditionEvaluatorPlanner).Prefix] = true
	}
	if len(prefixes) != 4 {
		t.Errorf("operand prefixes are not distinct: %v", prefixes)
	}
}
