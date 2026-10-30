package clickhouse_transpiler

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func TestSearchPlansStructuralOperators(t *testing.T) {
	for _, c := range []struct{ query, want string }{
		{`{.a = 1} > {.b = 2}`, `>({.a},{.b})`},
		{`{.a = 1} < {.b = 2}`, `<({.a},{.b})`},
		{`{.a = 1} ~ {.b = 2}`, `~({.a},{.b})`},
		{`{.a = 1} !> {.b = 2}`, `!>({.a},{.b})`},
		{`{.a = 1} !< {.b = 2}`, `!<({.a},{.b})`},
		{`{.a = 1} !~ {.b = 2}`, `!~({.a},{.b})`},
		{`{.a = 1} &> {.b = 2}`, `&>({.a},{.b})`},
		{`{.a = 1} &< {.b = 2}`, `&<({.a},{.b})`},
		{`{.a = 1} &~ {.b = 2}`, `&~({.a},{.b})`},
		{`{.a = 1} > {.b = 2} > {.c = 3}`, `>(>({.a},{.b}),{.c})`},
		{`{.a = 1} > ({.b = 2} > {.c = 3})`, `>({.a},>({.b},{.c}))`},
		{`{.a = 1} || {.b = 2} > {.c = 3} && {.d = 4}`, `and(or({.a},>({.b},{.c})),{.d})`},
		{`{} > {.b = 2}`, `>({},{.b})`},
		{`{.a = 1} >> {.b = 2}`, `>>({.a},{.b})`},
		{`{.a = 1} << {.b = 2}`, `<<({.a},{.b})`},
		{`{.a = 1} !>> {.b = 2}`, `!>>({.a},{.b})`},
		{`{.a = 1} !<< {.b = 2}`, `!<<({.a},{.b})`},
		{`{.a = 1} &>> {.b = 2}`, `&>>({.a},{.b})`},
		{`{.a = 1} &<< {.b = 2}`, `&<<({.a},{.b})`},
		{`{.a = 1} >> {.b = 2} < {.c = 3}`, `<(>>({.a},{.b}),{.c})`},
		{`{} !<< ({.b = 2} >> {.c = 3}) || {.d = 4}`, `or(!<<({},>>({.b},{.c})),{.d})`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); got != c.want {
				t.Errorf("plan = %s, want %s", got, c.want)
			}
		})
	}
}

// structuralWiths returns the CTEs of a rendered-ready search plan by alias.
func structuralWiths(t *testing.T, query string, ctx *shared.PlannerContext) map[string]*sql.With {
	t.Helper()
	req, err := planSearch(t, query).Ids[0].Statement.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res := map[string]*sql.With{}
	for _, w := range req.GetWith() {
		res[w.GetAlias()] = w
	}
	return res
}

func renderWith(t *testing.T, w *sql.With) string {
	t.Helper()
	res, err := w.String(sql.DefaultCtx(), sql.STRING_OPT_SKIP_WITH)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func withSuffix(withs map[string]*sql.With, suffix string) []*sql.With {
	var res []*sql.With
	for alias, w := range withs {
		if strings.HasSuffix(alias, suffix) {
			res = append(res, w)
		}
	}
	return res
}

func recursiveWiths(withs map[string]*sql.With) []*sql.With {
	var res []*sql.With
	for _, w := range withs {
		if w.IsRecursive() {
			res = append(res, w)
		}
	}
	return res
}

func renderSelect(t *testing.T, s sql.ISelect) string {
	t.Helper()
	res, err := s.String(sql.DefaultCtx(), sql.STRING_OPT_SKIP_WITH)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// One-hop operators relate spans in one pass over their keys; only root
// reachability recurses, over traces with a missing parent.
func TestOneHopRecursesOnlyForIncompleteTraces(t *testing.T) {
	for _, query := range oneHopQueries {
		t.Run(query, func(t *testing.T) {
			withs := structuralWiths(t, query, testPlannerContext(3))
			recursive := recursiveWiths(withs)
			if len(recursive) != 1 || !strings.HasSuffix(recursive[0].GetAlias(), "_jump") {
				t.Fatalf("want one recursive reachability CTE, got %d", len(recursive))
			}
			anchor, err := recursive[0].GetQuery().String(sql.DefaultCtx(), sql.STRING_OPT_SKIP_WITH)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(anchor, "has_root = 1 AND has_dangling = 1") {
				t.Errorf("reachability is not limited to traces with a missing parent:\n%s", anchor)
			}
			step, err := recursive[0].GetStep().String(sql.DefaultCtx(), sql.STRING_OPT_SKIP_WITH)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(step, fmt.Sprintf("((x.k) < (%d))", traceqlconfig.DefaultAncestryRounds)) {
				t.Errorf("reachability rounds are unbounded:\n%s", step)
			}
			for _, w := range withSuffix(withs, "_counted") {
				if res := renderWith(t, w); !strings.Contains(res, "OVER (PARTITION BY trace_id, reachable, key)") {
					t.Errorf("relation is not one pass over parent keys:\n%s", res)
				}
			}
		})
	}
}

// Transitive operators jump pointers over every span of the candidate traces.
func TestTransitiveOperatorsJumpOverEveryCandidateSpan(t *testing.T) {
	for _, query := range transitiveQueries {
		t.Run(query, func(t *testing.T) {
			withs := structuralWiths(t, query, testPlannerContext(3))
			jumps := withSuffix(withs, "transitive_jump")
			if len(jumps) == 0 || len(recursiveWiths(withs)) != len(jumps) {
				t.Fatalf("want only transitive jump CTEs to recurse, got %d of %d", len(jumps), len(recursiveWiths(withs)))
			}
			for _, jump := range jumps {
				if anchor := renderSelect(t, jump.GetQuery()); strings.Contains(anchor, "has_dangling") {
					t.Errorf("transitive jump is limited to incomplete traces:\n%s", anchor)
				}
			}
		})
	}
}

// Every pointer jump runs the configured number of rounds and fails the query
// when a pointer is still unresolved after the last one.
func TestAncestryRoundsComeFromTheSettingWithAGuard(t *testing.T) {
	for _, rounds := range []int{0, 3, 7} {
		want := rounds
		if rounds == 0 {
			want = traceqlconfig.DefaultAncestryRounds
		}
		for _, query := range structuralQueries {
			ctx := testPlannerContext(3)
			ctx.AncestryRounds = rounds
			withs := structuralWiths(t, query, ctx)
			guarded := map[string]bool{}
			for _, w := range withs {
				body := renderWith(t, w)
				for _, jump := range recursiveWiths(withs) {
					if strings.Contains(body, "FROM "+jump.GetAlias()+" ") && strings.Contains(body,
						fmt.Sprintf("throwIf(k = %d AND anc != %s, '%s')", want, zeroSpanID, unresolvedAncestryMessage(want))) {
						guarded[jump.GetAlias()] = true
					}
				}
			}
			for _, jump := range recursiveWiths(withs) {
				if step := renderSelect(t, jump.GetStep()); !strings.Contains(step, fmt.Sprintf("((x.k) < (%d))", want)) {
					t.Errorf("rounds=%d %s: %s does not stop after %d rounds:\n%s", rounds, query, jump.GetAlias(), want, step)
				}
				if !guarded[jump.GetAlias()] {
					t.Errorf("rounds=%d %s: %s has no unresolved-ancestry guard", rounds, query, jump.GetAlias())
				}
			}
		}
	}
}

func TestDefaultAncestryRoundsIs20(t *testing.T) {
	if traceqlconfig.DefaultAncestryRounds != 20 {
		t.Errorf("default ancestry rounds = %d, want 20", traceqlconfig.DefaultAncestryRounds)
	}
}

func TestUnresolvedAncestryMessageNamesTheSetting(t *testing.T) {
	msg := unresolvedAncestryMessage(3)
	for _, want := range []string{"deeper than", "8 spans", traceqlconfig.AncestryRoundsEnv} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q does not contain %q", msg, want)
		}
	}
	ctx := testPlannerContext(3)
	ctx.AncestryRounds = 3
	chErr := errors.New("code: 395, message: " + msg + ": while executing 'FUNCTION throwIf(...)'")
	if err := UnresolvedAncestryError(ctx, chErr); err == nil || err.Error() != msg {
		t.Errorf("UnresolvedAncestryError = %v, want %q", err, msg)
	}
	other := errors.New("code: 241, message: memory limit exceeded")
	if err := UnresolvedAncestryError(ctx, other); err != other {
		t.Errorf("UnresolvedAncestryError changed an unrelated error: %v", err)
	}
}

func TestStructuralEdgesComeFromTheTracesTableWithoutTimeBound(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		ctx := testPlannerContext(3)
		ctx.IsCluster = cluster
		table := "FROM tempo_traces as traces"
		in := " IN ("
		if cluster {
			table = "FROM tempo_traces_dist as traces"
			in = " GLOBAL IN ("
		}
		for _, query := range []string{`{.a = 1} > {.b = 2}`, `{.a = 1} << {.b = 2}`} {
			edges := withSuffix(structuralWiths(t, query, ctx), "_edges")
			if len(edges) != 1 {
				t.Fatalf("cluster=%v %s: %d edge CTEs, want 1", cluster, query, len(edges))
			}
			res := renderWith(t, edges[0])
			if !strings.Contains(res, table) || strings.Count(res, "traces.trace_id"+in) != 2 {
				t.Errorf("cluster=%v %s: edges are not read from the traces table by trace id:\n%s", cluster, query, res)
			}
			if strings.Contains(res, "timestamp_ns") || strings.Contains(res, "date") {
				t.Errorf("cluster=%v %s: edges are bounded in time:\n%s", cluster, query, res)
			}
		}
	}
}

func TestEnableAnalyzerOnlyOnStructuralQueries(t *testing.T) {
	setting := regexp.MustCompile(`enable_analyzer`)
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planSearch(t, query).Ids[0].Statement, testPlannerContext(3))
			want := 0
			for _, q := range structuralQueries {
				if q == query {
					want = 1
				}
			}
			if n := len(setting.FindAllString(res, -1)); n != want {
				t.Errorf("enable_analyzer appears %d times, want %d:\n%s", n, want, res)
			}
			if want == 1 && !strings.HasSuffix(res, " SETTINGS enable_analyzer=1") {
				t.Errorf("enable_analyzer is not a query setting:\n%s", res)
			}
		})
	}
}

func TestStructuralQueriesNeedClickHouse249(t *testing.T) {
	ctx := testPlannerContext(3)
	ctx.VersionInfo = map[string]int64{}
	for _, query := range structuralQueries {
		if _, err := planSearch(t, query).Ids[0].Statement.Process(ctx); err != errStructuralVersion {
			t.Errorf("%s: error = %v, want %v", query, err, errStructuralVersion)
		}
	}
	for _, query := range plainSearchQueries {
		plan := planSearch(t, query)
		for _, statement := range []shared.SQLRequestPlanner{plan.Ids[0].Statement, plan.Data} {
			if _, err := statement.Process(ctx); err != nil {
				t.Errorf("%s: %v", query, err)
			}
		}
	}
}

// `{}` under a spanset operator is every span in range.
func TestEmptyOperandReadsEverySpanInRange(t *testing.T) {
	for _, query := range []string{`{} > {.a = 1}`, `{.a = 1} && {}`} {
		t.Run(query, func(t *testing.T) {
			res := renderSearch(t, planSearch(t, query), testPlannerContext(3))
			if strings.Contains(res, "attrless_trace_ids") {
				t.Errorf("operand {} uses the trace pre-pick:\n%s", res)
			}
		})
	}
	q, err := traceql_ir.Parse(`{} ~ {.a = 1}`)
	if err != nil {
		t.Fatal(err)
	}
	eval, err := PlanEval(q)
	if err != nil {
		t.Fatal(err)
	}
	operands := eval.(*EvalFinalizerPlanner).Main.(*ComplexEvalOrPlanner).Operands
	if _, ok := operands[0].(*AllSpansEvaluatorPlanner); !ok {
		t.Errorf("operand {} is estimated by %T", operands[0])
	}
}
