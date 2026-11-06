package clickhouse_transpiler

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

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
	req, err := planSearch(t, query).Process(ctx)
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

// unresolvedAnc is a jump row's ancestor pointer still being set.
const unresolvedAnc = "anc != toFixedString('', 8)"

// jumpReaders is the aliases of the CTEs other than the jumps that read jump.
func jumpReaders(t *testing.T, withs map[string]*sql.With, jump *sql.With) []string {
	t.Helper()
	var res []string
	for alias, w := range withs {
		if !w.IsRecursive() && strings.Contains(renderWith(t, w), "FROM "+jump.GetAlias()+" ") {
			res = append(res, alias)
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

// Every pointer jump runs the configured number of rounds, and a row whose
// pointer is still set after the last one is unreachable.
func TestAncestryRoundsLeaveUnresolvedRowsUnreachable(t *testing.T) {
	for _, rounds := range []int{0, 3, 7} {
		want := rounds
		if rounds == 0 {
			want = traceqlconfig.DefaultAncestryRounds
		}
		for _, query := range structuralQueries {
			ctx := testPlannerContext(3)
			ctx.AncestryRounds = rounds
			if plan := renderPlan(t, planSearch(t, query), ctx); strings.Contains(plan, "throwIf") {
				t.Errorf("rounds=%d %s: plan can fail the query:\n%s", rounds, query, plan)
			}
			withs := structuralWiths(t, query, ctx)
			for _, jump := range recursiveWiths(withs) {
				if step := renderSelect(t, jump.GetStep()); !strings.Contains(step, fmt.Sprintf("((x.k) < (%d))", want)) {
					t.Errorf("rounds=%d %s: %s does not stop after %d rounds:\n%s", rounds, query, jump.GetAlias(), want, step)
				}
				if readers := jumpReaders(t, withs, jump); len(readers) != 1 {
					t.Errorf("rounds=%d %s: %s is read by %v, want one pass", rounds, query, jump.GetAlias(), readers)
				}
			}
			resolved, unreachable := withSuffix(withs, "_resolved"), withSuffix(withs, "_unreachable")
			if len(resolved)+len(unreachable) == 0 {
				t.Errorf("rounds=%d %s: no ancestry reader in the plan", rounds, query)
			}
			for _, w := range resolved {
				res := renderWith(t, w)
				for _, part := range []string{"cut = 0 AND NOT (" + unresolvedAnc + ")", fmt.Sprintf("k = %d", want)} {
					if !strings.Contains(res, part) {
						t.Errorf("rounds=%d %s: %s lacks %q:\n%s", rounds, query, w.GetAlias(), part, res)
					}
				}
			}
			for _, w := range unreachable {
				part := fmt.Sprintf("k = %d AND (cut = 1 OR %s)", want, unresolvedAnc)
				if res := renderWith(t, w); !strings.Contains(res, part) {
					t.Errorf("rounds=%d %s: %s lacks %q:\n%s", rounds, query, w.GetAlias(), part, res)
				}
			}
		}
	}
}

// unresolvedTraceFlag is the per-trace flag on a transitive operator's jump.
var unresolvedTraceFlag = "max(" + unresolvedAnc + ") OVER (PARTITION BY trace_id)"

// assertUnresolvedTraceSkip checks that every transitive operator of the plan
// carries the flag to its final condition, or that none has it.
func assertUnresolvedTraceSkip(t *testing.T, req sql.ISelect, want bool) {
	t.Helper()
	plan, err := req.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plan, "throwIf") {
		t.Errorf("plan can fail the query:\n%s", plan)
	}
	var resolved int
	for _, w := range req.GetWith() {
		if strings.HasSuffix(w.GetAlias(), "_resolved") {
			resolved++
			if res := renderWith(t, w); strings.Contains(res, unresolvedTraceFlag) != want {
				t.Errorf("%s: flag present = %v, want %v:\n%s", w.GetAlias(), !want, want, res)
			}
		}
	}
	if resolved == 0 {
		t.Errorf("no transitive operator in the plan:\n%s", plan)
	}
	if strings.Contains(plan, "unresolved = 0 AND (") != want {
		t.Errorf("final condition on the flag = %v, want %v:\n%s", !want, want, plan)
	}
}

// Operators reading down marks skip a trace with an unresolved span; those
// reading up marks test the row alone and read no flag.
func TestTransitiveOperatorsSkipUnresolvedTracesOnlyWhenReadingDownMarks(t *testing.T) {
	for _, c := range []struct {
		op   string
		skip bool
	}{
		{">>", false}, {"!>>", false},
		{"<<", true}, {"!<<", true}, {"&>>", true}, {"&<<", true},
	} {
		t.Run(c.op, func(t *testing.T) {
			req, err := planSearch(t, "{.a = 1} "+c.op+" {.b = 2}").Process(testPlannerContext(3))
			if err != nil {
				t.Fatal(err)
			}
			assertUnresolvedTraceSkip(t, req, c.skip)
		})
	}
}

// One-hop operators test the row alone.
func TestOneHopOperatorsHaveNoUnresolvedTraceFlag(t *testing.T) {
	for _, query := range oneHopQueries {
		t.Run(query, func(t *testing.T) {
			withs := structuralWiths(t, query, testPlannerContext(3))
			for _, w := range withSuffix(withs, "_unreachable") {
				if res := renderWith(t, w); strings.Contains(res, "OVER") {
					t.Errorf("%s has a window:\n%s", w.GetAlias(), res)
				}
			}
			if plan := renderPlan(t, planSearch(t, query), testPlannerContext(3)); strings.Contains(plan, "unresolved") {
				t.Errorf("plan has an unresolved flag:\n%s", plan)
			}
		})
	}
}

// Every entry point inherits the skip through the shared planners.
func TestEveryEntryPointSkipsUnresolvedTraces(t *testing.T) {
	const skipped, unskipped = `{.a = 1} !<< {.b = 2}`, `{.a = 1} !>> {.b = 2}`
	entries := map[string]func(t *testing.T, query string) shared.SQLRequestPlanner{
		"metrics": planMetrics,
		"count stage": func(t *testing.T, query string) shared.SQLRequestPlanner {
			return planSearch(t, query+" | count() > 1")
		},
		"tags": func(t *testing.T, query string) shared.SQLRequestPlanner {
			return autocompletePlans(t, query)["tags"]
		},
		"values": func(t *testing.T, query string) shared.SQLRequestPlanner {
			return autocompletePlans(t, query)["values"]
		},
		"compare": func(t *testing.T, query string) shared.SQLRequestPlanner {
			q, err := traceql_ir.Parse("(" + query + ") | compare({.c = 3})")
			if err != nil {
				t.Fatal(err)
			}
			plan, err := PlanCompare(q, q.Metrics.Compare.Filter, time.Unix(86000, 0), time.Unix(86500, 0), compareSpans)
			if err != nil {
				t.Fatal(err)
			}
			return plan
		},
	}
	for name, plan := range entries {
		t.Run(name, func(t *testing.T) {
			for query, want := range map[string]bool{skipped: true, unskipped: false} {
				req, err := plan(t, query).Process(testPlannerContext(3))
				if err != nil {
					t.Fatal(err)
				}
				assertUnresolvedTraceSkip(t, req, want)
			}
		})
	}
}

func TestDefaultAncestryRoundsIs20(t *testing.T) {
	if traceqlconfig.DefaultAncestryRounds != 20 {
		t.Errorf("default ancestry rounds = %d, want 20", traceqlconfig.DefaultAncestryRounds)
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
			res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
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
		if _, err := planSearch(t, query).Process(ctx); err != errStructuralVersion {
			t.Errorf("%s: error = %v, want %v", query, err, errStructuralVersion)
		}
	}
	for _, query := range plainSearchQueries {
		if _, err := planSearch(t, query).Process(ctx); err != nil {
			t.Errorf("%s: %v", query, err)
		}
	}
}

// `{}` under a spanset operator is every span in range.
func TestEmptyOperandReadsEverySpanInRange(t *testing.T) {
	for _, query := range []string{`{} > {.a = 1}`, `{.a = 1} && {}`} {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
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
