package clickhouse_transpiler

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// planProbes renders the unresolved-trace probes of query by jump alias, with
// the alias order of the plan.
func planProbes(t *testing.T, query string, ctx *shared.PlannerContext) ([]string, []string) {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanUnresolvedProbes(q)
	if err != nil {
		t.Fatal(err)
	}
	probes, err := plan.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var jumps, stmts []string
	for _, p := range probes {
		stmt, err := p.Select.String(sql.DefaultCtx())
		if err != nil {
			t.Fatal(err)
		}
		jumps = append(jumps, p.Jump)
		stmts = append(stmts, stmt)
	}
	return jumps, stmts
}

// A probe is one statement per recursive CTE of the search plan: it reads the
// jump's last round and reports the unresolved rows' traces.
func TestUnresolvedProbeReadsEachJump(t *testing.T) {
	queries := append(slices.Clone(structuralQueries),
		`({.a = 1} >> {.b = 2}) &<< ({.a = 1} > {.b = 2})`,
		`{.a = 1} && ({.b = 2} ~ {.c = 3})`)
	for _, rounds := range []int{3, 7} {
		for _, query := range queries {
			t.Run(fmt.Sprintf("%d rounds %s", rounds, query), func(t *testing.T) {
				ctx := testPlannerContext(7)
				ctx.AncestryRounds = rounds
				jumps, stmts := planProbes(t, query, ctx)

				req, err := planSearch(t, query).Process(testPlannerContext(7))
				if err != nil {
					t.Fatal(err)
				}
				var want []string
				for _, w := range req.GetWith() {
					if w.IsRecursive() {
						want = append(want, w.GetAlias())
					}
				}
				if len(want) == 0 || !slices.Equal(jumps, want) {
					t.Fatalf("probe jumps = %q, search jumps = %q", jumps, want)
				}
				for i, stmt := range stmts {
					tail := stmt[strings.LastIndex(stmt, "SELECT uniqExact(trace_id)"):]
					for _, part := range []string{
						"groupUniqArray(20)(lower(hex(trace_id)))",
						"FROM " + want[i] + " ",
						fmt.Sprintf("k = %d", rounds),
						unresolvedAnc,
						"enable_analyzer",
					} {
						if !strings.Contains(tail, part) {
							t.Errorf("no %q in %s", part, tail)
						}
					}
					if strings.Contains(tail, "UNION ALL") {
						t.Errorf("statement reads a union: %s", tail)
					}
				}
			})
		}
	}
}

func TestUnresolvedProbeReadsTheContext(t *testing.T) {
	const query = `{.a = 1} >> {.b = 2}`

	ctx := testPlannerContext(7)
	ctx.RandomFilter = shared.RandomFilter{Max: 3, I: 1}
	_, stmts := planProbes(t, query, ctx)
	if !strings.Contains(stmts[0], "cityHash64(trace_id) % 3") {
		t.Errorf("portion filter missing: %s", stmts[0])
	}

	ctx = testPlannerContext(7)
	ctx.From, ctx.To = time.Unix(1000, 0), time.Unix(2000, 0)
	_, stmts = planProbes(t, query, ctx)
	if !strings.Contains(stmts[0], "1000000000000") {
		t.Errorf("context window missing: %s", stmts[0])
	}

	_, stmts = planProbes(t, query+` | rate() with(sample=0.5)`, testPlannerContext(7))
	if !strings.Contains(stmts[0], "cityHash64(trace_id) <") {
		t.Errorf("sample filter missing: %s", stmts[0])
	}
}

// A probe samples where its request does: metrics and compare(), not search.
func TestUnresolvedProbeSamplesMetricsOnly(t *testing.T) {
	const query = `{.a = 1} >> {.b = 2}`
	for hinted, want := range map[string]bool{
		query + ` | rate() with(sample=0.5)`:            true,
		query + ` | compare({.c = 3}) with(sample=0.5)`: true,
		query + ` with(sample=0.5)`:                     false,
	} {
		t.Run(hinted, func(t *testing.T) {
			_, stmts := planProbes(t, hinted, testPlannerContext(7))
			if got := sampleCond.MatchString(stmts[0]); got != want {
				t.Errorf("sampled = %v, want %v: %s", got, want, stmts[0])
			}
		})
	}
}

func TestUnresolvedProbeNeedsStructuralCapability(t *testing.T) {
	q, err := traceql_ir.Parse(`{.a = 1} >> {.b = 2}`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanUnresolvedProbes(q)
	if err != nil {
		t.Fatal(err)
	}
	ctx := testPlannerContext(7)
	ctx.VersionInfo = map[string]int64{}
	if _, err := plan.Process(ctx); err != errStructuralVersion {
		t.Errorf("error = %v, want %v", err, errStructuralVersion)
	}
}

func TestUnresolvedProbeOfPlainQueryIsEmpty(t *testing.T) {
	jumps, _ := planProbes(t, `{.a = 1}`, testPlannerContext(7))
	if len(jumps) != 0 {
		t.Errorf("probes = %q", jumps)
	}
}
