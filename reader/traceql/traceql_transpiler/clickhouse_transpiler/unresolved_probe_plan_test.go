package clickhouse_transpiler

import (
	"fmt"
	"regexp"
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

				req, err := planSearch(t, query).Ids[0].Statement.Process(testPlannerContext(7))
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
						finalJump,
						unresolvedRow(rounds),
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

// On a cluster each probe runs whole on each shard over local tables and
// returns a shard's count and ids; on a single node it stays a plain statement.
func TestUnresolvedProbeRunsOnEachShard(t *testing.T) {
	coordinator := regexp.MustCompile(`^\s*SELECT n,\s*ids\s*$`)
	settings := regexp.MustCompile(`^ SETTINGS enable_analyzer=1, prefer_localhost_replica=0$`)
	for _, query := range structuralQueries {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			jumps, stmts := planProbes(t, query, ctx)
			if len(jumps) == 0 {
				t.Fatal("no probes")
			}
			for i, stmt := range stmts {
				m := shardViewThen(`(.*)`).FindStringSubmatch(stmt)
				if m == nil {
					t.Errorf("%s is not run on each shard:\n%s", jumps[i], stmt)
					continue
				}
				if !coordinator.MatchString(m[1]) {
					t.Errorf("%s: coordinator part %q, want n and ids", jumps[i], m[1])
				}
				shard := m[2]
				for _, want := range []string{"groupUniqArray(20)", "FROM " + jumps[i] + " ", finalJump} {
					if !strings.Contains(shard, want) {
						t.Errorf("%s: shard statement lacks %q:\n%s", jumps[i], want, shard)
					}
				}
				if strings.Contains(shard, "_dist") {
					t.Errorf("%s: shard statement reads a Distributed table:\n%s", jumps[i], shard)
				}
				if !settings.MatchString(m[3]) {
					t.Errorf("%s: coordinator settings %q, want the analyzer on every shard", jumps[i], m[3])
				}
			}
			_, plain := planProbes(t, query, testPlannerContext(3))
			for _, stmt := range plain {
				if strings.Contains(stmt, "cluster(") || strings.Contains(stmt, "prefer_localhost_replica") {
					t.Errorf("single-node probe holds a shard statement:\n%s", stmt)
				}
				if !strings.Contains(stmt, "enable_analyzer=1") {
					t.Errorf("single-node probe does not set the analyzer:\n%s", stmt)
				}
			}
		})
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
