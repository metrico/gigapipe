package clickhouse_transpiler

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

var plainSearchQueries = []string{
	`{.a = 1}`,
	`{}`,
	`{.a = 1} | count() > 1`,
	`{.a = 1} | avg(.b) > 1`,
	`{.a = 1} && {.b = 2}`,
	`{.a = 1} || {.b = 2}`,
	`{.a = 1} && {.b = 2} || {.c = 3}`,
	`({.a = 1} || {.b = 2}) && {.c = 3}`,
	`({.a = 1} | count() > 1) && {.b = 2}`,
	`count() > 1`,
	`{.a = 1} | count() > 1 | avg(duration) > 1ms | max(.b) < 3`,
	`{nestedSetParent < 0 && .a = 1}`,
	`{nestedSetParent < 0}`,
	`{nestedSetRight - nestedSetLeft = 1 && .a = 1}`,
	`{.a = 1} | by(nestedSetParent) | max(nestedSetLeft) > 1`,
	`{.a = .b}`,
	`{!(.a = 1) || duration > 100}`,
	`{duration * 2 > 1s}`,
	`{.a = 1} | avg(.b + .c) > 1`,
	`{.a = 1} | by(.b) | count() > 1 | coalesce()`,
	`by(.b * 2) | by(duration)`,
}

// oneHopQueries relate spans to their parent or siblings.
var oneHopQueries = []string{
	`{.a = 1} > {.b = 2}`,
	`{.a = 1} !~ {}`,
	`{} &< {.b = 2} || {.c = 3}`,
}

// transitiveQueries relate spans to their ancestors or descendants.
var transitiveQueries = []string{
	`{.a = 1} >> {.b = 2}`,
	`{.a = 1} !<< {}`,
	`{} &>> {.b = 2} && {.c = 3}`,
	`{.a = 1} &<< ({.b = 2} !>> {.c = 3})`,
	`{.a = 1} >> {.b = 2} | sum(.c) > 1`,
	`{.a = .b} >> {.c = nil}`,
}

// structuralQueries hold operators evaluated on the span tree.
var structuralQueries = append(append([]string{}, oneHopQueries...), transitiveQueries...)

var searchQueries = append(append([]string{}, plainSearchQueries...), structuralQueries...)

func testPlannerContext(spss int64) *shared.PlannerContext {
	distconfig.Init()
	return &shared.PlannerContext{
		ClusterName:              "test_cluster",
		From:                     time.Unix(86390, 0).UTC(),
		To:                       time.Unix(86410, 0).UTC(),
		Limit:                    5,
		SpansPerSpanSet:          spss,
		TracesAttrsTable:         "tempo_traces_attrs_gin",
		TracesAttrsDistTable:     "tempo_traces_attrs_gin_dist",
		TracesTable:              "tempo_traces",
		TracesDistTable:          "tempo_traces_dist",
		TracesNestedSetTable:     "tempo_traces_nested_set",
		TracesNestedSetDistTable: "tempo_traces_nested_set_dist",
		VersionInfo:              map[string]int64{dbversion.CapStructuralQueries: 0},
	}
}

func renderPlan(t *testing.T, planner shared.SQLRequestPlanner, ctx *shared.PlannerContext) string {
	t.Helper()
	req, err := planner.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := req.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// renderSearch renders plan's Search and Data statements, in that order.
func renderSearch(t *testing.T, plan *SearchPlan, ctx *shared.PlannerContext) string {
	t.Helper()
	return renderPlan(t, plan.Ids[0].Statement, ctx) + ";\n" + renderPlan(t, plan.Data, ctx)
}

func planSearch(t *testing.T, query string) *SearchPlan {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestSearchOperandSetsAreUncapped(t *testing.T) {
	capped := regexp.MustCompile(`group(Uniq)?Array\(\d+\)`)
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderSearch(t, planSearch(t, query), testPlannerContext(7))
			if m := capped.FindString(res); m != "" {
				t.Errorf("operand set capped by %s:\n%s", m, res)
			}
		})
	}
}

// searchStages unwraps the planners of the Search statement above the
// evaluated expression.
func searchStages(t *testing.T, search *SearchPlan) (stages []string, expr shared.SQLRequestPlanner) {
	t.Helper()
	plan := search.Ids[0].Statement
	for {
		switch p := plan.(type) {
		case *StructuralQueryPlanner:
			stages = append(stages, "structural")
			plan = p.Main
		case *IndexLimitPlanner:
			stages = append(stages, "limit")
			plan = p.Main
		case *searchRowsPlanner:
			stages = append(stages, "rows")
			plan = p.Main
		case *shardLocalPlanner:
			stages = append(stages, "shard-local")
			plan = p.Single
		case *SearchSpansetPlanner:
			stages = append(stages, "spanset")
			return stages, p.Main
		default:
			t.Fatalf("unexpected planner %T above the search spanset stage", plan)
		}
	}
}

func TestSearchCapsAndLimitsAfterEvaluation(t *testing.T) {
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			stages, expr := searchStages(t, planSearch(t, query))
			want := []string{"rows", "limit", "spanset"}
			if slices.Contains(structuralQueries, query) {
				want = []string{"structural", "rows", "shard-local", "limit", "spanset"}
			}
			if strings.Join(stages, ",") != strings.Join(want, ",") {
				t.Errorf("stages = %v, want %v", stages, want)
			}
			switch expr.(type) {
			case *IndexLimitPlanner, *searchRowsPlanner, *SearchSpansetPlanner, *shardLocalPlanner:
				t.Errorf("expression planner is %T", expr)
			}
		})
	}
}

func TestSearchSpanCapComesFromSpss(t *testing.T) {
	for _, c := range []struct {
		spss int64
		want string
	}{{7, ", 1, 7)"}, {0, ", 1, 3)"}} {
		for _, query := range searchQueries {
			res := renderSearch(t, planSearch(t, query), testPlannerContext(c.spss))
			if n := strings.Count(res, "arraySlice("); n != 1 {
				t.Errorf("%s: %d span caps, want 1:\n%s", query, n, res)
			}
			if !strings.Contains(res, c.want) {
				t.Errorf("%s: spss %d not applied as %q:\n%s", query, c.spss, c.want, res)
			}
		}
	}
}

func TestSearchSpanCapKeepsEarliestStarts(t *testing.T) {
	const earliestCap = "arraySlice(arraySort(groupUniqArray((result_spans.timestamp_ns, result_spans.span_id))), 1, 3)"
	for _, query := range searchQueries {
		res := renderSearch(t, planSearch(t, query), testPlannerContext(3))
		if !strings.Contains(res, earliestCap) {
			t.Errorf("%s: span cap is not earliest start, ties by span id:\n%s", query, res)
		}
	}
}

func TestSearchRootFilterBeforeSpanCap(t *testing.T) {
	const query = `{nestedSetParent < 0 && .a = 1}`
	_, expr := searchStages(t, planSearch(t, query))
	res := renderPlan(t, &SearchSpansetPlanner{Main: expr}, testPlannerContext(3))
	if !strings.Contains(res, rootColumn) {
		t.Errorf("root filter missing from the spanset stage:\n%s", res)
	}
	res = renderSearch(t, planSearch(t, query), testPlannerContext(3))
	if n := strings.Count(res, "traces.parent_id"); n != 1 {
		t.Errorf("root filter applied %d times, want once:\n%s", n, res)
	}
}

// spanStartBounds lists the span start-time comparisons against the test
// range, which spans midnight of 1970-01-01.
func spanStartBounds(res string) []string {
	re := regexp.MustCompile(`timestamp_ns\) (>=|>|<=|<) \((86390|86410)000000000\)`)
	var out []string
	for _, m := range re.FindAllStringSubmatch(res, -1) {
		out = append(out, m[1]+m[2])
	}
	return out
}

func TestSearchSpanStartIsLeftOpenRightClosed(t *testing.T) {
	check := func(t *testing.T, res string) {
		t.Helper()
		bounds := spanStartBounds(res)
		if len(bounds) == 0 {
			t.Fatalf("no span start bounds:\n%s", res)
		}
		for _, b := range bounds {
			if b != ">86390" && b != "<=86410" {
				t.Errorf("span start bound %s, want >86390 or <=86410:\n%s", b, res)
			}
		}
		if strings.Contains(res, "(date)") &&
			(!strings.Contains(res, "(date) >= ('1970-01-01')") || !strings.Contains(res, "(date) <= ('1970-01-02')")) {
			t.Errorf("date pruning is not toDate(from)..toDate(to):\n%s", res)
		}
	}
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			check(t, renderSearch(t, planSearch(t, query), testPlannerContext(3)))
		})
	}
}

// The Search statement of a non-structural search reads Distributed tables on
// a cluster; the Data statement's reads are checked by
// TestSearchDataUsesPlainInOnClusters.
func TestSearchReadsDistributedTablesOnClusters(t *testing.T) {
	local := regexp.MustCompile(`\btempo_traces(_attrs_gin)?\b[^_]`)
	localJoin := regexp.MustCompile(`[^l] any left JOIN`)
	plainIn := regexp.MustCompile(`[^L] IN \((trace_ids|trace_span_ids|attrless_trace_ids| SELECT trace_id FROM \w+)\)`)
	for _, query := range plainSearchQueries {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx)
			if m := local.FindString(res); m != "" {
				t.Errorf("reads a local table (%q):\n%s", m, res)
			}
			if m := plainIn.FindString(res); m != "" {
				t.Errorf("distributed subquery under a plain IN (%q):\n%s", m, res)
			}
			if localJoin.MatchString(res) {
				t.Errorf("distributed join is not GLOBAL:\n%s", res)
			}
		})
	}
}

// shardView splits a cluster's structural Search statement into the
// statement run on each shard and the coordinator's merge around it.
var shardView = regexp.MustCompile(`(?s)^(.*)FROM cluster\('test_cluster', view\((.*)\)\)( ORDER BY trace_newest .*)$`)

var newestMerge = regexp.MustCompile(`ORDER BY trace_newest desc\s*,\s*trace_id desc\s+LIMIT 5[ )]`)

func TestStructuralSearchRunsWholeOnEachShard(t *testing.T) {
	for _, query := range structuralQueries {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx)
			m := shardView.FindStringSubmatch(res)
			if m == nil {
				t.Fatalf("statement is not run on each shard:\n%s", res)
			}
			shard, merge := m[2], m[1]+m[3]
			for _, bad := range []string{"GLOBAL", "_dist", "cluster("} {
				if strings.Contains(shard, bad) {
					t.Errorf("shard statement holds %q:\n%s", bad, shard)
				}
			}
			if !strings.Contains(shard, "max(newest) as trace_newest") || !strings.HasSuffix(shard, "LIMIT 5") {
				t.Errorf("shard statement does not return its newest 5 traces:\n%s", shard)
			}
			if !newestMerge.MatchString(merge) {
				t.Errorf("coordinator does not keep the newest 5 traces:\n%s", merge)
			}
		})
	}
}

func TestShardStatementSendsEveryShardItsBodyAtOnce(t *testing.T) {
	setting := regexp.MustCompile(`LIMIT 5 SETTINGS prefer_localhost_replica=0\)`)
	for _, query := range structuralQueries {
		ctx := testPlannerContext(3)
		ctx.IsCluster = true
		m := shardView.FindStringSubmatch(renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx))
		if m == nil || !setting.MatchString(m[3]) {
			t.Errorf("%s: shard statement runs the local shard in-process:\n%v", query, m)
		}
	}
}

func TestStructuralSearchOnSingleNodeHasNoShardStatement(t *testing.T) {
	for _, query := range structuralQueries {
		res := renderPlan(t, planSearch(t, query).Ids[0].Statement, testPlannerContext(3))
		if strings.Contains(res, "cluster(") || strings.Contains(res, "trace_newest") {
			t.Errorf("%s: single-node statement is planned per shard:\n%s", query, res)
		}
	}
}

func TestShardStatementSkipsUnavailableShardsLikeTheReadTables(t *testing.T) {
	setting := regexp.MustCompile(`SETTINGS .*skip_unavailable_shards=1$`)
	for _, skip := range []bool{false, true} {
		for _, query := range structuralQueries {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			ctx.SkipUnavailableShards = skip
			res := renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx)
			if setting.MatchString(res) != skip || strings.Count(res, "skip_unavailable_shards") > 1 {
				t.Errorf("skip=%v %s: statement settings do not skip unavailable shards like the read tables:\n%s", skip, query, res)
			}
		}
	}
}
