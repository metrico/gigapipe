package clickhouse_transpiler

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// A slice inside testPlannerContext's range, and the bounds a read over it renders.
var (
	sliceFrom, sliceTo = time.Unix(86400, 0).UTC(), time.Unix(86410, 0).UTC()
	sliceFromBound     = "timestamp_ns) > (86400000000000)"
	sliceToBound       = "timestamp_ns) <= (86410000000000)"
)

// sliceContext is a request over the slice's candidates of query.
func sliceContext(t *testing.T, query string, cluster bool) (*SearchPlan, *shared.PlannerContext) {
	t.Helper()
	plan := planSearch(t, query)
	ctx := testPlannerContext(3)
	ctx.IsCluster = cluster
	ctx.TracesCandidates = plan.Candidates(sliceFrom, sliceTo)
	return plan, ctx
}

// A lone {} at limit 0 picks every trace.
func TestLoneSelectorAtLimitZeroHasNoLimit(t *testing.T) {
	ctx := testPlannerContext(3)
	ctx.Limit = 0
	for _, query := range []string{`{}`, `{} | select(.x)`} {
		s := renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx)
		if strings.Contains(s, "LIMIT 0") {
			t.Errorf("%s: %s", query, s)
		}
	}
}

// Candidates are the union of the query's leaf selectors over the slice, each
// from its own source; a selector or pipeline reading every span reads the
// spans table alone.
func TestCandidatesReadEveryLeafFromItsSource(t *testing.T) {
	const attrs, spans = "tempo_traces_attrs_gin", "tempo_traces as traces"
	for _, tc := range []struct {
		query    string
		sources  []string
		excluded []string
		reads    int
	}{
		{`{.a = 1}`, []string{attrs}, []string{spans}, 1},
		{`{.a = 1} >> {.b = 2}`, []string{attrs}, []string{spans}, 2},
		{`{.a = 1} !>> {.b = 2}`, []string{attrs}, []string{spans}, 2},
		{`{.a = 1} && {.b = 2}`, []string{attrs}, []string{spans}, 2},
		{`{.a = 1} || {.b = 2}`, []string{attrs}, []string{spans}, 2},
		{`{.a = 1} | {.b = 2}`, []string{attrs}, []string{spans}, 2},
		{`({.a = 1} | count() > 1) >> {.b = 2}`, []string{attrs}, []string{spans}, 2},
		{`{.a = 1} | count() > 2`, []string{attrs}, []string{spans}, 1},
		{`{.a = 1} | select(.x)`, []string{attrs}, []string{spans}, 1},
		{`{.a = .b}`, []string{attrs}, nil, 1},
		{`{}`, []string{spans}, []string{attrs}, 1},
		{`{} | select(.x)`, []string{spans}, []string{attrs}, 1},
		{`select(.x)`, []string{spans}, []string{attrs}, 1},
		{`count() > 2`, []string{spans}, []string{attrs}, 1},
		{`by(.a)`, []string{spans}, []string{attrs}, 1},
		{`by(.a) | count() > 2`, []string{spans}, []string{attrs}, 1},
		{`{} >> {.b = 2}`, []string{spans}, []string{attrs}, 1},
		{`{.a = 1} >> ({} | count() > 2)`, []string{spans}, []string{attrs}, 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			_, ctx := sliceContext(t, tc.query, false)
			s := renderPlan(t, ctx.TracesCandidates, ctx)
			for _, want := range tc.sources {
				if !strings.Contains(s, want) {
					t.Errorf("no %s:\n%s", want, s)
				}
			}
			for _, not := range tc.excluded {
				if strings.Contains(s, not) {
					t.Errorf("reads %s:\n%s", not, s)
				}
			}
			if got := strings.Count(s, "UNION DISTINCT") + 1; got != tc.reads {
				t.Errorf("%d reads, want %d:\n%s", got, tc.reads, s)
			}
			if !strings.Contains(s, sliceFromBound) || !strings.Contains(s, sliceToBound) {
				t.Errorf("not over the slice:\n%s", s)
			}
			if strings.Contains(s, "86390000000000") {
				t.Errorf("over the request:\n%s", s)
			}
		})
	}
}

// The extent read covers the candidates' spans over the request's range, one row
// per shard, from Distributed tables on a cluster, with no statement settings.
func TestExtentStatement(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cluster bool
		table   string
		in      string
	}{
		{"single node", false, "FROM tempo_traces as traces", " IN "},
		{"cluster", true, "FROM tempo_traces_dist as traces", " GLOBAL IN "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := sliceContext(t, `{.a = 1} >> {.b = 2}`, tc.cluster)
			s := renderPlan(t, &ExtentPlanner{}, ctx)
			for _, want := range []string{tc.table, tc.in, "shardNum()", "GROUP BY shard",
				"min(timestamp_ns)", "max(timestamp_ns)", "count()"} {
				if !strings.Contains(s, want) {
					t.Errorf("no %q:\n%s", want, s)
				}
			}
			if !strings.Contains(s, "WHERE ((timestamp_ns) > (86390000000000))") {
				t.Errorf("not over the request:\n%s", s)
			}
			for _, not := range []string{"SETTINGS", "cluster(", "cityHash64"} {
				if strings.Contains(s, not) {
					t.Errorf("has %q:\n%s", not, s)
				}
			}
		})
	}
}

// candidatesIn is the condition an operand read keeps the candidates by.
const candidatesIn = "IN ( SELECT trace_id FROM (SELECT trace_id FROM ("

// Every operand read of the evaluation keeps the candidates: Distributed tables and
// GLOBAL IN outside shard-local plans, local tables and plain IN inside them.
func TestCandidatesOnEveryOperandRead(t *testing.T) {
	for _, tc := range []struct {
		query    string
		cluster  bool
		operands int
		cond     string
	}{
		{`{.a = 1} && {.b = 2}`, false, 2, candidatesIn},
		{`{.a = 1} && {.b = 2}`, true, 2, "GLOBAL " + candidatesIn},
		{`{.a = 1} | count() > 2`, false, 1, candidatesIn},
		{`{.a = .b}`, true, 1, "GLOBAL " + candidatesIn},
		{`{.a = 1} >> {.b = 2}`, false, 2, candidatesIn},
		{`{.a = 1} !>> {.b = 2}`, false, 2, candidatesIn},
		{`{.a = 1} >> {.b = 2}`, true, 2, candidatesIn},
		{`{} >> {.b = 2}`, true, 2, candidatesIn},
	} {
		t.Run(tc.query+" cluster "+strconv.FormatBool(tc.cluster), func(t *testing.T) {
			plan, ctx := sliceContext(t, tc.query, tc.cluster)
			s := renderPlan(t, plan.Ids[0].Statement, ctx)
			if got := strings.Count(s, tc.cond); got < tc.operands {
				t.Errorf("%d candidate conditions, want at least %d:\n%s", got, tc.operands, s)
			}
			if strings.Contains(s, "cityHash64") {
				t.Errorf("hash condition:\n%s", s)
			}
			if tc.cluster && strings.Contains(tc.query, ">>") {
				if strings.Contains(s, "GLOBAL IN") || strings.Contains(s, "_dist") {
					t.Errorf("shard-local statement reads over the cluster:\n%s", s)
				}
			}
		})
	}
}

// A lone {} picks its traces among the candidates, by their newest span in the
// context's range.
func TestLoneSelectorPicksAmongTheCandidates(t *testing.T) {
	for _, query := range []string{`{}`, `({})`, `{} | select(.x)`} {
		t.Run(query, func(t *testing.T) {
			plan, ctx := sliceContext(t, query, false)
			req, err := plan.Ids[0].Statement.Process(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s, err := req.String(sql.DefaultCtx())
			if err != nil {
				t.Fatal(err)
			}
			picked, _, found := strings.Cut(s, "GROUP BY trace_id ORDER BY max(timestamp_ns)")
			if !found || !strings.Contains(picked, "attrless_trace_ids as (") || !strings.Contains(picked, candidatesIn) {
				t.Errorf("no candidates in attrless_trace_ids:\n%s", s)
			}
			if strings.Contains(s, "cityHash64") {
				t.Errorf("hash condition:\n%s", s)
			}
		})
	}
}

// Without candidates a statement reads the whole range.
func TestNoCandidatesReadTheWholeRange(t *testing.T) {
	plan := planSearch(t, `{.a = 1} >> {.b = 2}`)
	s := renderPlan(t, plan.Ids[0].Statement, testPlannerContext(3))
	if strings.Contains(s, candidatesIn) {
		t.Errorf("candidates in a whole-range read:\n%s", s)
	}
}

// Check rejects what a statement would, before any read.
func TestSearchPlanCheckRejectsAnUnsupportedServer(t *testing.T) {
	ctx := testPlannerContext(3)
	ctx.VersionInfo = nil
	if err := planSearch(t, `{.a = 1} >> {.b = 2}`).Check(ctx); err != errStructuralVersion {
		t.Errorf("structural: %v", err)
	}
	if err := planSearch(t, `{.a = 1}`).Check(ctx); err != nil {
		t.Errorf("plain: %v", err)
	}
}

// Metrics take no candidates, and autocomplete reads hash portions.
func TestOnlySearchReadsCandidates(t *testing.T) {
	const query = `{.a = 1} >> {.b = 2}`
	ctx := testPlannerContext(3)
	if s := renderMetrics(t, planMetrics(t, query), ctx); strings.Contains(s, candidatesIn) || strings.Contains(s, "cityHash64") {
		t.Errorf("metrics:\n%s", s)
	}
	statement := idsStatements(t, query)["tags"]
	ctx.RandomFilter = shared.RandomFilter{Max: 3, I: 1}
	if s := renderPlan(t, statement, ctx); !strings.Contains(s, "cityHash64") || strings.Contains(s, candidatesIn) {
		t.Errorf("autocomplete:\n%s", s)
	}
}

// A walk's probe reads the slice's candidates, shard-locally on a cluster.
func TestUnresolvedProbeReadsTheCandidates(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		_, ctx := sliceContext(t, `{.a = 1} >> {.b = 2}`, cluster)
		_, stmts := planProbes(t, `{.a = 1} >> {.b = 2}`, ctx)
		for _, s := range stmts {
			if !strings.Contains(s, candidatesIn) {
				t.Errorf("cluster %v: no candidates:\n%s", cluster, s)
			}
			if strings.Contains(s, "GLOBAL IN") {
				t.Errorf("cluster %v: reads over the cluster:\n%s", cluster, s)
			}
		}
	}
}

// A slice's date bounds are one UTC day wider than its range each way, in any
// reader time zone.
func TestSliceDateBoundsDoNotDependOnTheReaderZone(t *testing.T) {
	bound := regexp.MustCompile(`date\) (>=|<=) \('([0-9-]+)'\)`)
	from := time.Date(2026, 11, 6, 17, 0, 0, 0, time.UTC)
	for _, offset := range []int{0, 8, -8, 14, -12} {
		zone := time.FixedZone("reader", offset*3600)
		for _, query := range []string{`{.a = 1} >> {.b = 2}`, `{.a = .b}`, `{.a = 1} | select(.b)`} {
			ctx := testPlannerContext(3)
			ctx.From, ctx.To = from.In(zone), from.Add(time.Minute).In(zone)
			plan := planSearch(t, query)
			ctx.TracesCandidates = plan.Candidates(ctx.From, ctx.To)
			res := renderSearch(t, plan, ctx)
			bounds := bound.FindAllStringSubmatch(res, -1)
			if len(bounds) == 0 {
				t.Fatalf("offset %d %s: no date bounds:\n%s", offset, query, res)
			}
			for _, b := range bounds {
				want := map[string]string{">=": "2026-11-05", "<=": "2026-11-07"}[b[1]]
				if b[2] != want {
					t.Errorf("offset %d %s: date %s %s, want %s", offset, query, b[1], b[2], want)
				}
			}
		}
	}
}
