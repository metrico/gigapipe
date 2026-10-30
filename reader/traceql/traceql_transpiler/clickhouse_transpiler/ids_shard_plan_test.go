package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// idsStatements are the ids statements of metrics, compare() and
// autocomplete over query, by name; compare()'s selection statement aside.
func idsStatements(t *testing.T, query string) map[string]shared.SQLRequestPlanner {
	t.Helper()
	q, err := traceql_ir.Parse(query + " | compare({.c = 3})")
	if err != nil {
		t.Fatal(err)
	}
	cmp, err := PlanCompare(q, q.Metrics.Compare.Filter, time.Unix(86000, 0), time.Unix(86500, 0), compareSpans)
	if err != nil {
		t.Fatal(err)
	}
	q, err = traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := PlanTagsV2(q)
	if err != nil {
		t.Fatal(err)
	}
	values, err := PlanValuesV2(q, "a")
	if err != nil {
		t.Fatal(err)
	}
	return map[string]shared.SQLRequestPlanner{
		"metrics": planMetrics(t, query).Ids[0].Statement,
		"compare": cmp.Ids[0].Statement,
		"tags":    tags.Ids[0].Statement,
		"values":  values.Ids[0].Statement,
	}
}

// idsShardView splits a cluster's ids statement into the statement run on
// each shard and the coordinator's select around it.
var idsShardView = regexp.MustCompile(`(?s)^(.*)FROM cluster\('test_cluster', view\((.*)\)\)(.*)$`)

func TestStructuralIdsStatementsRunWholeOnEachShard(t *testing.T) {
	coordinator := regexp.MustCompile(`^\s*SELECT trace_id,\s*span_ids\s*$`)
	settings := regexp.MustCompile(`^ SETTINGS enable_analyzer=1, prefer_localhost_replica=0$`)
	for _, query := range structuralQueries {
		for name, statement := range idsStatements(t, query) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, statement, ctx)
			m := idsShardView.FindStringSubmatch(res)
			if m == nil {
				t.Errorf("%s %s: ids statement is not run on each shard:\n%s", name, query, res)
				continue
			}
			for _, bad := range []string{"GLOBAL", "_dist", "cluster(", "LIMIT"} {
				if strings.Contains(m[2], bad) {
					t.Errorf("%s %s: shard statement holds %q:\n%s", name, query, bad, m[2])
				}
			}
			if !coordinator.MatchString(m[1]) || !settings.MatchString(m[3]) {
				t.Errorf("%s %s: coordinator does more than pass on each shard's (trace_id, span_ids):\n%s", name, query, res)
			}
		}
	}
}

func TestNonStructuralIdsStatementsReadDistributedTables(t *testing.T) {
	for _, query := range []string{`{.a = 1}`, `{.a = 1} && {.b = 2}`} {
		for name, statement := range idsStatements(t, query) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, statement, ctx)
			if strings.Contains(res, "cluster(") || !strings.Contains(res, "_dist") {
				t.Errorf("%s %s: ids statement is not distributed:\n%s", name, query, res)
			}
		}
	}
}

func TestStructuralIdsStatementsOnSingleNodeHaveNoShardStatement(t *testing.T) {
	for _, query := range structuralQueries {
		for name, statement := range idsStatements(t, query) {
			res := renderPlan(t, statement, testPlannerContext(3))
			if strings.Contains(res, "cluster(") || strings.Contains(res, "prefer_localhost_replica") {
				t.Errorf("%s %s: single-node statement is planned per shard:\n%s", name, query, res)
			}
		}
	}
}
