package clickhouse_transpiler

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
)

var dataQueries = append(append([]string{}, searchQueries...),
	`{.a = 1} | select(.b, span.c)`,
	`{.a = 1} | select(nestedSetLeft, nestedSetParent)`,
	`{.a = 1} | select(.b, nestedSetRight)`,
	`{.a = 1} | by(.b)`,
)

func TestSearchDataReadsOnlySearchResult(t *testing.T) {
	for _, query := range dataQueries {
		for _, cluster := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cluster=%t", query, cluster), func(t *testing.T) {
				ctx := testPlannerContext(3)
				ctx.IsCluster = cluster
				res := renderPlan(t, planSearch(t, query).Data, ctx)
				if !strings.Contains(res, "FROM "+SearchResultTable) {
					t.Errorf("does not read %s:\n%s", SearchResultTable, res)
				}
				for _, banned := range []string{"index_grouped", "spansets", "result_spans"} {
					if strings.Contains(res, banned) {
						t.Errorf("reads %s:\n%s", banned, res)
					}
				}
			})
		}
	}
}

func TestSearchDataUsesPlainInOnClusters(t *testing.T) {
	plainInRe := regexp.MustCompile(`[^L] IN \((trace_ids|trace_span_ids)\)`)
	for _, query := range dataQueries {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, planSearch(t, query).Data, ctx)
			if strings.Contains(res, "GLOBAL IN") {
				t.Errorf("GLOBAL IN in the data statement:\n%s", res)
			}
			if !plainInRe.MatchString(res) {
				t.Errorf("no plain IN over the search result:\n%s", res)
			}
			if !strings.Contains(res, "tempo_traces_dist") {
				t.Errorf("does not read the distributed spans table:\n%s", res)
			}
		})
	}
}

func TestSearchStatementDoesNotReadSearchResult(t *testing.T) {
	for _, query := range dataQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planSearch(t, query).Ids[0].Statement, testPlannerContext(3))
			if strings.Contains(res, SearchResultTable) {
				t.Errorf("search statement reads %s:\n%s", SearchResultTable, res)
			}
		})
	}
}

func TestTracesDataPluginReadsSearchResult(t *testing.T) {
	var got shared.SQLRequestPlanner
	plugins.RegisterTracesDataPlugin(func(main shared.SQLRequestPlanner) shared.SQLRequestPlanner {
		got = main
		return &TracesDataPlanner{Main: main}
	})
	t.Cleanup(func() { plugins.RegisterTracesDataPlugin(nil) })
	planSearch(t, `{.a = 1}`)
	if got == nil {
		t.Fatal("plugin not called")
	}
	want := "SELECT trace_id, span_id FROM " + SearchResultTable
	if res := strings.TrimSpace(renderPlan(t, got, testPlannerContext(3))); res != want {
		t.Errorf("plugin main = %q, want %q", res, want)
	}
}
