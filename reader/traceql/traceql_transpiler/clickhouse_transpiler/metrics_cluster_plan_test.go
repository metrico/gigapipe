package clickhouse_transpiler_test

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/tempo"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	ct "github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// distFrom matches a FROM naming a Distributed table.
var distFrom = regexp.MustCompile(`(?i)\bFROM\s+[^\s()]*_dist\b`)

// sqlToken splits SQL into string literals, quoted names, parentheses and words.
var sqlToken = regexp.MustCompile("'(?:[^'\\\\]|\\\\.|'')*'|`[^`]*`|[()]|[^\\s()'`]+")

var joinModifiers = map[string]bool{
	"GLOBAL": true, "LOCAL": true, "ANY": true, "ALL": true, "ASOF": true, "SEMI": true, "ANTI": true,
	"LEFT": true, "RIGHT": true, "INNER": true, "FULL": true, "OUTER": true, "CROSS": true, "ARRAY": true,
}

// plainDistJoins returns every join in a FROM clause over a Distributed table
// that is neither GLOBAL nor ARRAY JOIN.
func plainDistJoins(query string) []string {
	var plain []string
	for _, from := range distFrom.FindAllStringIndex(query, -1) {
		var modifiers []string
		depth := 0
	clause:
		for _, token := range sqlToken.FindAllString(query[from[1]:], -1) {
			word := strings.ToUpper(token)
			switch {
			case token == "(":
				depth++
			case token == ")":
				depth--
				if depth < 0 {
					break clause
				}
			case depth > 0:
			case word == "SELECT":
				break clause
			case word == "JOIN":
				if !slices.Contains(modifiers, "GLOBAL") && !slices.Contains(modifiers, "ARRAY") {
					plain = append(plain, strings.TrimSpace(strings.Join(modifiers, " ")+" JOIN"))
				}
			}
			if depth == 0 && joinModifiers[word] {
				modifiers = append(modifiers, word)
			} else {
				modifiers = nil
			}
		}
	}
	return plain
}

func TestPlainDistJoinsRecognisesShapes(t *testing.T) {
	for _, tc := range []struct {
		query string
		plain bool
	}{
		{"SELECT 1 FROM `d`.tempo_traces_dist as main LEFT JOIN by_attrs ON 1", true},
		{"SELECT 1 FROM tempo_traces_dist AS main INNER JOIN filtered ON 1", true},
		{"SELECT 1 FROM tempo_traces_dist FINAL JOIN (SELECT 1 FROM tempo_traces_attrs_gin_dist) ON 1", true},
		{"SELECT 1 FROM tempo_traces_dist as main GLOBAL LEFT JOIN by_attrs ON 1", false},
		{"SELECT 1 FROM metrics_spans as main LEFT JOIN by_attrs ON 1", false},
		{"SELECT 1 FROM tempo_traces_dist as t WHERE (a) = (1)", false},
		{"SELECT 1 FROM tempo_traces_dist ARRAY JOIN x", false},
		{"SELECT 1 FROM tempo_traces_dist as main GLOBAL INNER JOIN a ON 1 LEFT JOIN b ON 1", true},
		{"SELECT 1 FROM tempo_traces_dist as main GLOBAL INNER JOIN (SELECT 1 FROM t LEFT JOIN u ON 1) a ON 1 ANY LEFT JOIN b ON x = ')'", true},
		{"SELECT 1 FROM tempo_traces_dist as main GLOBAL INNER JOIN a ON 1 GLOBAL LEFT JOIN b ON 1", false},
		{"SELECT 1 FROM tempo_traces_dist as main GLOBAL JOIN (SELECT 1 FROM t LEFT JOIN u ON 1) a ON 1", false},
		{"SELECT 1 FROM tempo_traces_dist as main GLOBAL JOIN (SELECT 1 FROM v_dist LEFT JOIN u ON 1) a ON 1", true},
		{"SELECT 1 FROM (SELECT 1 FROM tempo_traces_dist) as main LEFT JOIN b ON 1", false},
		{"SELECT 1 FROM tempo_traces_dist WHERE a UNION ALL SELECT 1 FROM t LEFT JOIN b ON 1", false},
	} {
		if got := len(plainDistJoins(tc.query)) > 0; got != tc.plain {
			t.Errorf("plainDistJoins(%q) found a join = %v, want %v", tc.query, got, tc.plain)
		}
	}
}

// renderMetricsPlanners calls check with the SQL of every metrics planner for
// every search query, wired as the tempo metrics service wires them.
func renderMetricsPlanners(t *testing.T, cluster bool, check func(t *testing.T, rendered string)) {
	t.Helper()
	ctx := ct.TestPlannerContext(3)
	ctx.IsCluster = cluster
	attrs := ctx.TracesAttrsTable
	if cluster {
		attrs = ctx.TracesAttrsDistTable
	}
	const fromNS, toNS, baselineFromNS, baselineToNS = 86390e9, 86410e9, 86000e9, 86500e9
	request := func(spans *sql.With, by ...string) *tempo.MetricsRequest {
		return &tempo.MetricsRequest{
			Spans: spans, Fn: "rate", Attr: "duration", By: by,
			FromNS: fromNS, ToNS: toNS, StepNS: 1e9, AttrsTable: attrs,
		}
	}
	builds := map[string]ct.MetricsBuild{
		"range": func(spans *sql.With) (sql.ISelect, error) {
			q, _, err := tempo.BuildMetricsRangeQuery(request(spans, "span.env", "name"))
			return q, err
		},
		"instant": func(spans *sql.With) (sql.ISelect, error) {
			q, _, err := tempo.BuildMetricsInstantQuery(request(spans, "span.env"))
			return q, err
		},
		"histogram": func(spans *sql.With) (sql.ISelect, error) {
			return tempo.BuildHistogramRangeQuery(request(spans))
		},
		"histogram exemplars": func(spans *sql.With) (sql.ISelect, error) {
			return tempo.BuildHistogramExemplarsQuery(request(spans))
		},
		"generic exemplars": func(spans *sql.With) (sql.ISelect, error) {
			q, _, err := tempo.BuildGenericExemplarsQuery(request(spans, "span.env"))
			return q, err
		},
	}
	builds["nested-set argument"] = func(spans *sql.With) (sql.ISelect, error) {
		req := request(spans, "nestedSetLeft")
		req.Fn, req.Attr = "avg_over_time", "nestedSetRight"
		q, _, err := tempo.BuildMetricsRangeQuery(req)
		return q, err
	}
	selectors := append(slices.Clone(ct.SearchQueries),
		`{nestedSetParent < 0}`,
		`{nestedSetParent < 0 && .a = 1}`,
		`{.a = 1} | select(.x)`,
		`{.a = 1} | avg_over_time(nestedSetRight)`)
	for _, query := range selectors {
		for name, build := range builds {
			t.Run(name+"/"+query, func(t *testing.T) {
				text := query
				if !strings.Contains(query, "_over_time") {
					text += " | rate()"
				}
				q, err := traceql_ir.Parse(text)
				if err != nil {
					t.Fatal(err)
				}
				planner, err := ct.PlanMetrics(q, build)
				if err != nil {
					t.Fatal(err)
				}
				rendered := ct.RenderPlan(t, planner.Metrics, ctx)
				for _, ids := range planner.Ids {
					rendered += "\n" + ct.RenderPlan(t, ids.Statement, ctx)
				}
				check(t, rendered)
			})
		}
		if strings.Contains(query, "_over_time") {
			continue
		}
		for _, selection := range []string{"{.c = 3}", "{}", "{nestedSetParent < 0}"} {
			t.Run("compare "+selection+"/"+query, func(t *testing.T) {
				q, err := traceql_ir.Parse(query + " | compare(" + selection + ")")
				if err != nil {
					t.Fatal(err)
				}
				planner, err := ct.PlanCompare(q, q.Metrics.Compare.Filter, time.Unix(86000, 0), time.Unix(86500, 0),
					ct.CompareBuild{
						Counts: func(baseline, selected, traceIds *sql.With, attrs string) (sql.ISelect, error) {
							return tempo.BuildCompareCounts(&tempo.CompareRequest{
								Baseline: baseline, Selection: selected, BaselineTraceIds: traceIds,
								FromNS: fromNS, ToNS: toNS,
								BaselineFromNS: baselineFromNS, BaselineToNS: baselineToNS,
								AttrsTable: attrs,
							})
						},
						Result: func(counts *sql.With) (sql.ISelect, error) {
							return tempo.BuildCompareResult(counts, 0, 1), nil
						},
					})
				if err != nil {
					t.Fatal(err)
				}
				rendered := ct.RenderPlan(t, planner.Metrics, ctx)
				for _, ids := range planner.Ids {
					rendered += "\n" + ct.RenderPlan(t, ids.Statement, ctx)
				}
				check(t, rendered)
			})
		}
	}
}

// On a cluster, no plain JOIN hangs directly off a Distributed table.
func TestMetricsPlannersHaveNoPlainDistributedJoin(t *testing.T) {
	renderMetricsPlanners(t, true, func(t *testing.T, rendered string) {
		if !strings.Contains(rendered, "tempo_traces_dist") && !strings.Contains(rendered, "cluster(") {
			t.Fatalf("cluster plan reads no Distributed table:\n%s", rendered)
		}
		if plain := plainDistJoins(rendered); len(plain) > 0 {
			t.Fatalf("plain joins off a Distributed table %q:\n%s", plain, rendered)
		}
	})
}

// On a single node, metrics plans have no GLOBAL join.
func TestMetricsPlannersHaveNoGlobalOnSingleNode(t *testing.T) {
	renderMetricsPlanners(t, false, func(t *testing.T, rendered string) {
		if strings.Contains(strings.ToUpper(rendered), "GLOBAL") {
			t.Fatalf("single-node plan uses GLOBAL:\n%s", rendered)
		}
	})
}
