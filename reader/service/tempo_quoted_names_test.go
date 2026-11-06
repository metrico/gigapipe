package service

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/tempo"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/stretchr/testify/require"
)

const hostilePayload = `OR 1=1 --`

// hostileNames each hold hostilePayload, spelled in a query and decoded.
var hostileNames = []struct{ title, spelled, decoded string }{
	{"backslash quote", `."a\\' OR 1=1 --"`, `a\' OR 1=1 --`},
	{"quote", `."a' OR 1=1 --"`, `a' OR 1=1 --`},
	{"trailing backslash", `."OR 1=1 --\\"`, `OR 1=1 --\`},
	{"NUL", ".\"a\x00' OR 1=1 --\"", "a\x00' OR 1=1 --"},
	{"newline", ".\"a\n' OR 1=1 --\"", "a\n' OR 1=1 --"},
}

func hostileContext() *shared.PlannerContext {
	return &shared.PlannerContext{
		From:                 time.Unix(86390, 0).UTC(),
		To:                   time.Unix(86410, 0).UTC(),
		Limit:                5,
		SpansPerSpanSet:      7,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
		VersionInfo:          map[string]int64{dbversion.CapStructuralQueries: 0},
	}
}

// outsideStringLiterals is query without its single-quoted literals, and those
// literals, under ClickHouse's backslash escapes.
func outsideStringLiterals(t *testing.T, query string) (outside string, literals []string) {
	t.Helper()
	var out, lit strings.Builder
	inside := false
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case !inside && c == '\'':
			inside = true
			lit.Reset()
		case !inside:
			out.WriteByte(c)
		case c == '\\':
			lit.WriteByte(c)
			i++
			if i < len(query) {
				lit.WriteByte(query[i])
			}
		case c == '\'':
			inside = false
			literals = append(literals, lit.String())
		default:
			lit.WriteByte(c)
		}
	}
	require.False(t, inside, "unterminated string literal in:\n%s", query)
	return out.String(), literals
}

// requireNoEscape checks a name of plan's query stays inside string literals,
// and returns those literals.
func requireNoEscape(t *testing.T, plan shared.SQLRequestPlanner) []string {
	t.Helper()
	req, err := plan.Process(hostileContext())
	require.NoError(t, err)
	res, err := req.String(sql.DefaultCtx())
	require.NoError(t, err)
	outside, literals := outsideStringLiterals(t, res)
	require.NotContains(t, outside, "1=1", "name escaped its string literal in:\n%s", res)
	return literals
}

// requireOneLiteral checks the name of plan's query reaches its SQL, only
// inside string literals.
func requireOneLiteral(t *testing.T, plans ...shared.SQLRequestPlanner) {
	t.Helper()
	found := false
	for _, plan := range plans {
		for _, lit := range requireNoEscape(t, plan) {
			found = found || strings.Contains(lit, hostilePayload)
		}
	}
	if found {
		return
	}
	require.Fail(t, "name does not reach the SQL")
}

// statements are the planners of plan's ids statements and of main.
func statements(ids []clickhouse_transpiler.IdsStatement, main shared.SQLRequestPlanner) []shared.SQLRequestPlanner {
	res := []shared.SQLRequestPlanner{main}
	for _, s := range ids {
		res = append(res, s.Statement)
	}
	return res
}

func hostileQuery(t *testing.T, tmpl, name string) *traceql_ir.Query {
	t.Helper()
	q, err := traceql_ir.Parse(strings.ReplaceAll(tmpl, "NAME", name))
	require.NoError(t, err)
	return q
}

func TestQuotedNameStaysOneStringLiteralInSearch(t *testing.T) {
	for _, name := range hostileNames {
		for _, tmpl := range []string{
			`{ NAME = 1 }`,
			`{ NAME = "x" }`,
			`{ NAME =~ "x" }`,
			`{ NAME = nil }`,
			`{ NAME = .b }`,
			`{ NAME + 1 > 2 }`,
			`{ NAME = 1 || .b = 2 }`,
			`{ NAME = 1 } >> { NAME = 2 }`,
			`{ NAME = 1 } && { NAME = 2 }`,
			`{} | select(NAME)`,
			`{} | by(NAME)`,
			`{} | avg(NAME) > 1`,
			`{} | by(NAME) | sum(NAME) > 1`,
		} {
			t.Run(name.title+" "+tmpl, func(t *testing.T) {
				q := hostileQuery(t, tmpl, name.spelled)
				plan, err := clickhouse_transpiler.Plan(q)
				require.NoError(t, err)
				requireOneLiteral(t, statements(plan.Ids, plan.Data)...)
				eval, err := clickhouse_transpiler.PlanEval(q)
				require.NoError(t, err)
				requireNoEscape(t, eval)
			})
		}
	}
}

func TestQuotedNameStaysOneStringLiteralInUnresolvedProbes(t *testing.T) {
	for _, name := range hostileNames {
		for _, tmpl := range []string{
			`{ NAME = 1 } >> { NAME = 2 }`,
			`{ .a = 1 } &>> { NAME = 2 }`,
			`{ NAME = 1 } !~ { .a = 2 }`,
			`{ NAME = 1 } >> { .a = 2 } | rate() by (.b)`,
		} {
			t.Run(name.title+" "+tmpl, func(t *testing.T) {
				plan, err := clickhouse_transpiler.PlanUnresolvedProbes(hostileQuery(t, tmpl, name.spelled))
				require.NoError(t, err)
				probes, err := plan.Process(hostileContext())
				require.NoError(t, err)
				require.NotEmpty(t, probes)
				for _, probe := range probes {
					res, err := probe.Select.String(sql.DefaultCtx())
					require.NoError(t, err)
					outside, literals := outsideStringLiterals(t, res)
					require.NotContains(t, outside, "1=1", "name escaped its string literal in:\n%s", res)
					require.True(t, slices.ContainsFunc(literals, func(lit string) bool {
						return strings.Contains(lit, hostilePayload)
					}), "name does not reach the probe:\n%s", res)
				}
			})
		}
	}
}

func TestQuotedNameStaysOneStringLiteralInMetricsBy(t *testing.T) {
	builds := map[string]func(*tempo.MetricsRequest) (sql.ISelect, []string, error){
		"range":     tempo.BuildMetricsRangeQuery,
		"instant":   tempo.BuildMetricsInstantQuery,
		"exemplars": tempo.BuildGenericExemplarsQuery,
	}
	for _, name := range hostileNames {
		for buildName, build := range builds {
			for _, tmpl := range []string{
				`{} | rate() by (NAME)`,
				`{ NAME = 1 } | rate() by (.b)`,
				`{ NAME = 1 } >> { .b = 2 } | count_over_time() by (span.c, NAME)`,
			} {
				t.Run(name.title+" "+buildName+" "+tmpl, func(t *testing.T) {
					q := hostileQuery(t, tmpl, name.spelled)
					mreq := metricsRequest(q, &model.MetricsQueryRequest{Step: time.Second}, hostileContext())
					plan, err := clickhouse_transpiler.PlanMetrics(q, func(spans *sql.With) (sql.ISelect, error) {
						res, _, err := build(over(mreq, spans))
						return res, err
					})
					require.NoError(t, err)
					requireOneLiteral(t, statements(plan.Ids, plan.Metrics)...)
				})
			}
		}
	}
}

func TestQuotedNameStaysOneStringLiteralInCompare(t *testing.T) {
	for _, name := range hostileNames {
		t.Run(name.title, func(t *testing.T) {
			q := hostileQuery(t, `{ NAME = 1 } | compare({ NAME = 2 })`, name.spelled)
			creq := compareRequest(q, hostileContext())
			plan, err := clickhouse_transpiler.PlanCompare(q, q.Metrics.Compare.Filter,
				time.Unix(0, creq.BaselineFromNS), time.Unix(0, creq.BaselineToNS),
				compareBuild(creq))
			require.NoError(t, err)
			requireOneLiteral(t, statements(plan.Ids, plan.Metrics)...)
		})
	}
}

func TestQuotedNameStaysOneStringLiteralInTagValues(t *testing.T) {
	for _, name := range hostileNames {
		t.Run(name.title, func(t *testing.T) {
			q := hostileQuery(t, `{ NAME = 1 }`, name.spelled)
			tags, err := clickhouse_transpiler.PlanTagsV2(q)
			require.NoError(t, err)
			requireOneLiteral(t, statements(tags.Ids, tags.Data)...)
			values, err := clickhouse_transpiler.PlanValuesV2(q, "x")
			require.NoError(t, err)
			requireOneLiteral(t, statements(values.Ids, values.Data)...)
			values, err = clickhouse_transpiler.PlanValuesV2(nil, name.decoded)
			require.NoError(t, err)
			requireOneLiteral(t, statements(values.Ids, values.Data)...)
		})
	}
}

func TestMetricsByQuotedNameReadsItsKey(t *testing.T) {
	for query, key := range map[string]string{
		`{} | rate() by (.foo)`:       `foo`,
		`{} | rate() by (."5xx")`:     `5xx`,
		`{} | rate() by (span."a b")`: `a b`,
		`{} | rate() by (."foo")`:     `foo`,
	} {
		t.Run(query, func(t *testing.T) {
			q, err := traceql_ir.Parse(query)
			require.NoError(t, err)
			mreq := metricsRequest(q, &model.MetricsQueryRequest{Step: time.Second}, hostileContext())
			mreq.Spans = sql.NewWith(sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewRawObject("s")), "metrics_spans")
			build, _, err := tempo.BuildMetricsRangeQuery(&mreq)
			require.NoError(t, err)
			res, err := build.String(sql.DefaultCtx())
			require.NoError(t, err)
			require.Contains(t, res, "anyIf(val, key = '"+key+"')")
		})
	}
}
