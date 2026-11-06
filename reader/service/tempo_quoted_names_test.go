package service

import (
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

// hostileName decodes to `.a\' OR 1=1 --`: its quote follows a backslash.
const (
	hostileName    = `."a\\' OR 1=1 --"`
	hostilePayload = `OR 1=1 --`
)

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

// requireOneLiteral checks the name of plan's query reaches its SQL only inside
// string literals.
func requireOneLiteral(t *testing.T, plan shared.SQLRequestPlanner) {
	t.Helper()
	req, err := plan.Process(hostileContext())
	require.NoError(t, err)
	res, err := req.String(sql.DefaultCtx())
	require.NoError(t, err)
	outside, literals := outsideStringLiterals(t, res)
	require.NotContains(t, outside, "1=1", "name escaped its string literal in:\n%s", res)
	for _, lit := range literals {
		if strings.Contains(lit, hostilePayload) {
			return
		}
	}
	require.Fail(t, "name does not reach the SQL", res)
}

func hostileQuery(t *testing.T, tmpl string) *traceql_ir.Query {
	t.Helper()
	q, err := traceql_ir.Parse(strings.ReplaceAll(tmpl, "NAME", hostileName))
	require.NoError(t, err)
	return q
}

func TestQuotedNameStaysOneStringLiteralInSearch(t *testing.T) {
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
		t.Run(tmpl, func(t *testing.T) {
			plan, err := clickhouse_transpiler.Plan(hostileQuery(t, tmpl))
			require.NoError(t, err)
			requireOneLiteral(t, plan)
		})
	}
}

func TestQuotedNameStaysOneStringLiteralInMetricsBy(t *testing.T) {
	builds := map[string]func(*tempo.MetricsRequest) (sql.ISelect, []string, error){
		"range":     tempo.BuildMetricsRangeQuery,
		"instant":   tempo.BuildMetricsInstantQuery,
		"exemplars": tempo.BuildGenericExemplarsQuery,
	}
	for name, build := range builds {
		for _, tmpl := range []string{
			`{} | rate() by (NAME)`,
			`{ NAME = 1 } | rate() by (.b)`,
			`{ NAME = 1 } >> { .b = 2 } | count_over_time() by (span.c, NAME)`,
		} {
			t.Run(name+" "+tmpl, func(t *testing.T) {
				q := hostileQuery(t, tmpl)
				mreq := metricsRequest(q.Metrics, &model.MetricsQueryRequest{Step: time.Second}, hostileContext())
				plan, err := clickhouse_transpiler.PlanMetrics(q, func(spans *sql.With) (sql.ISelect, error) {
					res, _, err := build(over(mreq, spans))
					return res, err
				})
				require.NoError(t, err)
				requireOneLiteral(t, plan)
			})
		}
	}
}

func TestQuotedNameStaysOneStringLiteralInCompare(t *testing.T) {
	q := hostileQuery(t, `{ NAME = 1 } | compare({ NAME = 2 })`)
	creq := compareRequest(q.Metrics, hostileContext())
	plan, err := clickhouse_transpiler.PlanCompare(q, q.Metrics.Compare.Filter,
		time.Unix(0, creq.BaselineFromNS), time.Unix(0, creq.BaselineToNS),
		func(baseline, selection *sql.With) (sql.ISelect, error) {
			creq.Baseline, creq.Selection = baseline, selection
			return tempo.BuildCompareQuery(&creq)
		})
	require.NoError(t, err)
	requireOneLiteral(t, plan)
}

func TestQuotedNameStaysOneStringLiteralInTagValues(t *testing.T) {
	q := hostileQuery(t, `{ NAME = 1 }`)
	tags, err := clickhouse_transpiler.PlanTagsV2(q)
	require.NoError(t, err)
	requireOneLiteral(t, tags)
	values, err := clickhouse_transpiler.PlanValuesV2(q, "x")
	require.NoError(t, err)
	requireOneLiteral(t, values)
	values, err = clickhouse_transpiler.PlanValuesV2(nil, `a\' OR 1=1 --`)
	require.NoError(t, err)
	requireOneLiteral(t, values)
}
