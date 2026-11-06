package promql_transpiler

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// TestTranspileIsDeterministic guards the rand.Int64() -> per-transpile-counter
// fix for pushdown-substitute naming: two independent Parse+TranspileExpressionV2
// passes over the same query must produce a byte-identical residual expression
// and byte-identical SQL for every substitute. Before the fix, each pass drew a
// fresh random name, so this failed intermittently.
func TestTranspileIsDeterministic(t *testing.T) {
	queries := []string{
		`sum by (job) (rate(http_requests_total{job="myjob"}[5m]))`,
		`sum(rate(a[5m])) / sum(rate(b[5m]))`,
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			first := transpileForDeterminism(t, query)
			second := transpileForDeterminism(t, query)
			if first != second {
				t.Fatalf("transpile is not deterministic:\n first: %s\nsecond: %s", first, second)
			}
		})
	}
}

// transpileForDeterminism returns the residual expression followed by each
// substitute's name and SQL, in name order.
func transpileForDeterminism(t *testing.T, query string) string {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	expr, err = TranspileExpressionV2(expr)
	if err != nil {
		t.Fatal(err)
	}
	if len(expr.Substitutes) == 0 {
		t.Fatalf("%s: no substitutes, so the test would not exercise substitute naming", query)
	}

	from := time.Unix(1700000000, 0)
	names := make([]string, 0, len(expr.Substitutes))
	for name := range expr.Substitutes {
		names = append(names, name)
	}
	sort.Strings(names)

	var out strings.Builder
	out.WriteString(expr.Expr.String())
	for _, name := range names {
		req, err := expr.Substitutes[name].Request.Process(&shared.PlannerContext{
			From:                    from,
			To:                      from.Add(time.Hour),
			Step:                    15 * time.Second,
			TimeSeriesGinTableName:  "time_series_gin",
			SamplesTableName:        "samples_v3",
			TimeSeriesTableName:     "time_series",
			TimeSeriesDistTableName: "time_series",
			Metrics15sTableName:     "metrics_15s",
		})
		if err != nil {
			t.Fatal(err)
		}
		str, err := req.String(sql.DefaultCtx())
		if err != nil {
			t.Fatal(err)
		}
		out.WriteString("\n" + name + ": " + str)
	}
	return out.String()
}
