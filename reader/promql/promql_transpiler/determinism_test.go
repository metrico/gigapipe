package promql_transpiler

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
)

// TestTranspileIsDeterministic guards the rand.Int63() -> per-transpile-counter
// fix for pushdown-substitute naming: two independent Parse+TranspileExpressionV2
// passes over the same query must assign identical substitute names, so the
// residual expression handed to the embedded engine is byte-identical across
// runs. Before the fix, each pass drew a fresh random name, so this failed
// intermittently.
func TestTranspileIsDeterministic(t *testing.T) {
	const query = `sum by (job) (rate(http_requests_total{job="myjob"}[5m]))`

	transpile := func() string {
		t.Helper()
		expr, err := promql_parser.Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		expr, err = TranspileExpressionV2(expr)
		if err != nil {
			t.Fatal(err)
		}
		return expr.Expr.String()
	}

	first := transpile()
	second := transpile()
	if first != second {
		t.Fatalf("transpile is not deterministic:\n first: %s\nsecond: %s", first, second)
	}
}
