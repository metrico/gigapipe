package logql_transpiler

import (
	"testing"

	log_parser "github.com/metrico/qryn/v5/reader/logql/logql_parser"
)

// Grafana Logs Drilldown always emits a chained `| json | logfmt` pipeline.
// cancelJsonAndLogFmt strips that pair from the AST, which can turn an
// internally-planned script into a fully ClickHouse-planned one. The plan mode
// must therefore be re-derived after the mutation, otherwise breakScript
// returns a nil internal script and planning dereferences it.
func TestPlanCancelledJsonLogfmt(t *testing.T) {
	for _, query := range []string{
		`{service_name="tix-webhook"} | json | logfmt | drop __error__, __error_details__`,
		`sum(count_over_time({service_name="tix-webhook"} | json | logfmt | drop __error__, __error_details__ [5m]))`,
		`{service_name="tix-webhook"} | json | logfmt`,
	} {
		script, err := log_parser.Parse(query)
		if err != nil {
			t.Fatalf("Parse(%q): %v", query, err)
		}
		chain, err := Plan(script)
		if err != nil {
			t.Fatalf("Plan(%q): %v", query, err)
		}
		if len(chain) == 0 {
			t.Fatalf("Plan(%q): empty processor chain", query)
		}
	}
}
