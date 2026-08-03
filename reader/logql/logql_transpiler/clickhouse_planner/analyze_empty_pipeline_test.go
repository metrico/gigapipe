package clickhouse_planner

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_parser"
)

// Grafana Logs Drilldown emits `sum(count_over_time({...} | json | logfmt [1m]))`
// (its LOG_STREAM_SELECTOR_EXPR has no `| drop __error__`). The planner cancels the
// json+logfmt pair, leaving an empty-but-non-nil Pipelines slice; the old
// `Pipelines != nil` guard then indexed [-1] and panicked.
func TestAnalyzeMetrics15sShortcutEmptyPipelines(t *testing.T) {
	for _, q := range []string{
		`sum(count_over_time({service_name=~".+"} | json | logfmt [1m]))`,
		`sum(rate({service_name=~".+"} | json | logfmt [5m]))`,
	} {
		script, err := logql_parser.Parse(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		lra := findFirst[logql_parser.LRAOrUnwrap](script)
		if lra == nil {
			t.Fatalf("no LRAOrUnwrap in %q", q)
		}
		// Emulate cancelJsonAndLogFmt: pair stripped, slice non-nil but empty.
		lra.StrSel.Pipelines = []logql_parser.StrSelectorPipeline{}
		if !AnalyzeMetrics15sShortcut(script) {
			t.Errorf("%q: want shortcut eligible for empty pipeline", q)
		}
	}
}
