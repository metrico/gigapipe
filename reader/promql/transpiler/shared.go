package transpiler

import (
	logql_transpiler "github.com/metrico/qryn/reader/logql/logql_transpiler_v2/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/promql/parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
)

func fingerprintsQuery(ctx *shared.PlannerContext, matchers ...*labels.Matcher) (sql.ISelect, error) {
	var (
		labelNames []string
		ops        []string
		values     []string
	)
	for _, _matcher := range matchers {
		matcher := parser.LabelMatcher{Node: _matcher}
		labelNames = append(labelNames, matcher.GetLabel())
		ops = append(ops, matcher.GetOp())
		values = append(values, matcher.GetVal())
	}
	plannerStreamSelect := logql_transpiler.NewStreamSelectPlanner(labelNames, ops, values)

	return plannerStreamSelect.Process(ctx)
}
