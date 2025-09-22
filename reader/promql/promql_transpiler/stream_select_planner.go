package promql_transpiler

import (
	logql_transpiler "github.com/metrico/qryn/reader/logql/logql_transpiler_v2/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/promql/promql_parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
)

type StreamSelectPlanner struct {
	Matchers []*labels.Matcher
}

func (s *StreamSelectPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var (
		labelNames []string
		ops        []string
		values     []string
	)
	for _, _matcher := range s.Matchers {
		matcher := promql_parser.LabelMatcher{Node: _matcher}
		labelNames = append(labelNames, matcher.GetLabel())
		ops = append(ops, matcher.GetOp())
		values = append(values, matcher.GetVal())
	}
	plannerStreamSelect := logql_transpiler.NewStreamSelectPlanner(labelNames, ops, values, nil)
	return plannerStreamSelect.Process(ctx)
}
