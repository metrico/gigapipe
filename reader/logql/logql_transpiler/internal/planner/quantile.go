package planner

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

type QuantilePlanner struct {
	AggregatorPlanner
	Param float64
}

func (q *QuantilePlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return q.processWindows(ctx, in, rangeAgg{fn: "quantile_over_time", unwrap: true, q: q.Param, r: q.Duration})
}
