package planner

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

type UnwrapAggPlanner struct {
	AggregatorPlanner
	Function string
	Offset   time.Duration
}

func (l *UnwrapAggPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return l.processWindows(ctx, in, rangeAgg{fn: l.Function, unwrap: true, r: l.Duration, offset: l.Offset})
}
