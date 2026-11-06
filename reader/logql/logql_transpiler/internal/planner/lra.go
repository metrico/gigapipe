package planner

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

type LRAPlanner struct {
	AggregatorPlanner
	Func   string
	Offset time.Duration
}

func (l *LRAPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return l.processWindows(ctx, in, rangeAgg{fn: l.Func, r: l.Duration, offset: l.Offset})
}
