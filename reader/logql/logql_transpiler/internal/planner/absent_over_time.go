package planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type AbsentOverTimePlanner struct {
	AggregatorPlanner
}

func (a *AbsentOverTimePlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return a.process(ctx, in, aggregatorPlannerOps{
		addValue: func(ctx *shared.PlannerContext, entry *shared.LogEntry, stream *aggOpStream) {
			idx := (entry.TimestampNS - ctx.From.UnixNano()) / a.Duration.Nanoseconds() * 2
			if idx > 0 && idx < int64(len(stream.values)) {
				stream.values[idx] = 0
				stream.values[idx+1] = 0
			}
		},
		finalize: func(ctx *shared.PlannerContext, stream *aggOpStream) {},
		initStream: func(ctx *shared.PlannerContext, stream *aggOpStream) {
			stream.values[0] = 1
			for i := 1; i < len(stream.values); i <<= 1 {
				copy(stream.values[i:], stream.values[:i])
			}
		},
	})
}
