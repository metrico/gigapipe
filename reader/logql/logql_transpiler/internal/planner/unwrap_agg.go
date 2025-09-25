package planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
)

type UnwrapAggPlanner struct {
	AggregatorPlanner
	Function string
}

func (l *UnwrapAggPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return l.process(ctx, in, aggregatorPlannerOps{
		addValue: l.addValue,
		finalize: l.finalize,
	})
}

func (l *UnwrapAggPlanner) addValue(ctx *shared.PlannerContext, entry *shared.LogEntry, stream *aggOpStream) {
	idx := (entry.TimestampNS - ctx.From.UnixNano()) / l.Duration.Nanoseconds() * 2
	switch l.Function {
	case "rate":
		stream.values[idx] += entry.Value
		stream.values[idx+1] = 1
	case "sum_over_time":
		stream.values[idx] += entry.Value
		stream.values[idx+1] = 1
	case "avg_over_time":
		stream.values[idx] += entry.Value
		stream.values[idx+1]++
	case "max_over_time":
		if stream.values[idx] < entry.Value || stream.values[idx+1] == 0 {
			stream.values[idx] = entry.Value
			stream.values[idx+1] = 1
		}
	case "min_over_time":
		if stream.values[idx] < entry.Value || stream.values[idx+1] == 0 {
			stream.values[idx] = entry.Value
			stream.values[idx+1] = 1
		}
	case "first_over_time":
		if stream.values[idx] == 0 {
			stream.values[idx] = entry.Value
			stream.values[idx+1] = 1
		}
	case "last_over_time":
		stream.values[idx] = entry.Value
		stream.values[idx+1] = 1
	}
}

func (l *UnwrapAggPlanner) finalize(ctx *shared.PlannerContext, stream *aggOpStream) {
	switch l.Function {
	case "rate":
		for i := 0; i < len(stream.values); i += 2 {
			stream.values[i] /= float64(l.Duration.Milliseconds()) / 1000
		}
	case "avg_over_time":
		for i := 0; i < len(stream.values); i += 2 {
			if stream.values[i+1] != 0 {
				stream.values[i] /= stream.values[i+1]
			}
		}
	}
}
