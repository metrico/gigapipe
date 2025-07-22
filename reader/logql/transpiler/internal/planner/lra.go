package planner

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

type LRAPlanner struct {
	AggregatorPlanner
	Func string
}

func (l *LRAPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	if l.Func == "absent_over_time" {
		return (&AbsentOverTimePlanner{
			AggregatorPlanner: l.AggregatorPlanner,
		}).Process(ctx, in)
	}
	return l.process(ctx, in, aggregatorPlannerOps{
		addValue: l.addValue,
		finalize: l.finalize,
	})
}

func (l *LRAPlanner) addValue(ctx *shared.PlannerContext, entry *shared.LogEntry, stream *aggOpStream) {
	idx := (entry.TimestampNS - ctx.From.UnixNano()) / l.Duration.Nanoseconds() * 2
	switch l.Func {
	case "rate":
		stream.values[idx]++
		stream.values[idx+1] = 1
	case "count_over_time":
		stream.values[idx]++
		stream.values[idx+1] = 1
	case "bytes_rate":
		stream.values[idx] += float64(len(entry.Message))
		stream.values[idx+1] = 1
	case "bytes_over_time":
		stream.values[idx] += float64(len(entry.Message))
		stream.values[idx+1] = 1
	}
}

func (l *LRAPlanner) finalize(ctx *shared.PlannerContext, stream *aggOpStream) {
	switch l.Func {
	case "rate":
		for i := 0; i < len(stream.values); i += 2 {
			stream.values[i] /= float64(l.Duration.Milliseconds()) / 1000
		}
	case "bytes_rate":
		for i := 0; i < len(stream.values); i += 2 {
			stream.values[i] /= float64(l.Duration.Milliseconds()) / 1000
		}
	}
}
