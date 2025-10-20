package planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type AggOpPlanner struct {
	AggregatorPlanner
	Func string
}

func (a *AggOpPlanner) IsMatrix() bool {
	return true
}
func (a *AggOpPlanner) Process(ctx *shared.PlannerContext, in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	switch a.Func {
	case "stddev":
		return nil, &shared.NotSupportedError{Msg: "stddev is not supported yet."}
	case "stdvar":
		return nil, &shared.NotSupportedError{Msg: "stdvar is not supported yet."}
	}
	return a.process(ctx, in, aggregatorPlannerOps{
		addValue: a.addValue,
		finalize: a.finalize,
	})
}

func (a *AggOpPlanner) addValue(ctx *shared.PlannerContext, entry *shared.LogEntry, stream *aggOpStream) {
	idx := (entry.TimestampNS - ctx.From.UnixNano()) / a.Duration.Nanoseconds()
	if idx < 0 || idx*2 > int64(len(stream.values)) {
		return
	}
	switch a.Func {
	case "sum":
		stream.values[idx*2] += entry.Value
		stream.values[idx*2+1] = 1
	case "min":
		if stream.values[idx*2] > entry.Value || stream.values[idx*2+1] == 0 {
			stream.values[idx*2] = entry.Value
			stream.values[idx*2+1] = 1
		}
	case "max":
		if stream.values[idx*2] < entry.Value || stream.values[idx*2+1] == 0 {
			stream.values[idx*2] = entry.Value
			stream.values[idx*2+1] = 1
		}
	case "avg":
		stream.values[idx*2] += entry.Value
		stream.values[idx*2+1]++
	case "count":
		stream.values[idx*2]++
		stream.values[idx*2+1] = 1
	}
}

func (a *AggOpPlanner) finalize(ctx *shared.PlannerContext, stream *aggOpStream) {
	switch a.Func {
	case "avg":
		for i := 0; i < len(stream.values); i += 2 {
			if stream.values[i+1] > 0 {
				stream.values[i] /= stream.values[i+1]
			}
		}
	}
}

type aggOpStream struct {
	labels map[string]string
	values []float64
}
