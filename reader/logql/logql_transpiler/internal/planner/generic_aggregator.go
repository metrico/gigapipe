package planner

import (
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
)

type AggregatorPlanner struct {
	GenericPlanner
	Duration time.Duration
}

func (g *AggregatorPlanner) IsMatrix() bool {
	return true
}

func (p *AggregatorPlanner) process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry, ops aggregatorPlannerOps) (chan []shared.LogEntry, error) {

	streamLen := ctx.To.Sub(ctx.From).Nanoseconds() / p.Duration.Nanoseconds()
	if streamLen > 4000000000 {
		return nil, &shared.NotSupportedError{Msg: "stream length is too large. Please try increasing duration."}
	}

	res := map[uint64]*aggOpStream{}

	return p.GenericPlanner.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil {
				return entry.Err
			}
			if _, ok := res[entry.Fingerprint]; !ok {
				if len(res) >= 2000 {
					return &shared.NotSupportedError{
						Msg: "Too many time-series. Please try changing `by / without` clause.",
					}
				}
				res[entry.Fingerprint] = &aggOpStream{
					labels: entry.Labels,
					values: make([]float64, streamLen*2),
				}
				if ops.initStream != nil {
					ops.initStream(ctx, res[entry.Fingerprint])
				}
			}
			ops.addValue(ctx, entry, res[entry.Fingerprint])
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			return nil
		},
		OnAfterEntries: func(out chan []shared.LogEntry) error {
			for k, v := range res {
				ops.finalize(ctx, v)
				entries := make([]shared.LogEntry, 0, len(v.values)/2)
				for i := 0; i < len(v.values); i += 2 {
					if v.values[i+1] > 0 {
						entries = append(entries, shared.LogEntry{
							Fingerprint: k,
							TimestampNS: ctx.From.Add(time.Duration(i/2) * p.Duration).UnixNano(),
							Labels:      v.labels,
							Value:       v.values[i],
						})
					}
				}
				if len(entries) > 0 {
					out <- entries
				}
			}
			return nil
		},
	})

}

type aggregatorPlannerOps struct {
	addValue   func(ctx *shared.PlannerContext, entry *shared.LogEntry, stream *aggOpStream)
	finalize   func(ctx *shared.PlannerContext, stream *aggOpStream)
	initStream func(ctx *shared.PlannerContext, stream *aggOpStream)
}
