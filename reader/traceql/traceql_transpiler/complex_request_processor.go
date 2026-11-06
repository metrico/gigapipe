package traceql_transpiler

import (
	"cmp"
	"slices"
	"strconv"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

// ComplexRequestProcessor reads main one portion of traces at a time and keeps
// the top ctx.Limit by rank, so the result is the same at any portion count.
// Every portion queries the requested time range.
type ComplexRequestProcessor struct {
	main *clickhouse_transpiler.SearchPlan
	read func(*shared.PlannerContext, *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error)
}

func (t *ComplexRequestProcessor) Process(ctx *shared.PlannerContext,
	portions int64) (chan []model.TraceInfo, error) {
	var res []model.TraceInfo
	for i := range portions {
		ctx.RandomFilter = shared.RandomFilter{
			Max: int(portions),
			I:   int(i),
		}
		part, err := t.read(ctx, t.main)
		if err != nil {
			return nil, err
		}
		res = keepTop(append(res, part...), ctx.Limit)
	}
	slices.SortFunc(res, func(a, b model.TraceInfo) int {
		s1, _ := strconv.ParseInt(a.StartTimeUnixNano, 10, 64)
		s2, _ := strconv.ParseInt(b.StartTimeUnixNano, 10, 64)
		return cmp.Or(cmp.Compare(s2, s1), cmp.Compare(b.TraceID, a.TraceID))
	})

	ch := make(chan []model.TraceInfo)
	go func() {
		defer close(ch)
		ch <- res
	}()
	return ch, nil
}

// keepTop keeps the limit highest ranked traces, by newest matched span then
// trace id, or all of them at limit 0.
func keepTop(traces []model.TraceInfo, limit int64) []model.TraceInfo {
	slices.SortFunc(traces, func(a, b model.TraceInfo) int {
		return cmp.Or(cmp.Compare(b.Newest, a.Newest), cmp.Compare(b.TraceID, a.TraceID))
	})
	if limit > 0 && int64(len(traces)) > limit {
		return traces[:limit]
	}
	return traces
}

func (c *ComplexRequestProcessor) SetMain(main *clickhouse_transpiler.SearchPlan) {
	c.main = main
}

// readTraces runs main and collects its traces.
func readTraces(ctx *shared.PlannerContext, main *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
	planner := &TraceQLRequestProcessor{main}
	ch, err := planner.Process(ctx)
	if err != nil {
		return nil, err
	}
	var res []model.TraceInfo
	for traces := range ch {
		res = append(res, traces...)
	}
	return res, nil
}
