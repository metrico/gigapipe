package traceql_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
)

// ComplexRequestProcessor reads main one portion of traces at a time. Every
// portion queries the requested time range.
type ComplexRequestProcessor struct {
	main shared.SQLRequestPlanner
	read func(*shared.PlannerContext, shared.SQLRequestPlanner) ([]model.TraceInfo, error)
}

func (t *ComplexRequestProcessor) Process(ctx *shared.PlannerContext,
	portions int64) (chan []model.TraceInfo, error) {
	var ids []string
	var res []model.TraceInfo
	for i := range portions {
		ctx.RandomFilter = shared.RandomFilter{
			Max: int(portions),
			I:   int(i),
		}
		ctx.CachedTraceIds = ids
		var err error
		res, err = t.read(ctx, t.main)
		if err != nil {
			return nil, err
		}
		ids = make([]string, 0, len(res))
		for _, info := range res {
			ids = append(ids, info.TraceID)
		}
	}

	ch := make(chan []model.TraceInfo)
	go func() {
		defer close(ch)
		ch <- res
	}()
	return ch, nil
}

func (c *ComplexRequestProcessor) SetMain(main shared.SQLRequestPlanner) {
	c.main = main
}

// readTraces runs main and collects its traces.
func readTraces(ctx *shared.PlannerContext, main shared.SQLRequestPlanner) ([]model.TraceInfo, error) {
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
