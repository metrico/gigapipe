package traceql_transpiler

import (
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

type ComplexRequestProcessor struct {
	main *clickhouse_transpiler.SearchPlan
}

func (t *ComplexRequestProcessor) Process(ctx *shared.PlannerContext,
	complexity int64) (chan []model.TraceInfo, error) {
	portions := (complexity + COMPLEXITY_THRESHOLD - 1) / COMPLEXITY_THRESHOLD
	from := ctx.From
	var cachedTraceIDs []string
	var res []model.TraceInfo
	for i := range portions {
		ctx.RandomFilter = shared.RandomFilter{
			Max: int(portions),
			I:   int(i),
		}
		ctx.CachedTraceIds = cachedTraceIDs
		ctx.From = from
		var err error
		res, from, cachedTraceIDs, err = t.ProcessComplexReqIteration(ctx)
		if err != nil {
			return nil, err
		}
	}

	ch := make(chan []model.TraceInfo)
	go func() {
		defer close(ch)
		ch <- res
	}()
	return ch, nil
}

func (t *ComplexRequestProcessor) ProcessComplexReqIteration(ctx *shared.PlannerContext) (
	[]model.TraceInfo, time.Time, []string, error) {
	var res []model.TraceInfo
	var from time.Time
	var cachedTraceIDs []string
	planner := &TraceQLRequestProcessor{t.main}
	_res, err := planner.Process(ctx)
	if err != nil {
		return nil, from, cachedTraceIDs, err
	}
	for info := range _res {
		for _, _info := range info {
			startTimeUnixNano, err := strconv.ParseInt(_info.StartTimeUnixNano, 10, 64)
			if err != nil {
				return nil, from, cachedTraceIDs, err
			}
			// The range is (from, to], so from sits just before the trace start.
			if from.Nanosecond() == 0 || from.After(time.Unix(0, startTimeUnixNano-1)) {
				from = time.Unix(0, startTimeUnixNano-1)
			}
			res = append(res, _info)
			cachedTraceIDs = append(cachedTraceIDs, _info.TraceID)
		}
	}
	if int64(len(res)) != ctx.Limit {
		from = ctx.From
	}
	return res, from, cachedTraceIDs, nil
}

func (c *ComplexRequestProcessor) SetMain(main *clickhouse_transpiler.SearchPlan) {
	c.main = main
}
