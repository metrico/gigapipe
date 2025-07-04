package traceql_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"sort"
	"strconv"
)

type simpleRequestProcessor[T any] interface {
	Process(ctx *shared.PlannerContext) (chan []T, error)
	SetMain(main shared.SQLRequestPlanner)
}

type complexRequestProcessor[T any] interface {
	Process(ctx *shared.PlannerContext, complexity int64) (chan []T, error)
	SetMain(main shared.SQLRequestPlanner)
}

type TraceQLComplexityEvaluator[T any] struct {
	initSqlPlanner            shared.SQLRequestPlanner
	simpleRequestProcessor    simpleRequestProcessor[T]
	complexRequestProcessor   complexRequestProcessor[T]
	evaluateComplexityPlanner shared.SQLRequestPlanner
}

const COMPLEXITY_THRESHOLD = 10000000

func (t *TraceQLComplexityEvaluator[T]) Process(ctx *shared.PlannerContext) (chan []T, error) {
	evaluateComplexity, err := t.evaluateComplexityPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	sqlReq, err := evaluateComplexity.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}

	var complexity int64
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, sqlReq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var _complexity int64
		err = rows.Scan(&_complexity)
		if err != nil {
			return nil, err
		}

		if _complexity > complexity {
			complexity = _complexity
		}
	}
	if complexity < COMPLEXITY_THRESHOLD {
		return t.ProcessSimpleReq(ctx)
	}
	return t.ProcessComplexReq(ctx, complexity)
}

func (t *TraceQLComplexityEvaluator[T]) ProcessSimpleReq(ctx *shared.PlannerContext) (chan []T, error) {
	t.simpleRequestProcessor.SetMain(t.initSqlPlanner)
	return t.simpleRequestProcessor.Process(ctx)
}

func (t *TraceQLComplexityEvaluator[T]) ProcessComplexReq(ctx *shared.PlannerContext,
	complexity int64) (chan []T, error) {
	t.complexRequestProcessor.SetMain(t.initSqlPlanner)
	return t.complexRequestProcessor.Process(ctx, complexity)
}

func sortSpans(spans []model.SpanInfo) {
	sort.Slice(spans, func(_i, j int) bool {
		s1, _ := strconv.ParseInt(spans[_i].StartTimeUnixNano, 10, 64)
		s2, _ := strconv.ParseInt(spans[j].StartTimeUnixNano, 10, 64)
		return s1 > s2
	})
}

type TraceQLSamplesComplexityEvaluator struct {
	TraceQLComplexityEvaluator[model.TraceInfo]
}

func (t *TraceQLSamplesComplexityEvaluator) Process(context *shared.PlannerContext) (model.TraceQLResponse, error) {
	c, err := t.TraceQLComplexityEvaluator.Process(context)
	if err != nil {
		return model.TraceQLResponse{}, err
	}
	return model.TraceQLResponse{
		TraceInfo: c,
	}, nil
}
