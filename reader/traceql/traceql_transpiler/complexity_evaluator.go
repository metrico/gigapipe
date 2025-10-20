package traceql_transpiler

import (
	"sort"
	"strconv"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/utils/sql_select"
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

	sqlReq, err := evaluateComplexity.String(&sql_select.Ctx{
		Params: map[string]sql_select.SQLObject{},
		Result: map[string]sql_select.SQLObject{},
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
