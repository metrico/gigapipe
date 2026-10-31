package traceql_transpiler

import (
	"cmp"
	"slices"
	"strconv"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type simpleRequestProcessor[T, P any] interface {
	Process(ctx *shared.PlannerContext) (chan []T, error)
	SetMain(main P)
}

type complexRequestProcessor[T, P any] interface {
	Process(ctx *shared.PlannerContext, complexity int64) (chan []T, error)
	SetMain(main P)
}

// TraceQLComplexityEvaluator reads a plan P of results T, at once or by
// complexity portions.
type TraceQLComplexityEvaluator[T, P any] struct {
	main                      P
	simpleRequestProcessor    simpleRequestProcessor[T, P]
	complexRequestProcessor   complexRequestProcessor[T, P]
	evaluateComplexityPlanner shared.SQLRequestPlanner
}

const COMPLEXITY_THRESHOLD = 10000000

func (t *TraceQLComplexityEvaluator[T, P]) Process(ctx *shared.PlannerContext) (chan []T, error) {
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

func (t *TraceQLComplexityEvaluator[T, P]) ProcessSimpleReq(ctx *shared.PlannerContext) (chan []T, error) {
	t.simpleRequestProcessor.SetMain(t.main)
	return t.simpleRequestProcessor.Process(ctx)
}

func (t *TraceQLComplexityEvaluator[T, P]) ProcessComplexReq(ctx *shared.PlannerContext,
	complexity int64) (chan []T, error) {
	t.complexRequestProcessor.SetMain(t.main)
	return t.complexRequestProcessor.Process(ctx, complexity)
}

// sortSpans orders spans earliest-first, ties by span id.
func sortSpans(spans []model.SpanInfo) {
	slices.SortFunc(spans, func(a, b model.SpanInfo) int {
		s1, _ := strconv.ParseInt(a.StartTimeUnixNano, 10, 64)
		s2, _ := strconv.ParseInt(b.StartTimeUnixNano, 10, 64)
		return cmp.Or(cmp.Compare(s1, s2), cmp.Compare(a.SpanID, b.SpanID))
	})
}
