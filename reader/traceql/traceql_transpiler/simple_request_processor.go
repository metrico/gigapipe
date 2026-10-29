package traceql_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

type SimpleRequestProcessor struct {
	main *clickhouse_transpiler.SearchPlan
}

func (s *SimpleRequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	planner := &TraceQLRequestProcessor{s.main}
	return planner.Process(ctx)
}

func (s *SimpleRequestProcessor) SetMain(main *clickhouse_transpiler.SearchPlan) {
	s.main = main
}
