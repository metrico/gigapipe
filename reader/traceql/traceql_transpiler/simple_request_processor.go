package traceql_transpiler

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
)

type SimpleRequestProcessor struct {
	main shared.SQLRequestPlanner
}

func (s *SimpleRequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	planner := &TraceQLRequestProcessor{s.main}
	return planner.Process(ctx)
}

func (s *SimpleRequestProcessor) SetMain(main shared.SQLRequestPlanner) {
	s.main = main
}
