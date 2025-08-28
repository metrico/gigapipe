package transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/model"
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
