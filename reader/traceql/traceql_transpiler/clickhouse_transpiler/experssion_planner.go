package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
)

type iExpressionPlanner interface {
	fn() string
	operands() []iExpressionPlanner
	planner() (shared.SQLRequestPlanner, error)
	addOp(selector iExpressionPlanner)
	setOps(selector []iExpressionPlanner)
	planEval() (shared.SQLRequestPlanner, error)
}

type rootExpressionPlanner struct {
	operand iExpressionPlanner
}

func (r *rootExpressionPlanner) planEval() (shared.SQLRequestPlanner, error) {
	return r.operand.planEval()
}

func (r *rootExpressionPlanner) fn() string {
	return ""
}

func (r *rootExpressionPlanner) operands() []iExpressionPlanner {
	return []iExpressionPlanner{r.operand}
}

func (r *rootExpressionPlanner) planner() (shared.SQLRequestPlanner, error) {
	return r.operand.planner()
}

func (r *rootExpressionPlanner) addOp(selector iExpressionPlanner) {
	r.operand = selector
}

func (r *rootExpressionPlanner) setOps(selector []iExpressionPlanner) {
	r.operand = selector[0]
}
