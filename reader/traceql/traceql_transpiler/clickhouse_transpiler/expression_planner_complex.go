package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
)

type complexExpressionPlanner struct {
	prefix    string
	_fn       string
	_operands []iExpressionPlanner
}

func (c *complexExpressionPlanner) planEval() (shared.SQLRequestPlanner, error) {
	res := make([]shared.SQLRequestPlanner, len(c._operands))
	var err error
	for i, operand := range c._operands {
		res[i], err = operand.planEval()
		if err != nil {
			return nil, err
		}
	}
	return &ComplexEvalOrPlanner{
		Operands: res,
		Prefix:   c.prefix,
	}, nil
}

func (c *complexExpressionPlanner) addOp(selector iExpressionPlanner) {
	c._operands = append(c._operands, selector)
}

func (c *complexExpressionPlanner) setOps(selector []iExpressionPlanner) {
	c._operands = selector
}

func (c *complexExpressionPlanner) fn() string {
	return c._fn
}

func (c *complexExpressionPlanner) operands() []iExpressionPlanner {
	return c._operands
}

func (c *complexExpressionPlanner) planner() (shared.SQLRequestPlanner, error) {
	planners := make([]shared.SQLRequestPlanner, len(c._operands))
	var err error
	for i, operand := range c._operands {
		planners[i], err = operand.planner()
		if err != nil {
			return nil, err
		}
	}
	switch c._fn {
	case "||":
		return &ComplexOrPlanner{
			Operands: planners,
			Prefix:   c.prefix,
		}, nil
	case "&&":
		return &ComplexAndPlanner{
			Operands: planners,
			Prefix:   c.prefix,
		}, nil
	}
	return nil, fmt.Errorf("unknown operator: %s", c._fn)
}
