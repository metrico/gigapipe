package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type ComparisonPlanner struct {
	Main  shared.SQLRequestPlanner
	Fn    string
	Param float64
}

func (c *ComparisonPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := c.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	var fn func(sql.SQLObject, sql.SQLObject) *sql.LogicalOp
	switch c.Fn {
	case ">":
		fn = sql.Gt
	case "<":
		fn = sql.Lt
	case ">=":
		fn = sql.Ge
	case "<=":
		fn = sql.Le
	case "==":
		fn = sql.Eq
	case "!=":
		fn = sql.Neq
	}

	return main.AndHaving(fn(sql.NewRawObject("value"), sql.NewFloatVal(c.Param))), nil
}
