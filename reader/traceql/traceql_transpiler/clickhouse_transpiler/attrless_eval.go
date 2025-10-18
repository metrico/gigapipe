package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type AttrlessEvaluatorPlanner struct {
	Prefix string
}

func (a *AttrlessEvaluatorPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Select(
			sql.NewCol(sql.NewStringVal(a.Prefix), "prefix"),
			sql.NewCol(sql.NewIntVal(ctx.Limit), "_count")), nil
}
