package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type AllValuesRequestPlanner struct {
	Key string
}

func (a *AllValuesRequestPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Distinct(true).
		Select(sql.NewSimpleCol("val", "val")).
		From(sql.NewRawObject(ctx.TracesKVDistTable)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.To))),
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key))), nil
}
