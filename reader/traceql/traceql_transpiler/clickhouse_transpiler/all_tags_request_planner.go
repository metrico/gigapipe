package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type AllTagsRequestPlanner struct {
}

func (a *AllTagsRequestPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Distinct(true).
		Select(sql.NewSimpleCol("key", "key")).
		From(sql.NewRawObject(ctx.TracesKVDistTable)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.To)))), nil
}
