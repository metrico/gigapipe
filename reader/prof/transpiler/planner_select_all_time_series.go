package transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type AllTimeSeriesSelectPlanner struct {
}

func (s *AllTimeSeriesSelectPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	res := sql.NewSelect().
		Distinct(true).
		Select(sql.NewSimpleCol("tags", "tags"),
			sql.NewSimpleCol("type_id", "type_id"),
			sql.NewSimpleCol("_sample_types_units", "__sample_types_units")).
		From(sql.NewSimpleCol(ctx.ProfilesSeriesDistTable, "p")).
		Join(sql.NewJoin("array", sql.NewSimpleCol("sample_types_units", "_sample_types_units"), nil)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.To))))
	return res, nil
}
