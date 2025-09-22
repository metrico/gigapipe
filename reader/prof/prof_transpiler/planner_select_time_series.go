package prof_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/prof/prof_parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type TimeSeriesSelectPlanner struct {
	/* StreamSelectPlanner or union of StreamSelectPlanners */
	Fp        shared.SQLRequestPlanner
	Selectors []prof_parser.Selector
}

func (t *TimeSeriesSelectPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := t.Fp.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFp := sql.NewWith(fp, "fp")

	matchers, err := (&StreamSelectorPlanner{Selectors: t.Selectors}).getMatchers()
	if err != nil {
		return nil, err
	}

	res := sql.NewSelect().
		With(withFp).
		Distinct(true).
		Select(
			sql.NewSimpleCol("tags", "tags"),
			sql.NewSimpleCol("type_id", "type_id"),
			sql.NewSimpleCol("_sample_types_units", "__sample_types_units")).
		From(sql.NewSimpleCol(ctx.ProfilesSeriesDistTable, "p")).
		Join(sql.NewJoin("array", sql.NewSimpleCol("sample_types_units", "_sample_types_units"), nil)).
		AndWhere(
			sql.NewIn(sql.NewRawObject("p.fingerprint"), sql.NewWithRef(withFp)),
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.To))))
	if len(matchers.globalMatchers) > 0 {
		res = res.AndWhere(matchers.globalMatchers...)
	}
	return res, nil
}
