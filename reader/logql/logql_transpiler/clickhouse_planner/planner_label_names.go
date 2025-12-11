package clickhouse_planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type LabelNamesPlanner struct {
	FPPlanners []shared.SQLRequestPlanner
}

func (l *LabelNamesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var fpSelects []sql.ISelect
	for _, fp := range l.FPPlanners {
		sel, err := fp.Process(ctx)
		if err != nil {
			return nil, err
		}
		fpSelects = append(fpSelects, sel)
	}
	sel := sql.NewSelect().Distinct(true).
		Select(sql.NewRawObject("key")).
		From(sql.NewSimpleCol(ctx.TimeSeriesGinDistTableName, "keys")).
		AndWhere(
			sql.NewIn(sql.NewRawObject("type"), sql.NewIntVal(int64(ctx.Type)), sql.NewIntVal(int64(0))),
			sql.Ge(sql.NewRawObject("date"),
				sql.NewStringVal(FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"),
				sql.NewStringVal(ctx.To.Format("2006-01-02"))),
		)

	if len(fpSelects) > 0 {
		fpUnion := UnionAll{
			ISelect:  fpSelects[0],
			Anothers: fpSelects[1:],
		}
		withFpUnion := sql.NewWith(&fpUnion, "fp")
		sel = sel.
			With(withFpUnion).
			AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFpUnion)))
	}
	return sel, nil
}
