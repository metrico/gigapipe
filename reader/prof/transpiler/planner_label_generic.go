package transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type GenericLabelsPlanner struct {
	Fingerprints shared.SQLRequestPlanner
}

func (l *GenericLabelsPlanner) _process(ctx *shared.PlannerContext, returnCol string) (sql.ISelect, error) {
	var (
		fpReq sql.ISelect
		err   error
	)
	if l.Fingerprints != nil {
		fpReq, err = l.Fingerprints.Process(ctx)
		if err != nil {
			return nil, err
		}
	}
	withFpReq := sql.NewWith(fpReq, "fp")
	res := sql.NewSelect().
		Distinct(true).
		Select(sql.NewRawObject(returnCol)).
		From(sql.NewRawObject(ctx.ProfilesSeriesGinDistTable)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.To)))).
		Limit(sql.NewIntVal(10000))
	if fpReq != nil {
		res = res.With(withFpReq).AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFpReq)))
	}
	return res, nil
}
