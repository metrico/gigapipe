package clickhouse_planner

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type TimeSeriesInitPlanner struct{}

func NewTimeSeriesInitPlanner() shared.SQLRequestPlanner {
	p := plugins.GetTimeSeriesInitPlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &TimeSeriesInitPlanner{}
}

func (t *TimeSeriesInitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("time_series.fingerprint", "fingerprint"),
			sql.NewSimpleCol("mapFromArrays("+
				"arrayMap(x -> x.1, JSONExtractKeysAndValues(time_series.labels, 'String') as rawlbls), "+
				"arrayMap(x -> x.2, rawlbls))", "labels")).
		From(sql.NewSimpleCol(ctx.TimeSeriesDistTableName, "time_series")).
		AndPreWhere(
			sql.Ge(sql.NewRawObject("time_series.date"), sql.NewStringVal(FormatFromDate(ctx.From))),
			GetTypes(ctx),
		), nil
}
