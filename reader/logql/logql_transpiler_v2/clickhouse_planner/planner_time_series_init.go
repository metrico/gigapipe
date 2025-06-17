package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"time"
)

type TimeSeriesInitPlanner struct {
	Offset *time.Duration
}

func NewTimeSeriesInitPlanner(offset *time.Duration) shared.SQLRequestPlanner {
	p := plugins.GetTimeSeriesInitPlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &TimeSeriesInitPlanner{offset}
}

func (t *TimeSeriesInitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	from := ctx.From
	if t.Offset != nil {
		from = from.Add(*t.Offset)
	}
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("time_series.fingerprint", "fingerprint"),
			sql.NewSimpleCol("mapFromArrays("+
				"arrayMap(x -> x.1, JSONExtractKeysAndValues(time_series.labels, 'String') as rawlbls), "+
				"arrayMap(x -> x.2, rawlbls))", "labels")).
		From(sql.NewSimpleCol(ctx.TimeSeriesDistTableName, "time_series")).
		AndPreWhere(
			sql.Ge(sql.NewRawObject("time_series.date"), sql.NewStringVal(FormatFromDate(from))),
			GetTypes(ctx),
		), nil
}
