package promql_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type InitClickhousePlanner struct {
}

func NewInitClickhousePlanner() shared.SQLRequestPlanner {
	p := plugins.GetInitClickhousePlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &InitClickhousePlanner{}
}

func (i *InitClickhousePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	res := sql.NewSelect().Select(
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		sql.NewSimpleCol("samples.value", "value"),
		sql.NewSimpleCol("intDiv(samples.timestamp_ns, 1000000)", "timestamp_ms"),
	).From(sql.NewSimpleCol(ctx.SamplesTableName, "samples")).AndWhere(
		sql.Gt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
		sql.Le(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
		clickhouse_planner.GetTypes(ctx),
	).OrderBy(sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
		sql.NewOrderBy(sql.NewRawObject("samples.timestamp_ns"), sql.ORDER_BY_DIRECTION_ASC))
	if ctx.Limit > 0 {
		res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}
