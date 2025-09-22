package promql_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type InitDownsamplePlanner struct {
	/*TODO: move to PRO !!!TURNED OFF
	Use15SV2 bool
	Partial  bool
	*/
}

func NewInitDownsamplePlanner() shared.SQLRequestPlanner {
	p := plugins.GetInitDownsamplePlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &InitDownsamplePlanner{}
}

func (i *InitDownsamplePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tableName := ctx.Metrics15sTableName
	/* TODO: move to PRO !!!TURNED OFF
	if i.Use15SV2 {
		tableName = ctx.Metrics15sV2TableName
	}
	*/
	valueCol := "argMaxMerge(samples.last)"
	/* TODO: move to PRO !!!TURNED OFF
	if i.Partial {
		valueCol = "argMaxMergeState(samples.last)"
	}*/
	res := sql.NewSelect().Select(
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		//sql.NewSimpleCol(labelsCol, "labels"),
		sql.NewSimpleCol(valueCol, "value"),
		sql.NewSimpleCol("intDiv(samples.timestamp_ns, 1000000)", "timestamp_ms"),
	).From(sql.NewSimpleCol(tableName, "samples")).AndWhere(
		sql.Gt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
		sql.Le(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
		clickhouse_planner.GetTypes(ctx),
	).OrderBy(
		sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
		sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC),
	).GroupBy(sql.NewRawObject("timestamp_ms"), sql.NewRawObject("fingerprint"))
	if ctx.Limit > 0 {
		res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}
