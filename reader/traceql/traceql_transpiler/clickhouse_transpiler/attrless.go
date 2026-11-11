package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// AttrlessConditionPlanner plans a whole-query `{}`: every span starting in
// range of the ctx.Limit traces with the newest such spans.
type AttrlessConditionPlanner struct {
}

func NewAttrlessConditionPlanner() shared.SQLRequestPlanner {
	p := plugins.GetAttrlessConditionPlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &AttrlessConditionPlanner{}
}

func (a *AttrlessConditionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	table := tracesTable(ctx)
	tsFrom, tsTo := timerange.SpanStart("timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	traceIds := sql.NewSelect().Select(sql.NewSimpleCol("trace_id", "trace_id")).
		From(sql.NewSimpleCol(table, "traces")).
		AndWhere(sql.And(tsFrom, tsTo)).
		GroupBy(sql.NewRawObject("trace_id")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("max(timestamp_ns)"), sql.ORDER_BY_DIRECTION_DESC),
			sql.NewOrderBy(sql.NewRawObject("trace_id"), sql.ORDER_BY_DIRECTION_DESC)).
		Limit(sql.NewIntVal(ctx.Limit))
	withTraceIds := sql.NewWith(traceIds, "attrless_trace_ids")
	return sql.NewSelect().
		With(withTraceIds).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("duration_ns", "duration"),
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns")).
		From(sql.NewSimpleCol(table, "traces")).
		AndWhere(sql.And(
			tsFrom,
			tsTo,
			inSubquery(ctx, sql.NewRawObject("trace_id"), sql.NewWithRef(withTraceIds)))).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
