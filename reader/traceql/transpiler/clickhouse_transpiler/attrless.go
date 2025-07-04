package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type AttrlessConditionPlanner struct {
	onlyRootSpans bool
}

func NewAttrlessConditionPlanner(onlyRootSpans bool) shared.SQLRequestPlanner {
	p := plugins.GetAttrlessConditionPlannerPlugin()
	if p != nil {
		return (*p)(onlyRootSpans)
	}
	return &AttrlessConditionPlanner{onlyRootSpans: onlyRootSpans}
}

func (a *AttrlessConditionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tracesTable := ctx.TracesTable
	traceIds := sql.NewSelect().Select(sql.NewSimpleCol("trace_id", "trace_id")).
		Distinct(true).
		From(sql.NewSimpleCol(tracesTable, "traces")).
		AndWhere(sql.And(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Le(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
		)).OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC))
	if ctx.Limit > 0 {
		traceIds.Limit(sql.NewIntVal(ctx.Limit))
	}

	withTraceIds := sql.NewWith(traceIds, "trace_ids")
	traceAndSpanIds := sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("groupArray(100)(span_id)", "span_id")).
		From(sql.NewSimpleCol(tracesTable, "traces")).
		AndWhere(sql.And(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("trace_id"), sql.NewWithRef(withTraceIds)),
		)).
		GroupBy(sql.NewRawObject("trace_id"))
	withTraceAndSpanIds := sql.NewWith(traceAndSpanIds, "trace_and_span_ids")
	traceAndSpanIdsUnnested := sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("_span_id", "span_id")).
		From(sql.NewWithRef(withTraceAndSpanIds)).
		Join(sql.NewJoin("array", sql.NewSimpleCol(withTraceAndSpanIds.GetAlias()+".span_id", "_span_id"), nil))
	withTraceAndSpanIdsUnnested := sql.NewWith(traceAndSpanIdsUnnested, "trace_and_span_ids_unnested")
	if a.onlyRootSpans {
		return sql.NewSelect().
			With(withTraceIds, withTraceAndSpanIds, withTraceAndSpanIdsUnnested).
			Select(
				sql.NewSimpleCol("trace_id", "trace_id"),
				sql.NewSimpleCol("argMin(span_id, traces.timestamp_ns)", "span_id"),
				sql.NewSimpleCol("argMin(duration_ns, traces.timestamp_ns)", "duration"),
				sql.NewSimpleCol("min(traces.timestamp_ns)", "timestamp_ns")).
			From(sql.NewSimpleCol(tracesTable, "traces")).
			AndWhere(sql.And(
				sql.Ge(sql.NewRawObject("traces.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
				sql.Lt(sql.NewRawObject("traces.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
				sql.NewIn(
					sql.NewRawObject("(traces.trace_id, traces.span_id)"),
					sql.NewWithRef(withTraceAndSpanIdsUnnested)))).
			GroupBy(sql.NewRawObject("trace_id")).
			OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)), nil
	}
	return sql.NewSelect().
		With(withTraceIds, withTraceAndSpanIds, withTraceAndSpanIdsUnnested).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("duration_ns", "duration"),
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns")).
		From(sql.NewSimpleCol(tracesTable, "traces")).
		AndWhere(sql.And(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(
				sql.NewRawObject("(traces.trace_id, traces.span_id)"),
				sql.NewWithRef(withTraceAndSpanIdsUnnested)))).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
