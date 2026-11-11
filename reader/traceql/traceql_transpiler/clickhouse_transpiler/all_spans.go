package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// AllSpansPlanner is `{}` as an operand of a spanset operator or a pipeline
// stage: every candidate span starting in range.
type AllSpansPlanner struct{}

func (a *AllSpansPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	res := sql.NewSelect().
		Select(
			sql.NewSimpleCol("traces.trace_id", "trace_id"),
			sql.NewSimpleCol("traces.span_id", "span_id"),
			sql.NewSimpleCol("any(traces.timestamp_ns)", "timestamp_ns")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(tsFrom, tsTo).
		GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id"))
	restrictOperand(ctx, res)
	return res, nil
}

// AllSpansEvaluatorPlanner estimates AllSpansPlanner as its span count.
type AllSpansEvaluatorPlanner struct {
	Prefix string
}

func (a *AllSpansEvaluatorPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		Select(
			sql.NewCol(sql.NewStringVal(a.Prefix), "prefix"),
			sql.NewSimpleCol("count()", "_count")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(tsFrom, tsTo), nil
}
