package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// nestedSetCol is a column of the nested-set side table, and names the
// columns that carry its value through a query.
type nestedSetCol string

// nestedSetCols maps the nested-set intrinsics to their side-table columns.
var nestedSetCols = map[string]nestedSetCol{
	traceql_ir.NestedSetLeft: "left", traceql_ir.NestedSetRight: "right", traceql_ir.NestedSetParent: "parent",
}

// newest is the nestedSetValues column of the newest stored value.
func (c nestedSetCol) newest() string { return string(c) + "_" }

// pivot is the field pivot column.
func (c nestedSetCol) pivot() string { return "_ns_" + string(c) }

// metrics is the metrics spans column.
func (c nestedSetCol) metrics() string { return "nested_set_" + string(c) }

// valueIn is the value in a nestedSetValues row joined as alias, 0 for a span
// without a row.
func (c nestedSetCol) valueIn(alias string) string {
	return fmt.Sprintf("ifNull(%s.%s, 0)", alias, c.newest())
}

// MetricsNestedSetColumn is the metrics spans column of a nested-set field,
// or false for any other field.
func MetricsNestedSetColumn(field string) (string, bool) {
	col, ok := nestedSetCols[field]
	return col.metrics(), ok
}

// nestedSetValues reads the newest stored values of cols of the traces in
// traceIds (and the spans in spanIds unless nil), each filtered by in: one row
// per numbered span.
func nestedSetValues(ctx *shared.PlannerContext, cols []nestedSetCol, in inFunc,
	traceIds, spanIds sql.SQLObject, alias string) *sql.With {
	table := ctx.TracesNestedSetTable
	if ctx.IsCluster {
		table = ctx.TracesNestedSetDistTable
	}
	sel := []sql.SQLObject{sql.NewSimpleCol("ns.trace_id", "trace_id"), sql.NewSimpleCol("ns.span_id", "span_id")}
	for _, col := range cols {
		sel = append(sel, sql.NewSimpleCol(fmt.Sprintf("argMax(ns.%s, ns.version)", col), col.newest()))
	}
	res := sql.NewSelect().
		Select(sel...).
		From(sql.NewSimpleCol(table, "ns")).
		AndWhere(in(sql.NewRawObject("ns.trace_id"), traceIds)).
		GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id"))
	if spanIds != nil {
		res.AndWhere(in(sql.NewRawObject("(ns.trace_id, ns.span_id)"), spanIds))
	}
	return sql.NewWith(res, alias)
}

// nestedSetJoin is a nestedSetValues CTE, its value columns and its left
// join onto a CTE of spans.
type nestedSetJoin struct {
	with *sql.With
	cols []sql.SQLObject
	join *sql.Join
}

// joinNestedSet joins the newest stored values of cols onto the spans of the
// CTE aliased spans, each value named name(col), with ids filtered by in.
func joinNestedSet(ctx *shared.PlannerContext, cols []nestedSetCol, in inFunc, traceIds, spanIds sql.SQLObject,
	spans, alias string, name func(nestedSetCol) string) nestedSetJoin {
	with := nestedSetValues(ctx, cols, in, traceIds, spanIds, alias)
	v := with.GetAlias()
	res := nestedSetJoin{with: with, join: sql.NewJoin("left", sql.NewWithRef(with), sameSpan(spans+".span_id", v+".span_id"))}
	for _, col := range cols {
		res.cols = append(res.cols, sql.NewSimpleCol(col.valueIn(v), name(col)))
	}
	return res
}

// columnsOf selects the named columns of the CTE aliased alias under the same names.
func columnsOf(alias string, names ...string) []sql.SQLObject {
	cols := make([]sql.SQLObject, len(names))
	for i, n := range names {
		cols[i] = sql.NewSimpleCol(alias+"."+n, n)
	}
	return cols
}

// traceIdsOf selects the trace_id column of with.
func traceIdsOf(with *sql.With) sql.ISelect {
	return sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(with))
}
