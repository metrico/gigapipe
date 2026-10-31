package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// emptySelected is a selected-value column of a search without select().
const emptySelected = "CAST([], 'Array(Array(String))')"

// TracesDataPlanner reads the listed spans and trace summaries of Main's
// (trace_id, span_id[], spansets) rows, as SearchSpansetPlanner returns them.
// Each spanset comes out as an element of six arrays: its span ids, matched,
// and its attributes' names, types, values and texts. Each listed span also
// comes with the keys and values of the Selected attributes it has, sorted by
// key. A selected nested-set field is always there, 0 for an unnumbered span.
type TracesDataPlanner struct {
	Main     shared.SQLRequestPlanner
	Selected []string
}

func NewTracesDataPlanner(main shared.SQLRequestPlanner, selected []string) shared.SQLRequestPlanner {
	p := plugins.GetTracesDataPlugin()
	if p != nil {
		return (*p)(main)
	}
	return &TracesDataPlanner{Main: main, Selected: selected}
}

func (t *TracesDataPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := t.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	table := tracesTable(ctx)
	join := "any left"
	if ctx.IsCluster {
		join = "global any left"
	}

	withMain := sql.NewWith(main, "index_grouped")
	withTraceIds := sql.NewWith(traceIdsOf(withMain), "trace_ids")
	withTraceIdsRef := sql.NewWithRef(withTraceIds)
	withTraceIdsSpanIds := sql.NewWith(
		sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
			From(sql.NewWithRef(withMain)).
			Join(sql.NewJoin("array", sql.NewRawObject("span_id"), nil)),
		"trace_span_ids")
	withTraceIdsSpanIdsRef := sql.NewWithRef(withTraceIdsSpanIds)
	withTracesInfo := sql.NewWith(
		sql.NewSelect().
			Select(
				sql.NewSimpleCol("traces.trace_id", "trace_id"),
				sql.NewSimpleCol("min(traces.timestamp_ns)", "_start_time_unix_nano"),
				sql.NewSimpleCol("intDiv(max(traces.timestamp_ns + traces.duration_ns) - min(traces.timestamp_ns), 1000000)", "_duration_ms"),
				sql.NewSimpleCol("argMin(traces.service_name, traces.timestamp_ns)", "_root_service_name"),
				sql.NewSimpleCol("argMin(traces.name, traces.timestamp_ns)", "_root_trace_name")).
			From(sql.NewSimpleCol(table, "traces")).
			AndWhere(inSubquery(ctx, sql.NewRawObject("traces.trace_id"), withTraceIdsRef)).
			GroupBy(sql.NewRawObject("traces.trace_id")),
		"traces_info")
	attrKeys, nestedSet := splitSelected(t.Selected)
	keysCol, valsCol := emptySelected, emptySelected
	var withValues, withSelected, withNestedSet *sql.With
	if len(attrKeys) > 0 {
		withValues, withSelected = selectedAttrs(ctx, attrKeys, withTraceIdsRef, withTraceIdsSpanIdsRef)
		keysCol, valsCol = "groupArray(selected_attrs.keys)", "groupArray(selected_attrs.vals)"
	}
	if len(nestedSet) > 0 {
		withNestedSet = selectedNestedSet(ctx, nestedSet, withTraceIdsRef, withTraceIdsSpanIdsRef)
		kv := nestedSetKV(nestedSet)
		if withSelected != nil {
			kv = "arrayConcat(selected_attrs.kv, " + kv + ")"
		}
		keysCol = fmt.Sprintf("groupArray(arrayMap(x -> x.1, arraySort(%s)))", kv)
		valsCol = fmt.Sprintf("groupArray(arrayMap(x -> x.2, arraySort(%s)))", kv)
	}
	res := sql.NewSelect().
		With(withMain, withTraceIds, withTraceIdsSpanIds, withTracesInfo).
		Select(
			sql.NewSimpleCol("lower(hex(traces.trace_id))", "trace_id"),
			sql.NewSimpleCol(`arrayMap(x -> lower(hex(x)), groupArray(traces.span_id))`, "span_id"),
			sql.NewSimpleCol(`groupArray(traces.duration_ns)`, "duration"),
			sql.NewSimpleCol(`groupArray(traces.timestamp_ns)`, "timestamp_ns"),
			sql.NewSimpleCol("min(_start_time_unix_nano)", "start_time_unix_nano"),
			sql.NewSimpleCol("min(_duration_ms)", "duration_ms"),
			sql.NewSimpleCol("min(_root_service_name)", "root_service_name"),
			sql.NewSimpleCol("min(_root_trace_name)", "root_trace_name"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> lower(hex(x)), s.2), any(index_grouped.spansets))", "spanset_span_ids"),
			sql.NewSimpleCol("arrayMap(s -> s.3, any(index_grouped.spansets))", "matched"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.1, s.4), any(index_grouped.spansets))", "attr_names"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.2, s.4), any(index_grouped.spansets))", "attr_types"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.3, s.4), any(index_grouped.spansets))", "attr_values"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.4, s.4), any(index_grouped.spansets))", "attr_texts"),
			sql.NewSimpleCol(keysCol, "span_attr_keys"),
			sql.NewSimpleCol(valsCol, "span_attr_vals"),
		).
		From(sql.NewSimpleCol(table, "traces")).
		Join(sql.NewJoin(
			join,
			sql.NewWithRef(withTracesInfo),
			sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject(withTracesInfo.GetAlias()+".trace_id"))),
			sql.NewJoin(
				join,
				sql.NewWithRef(withMain),
				sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject(withMain.GetAlias()+".trace_id"))),
		).
		AndWhere(
			inSubquery(ctx, sql.NewRawObject("traces.trace_id"), withTraceIdsRef),
			inSubquery(ctx, sql.NewRawObject("(traces.trace_id, traces.span_id)"), withTraceIdsSpanIdsRef)).
		GroupBy(sql.NewRawObject("traces.trace_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("start_time_unix_nano"), sql.ORDER_BY_DIRECTION_DESC))
	if withSelected != nil {
		res.AddWith(withValues, withSelected)
		res.AddJoin(sql.NewJoin(join, sql.NewWithRef(withSelected), sql.And(
			sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("selected_attrs.trace_id")),
			sql.Eq(sql.NewRawObject("traces.span_id"), sql.NewRawObject("selected_attrs.span_id")))))
	}
	if withNestedSet != nil {
		res.AddWith(withNestedSet)
		res.AddJoin(sql.NewJoin(join, sql.NewWithRef(withNestedSet), sql.And(
			sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("selected_nested_set.trace_id")),
			sql.Eq(sql.NewRawObject("traces.span_id"), sql.NewRawObject("selected_nested_set.span_id")))))
	}
	return res, nil
}

// selectedNestedSet reads the newest stored values of the nested-set fields
// (nested-set keys) of the listed spans in spanIds: one selected_nested_set
// row per numbered span.
func selectedNestedSet(ctx *shared.PlannerContext, fields []string, traceIds, spanIds sql.SQLObject) *sql.With {
	cols := make([]nestedSetCol, len(fields))
	for i, f := range fields {
		cols[i] = nestedSetCols[f]
	}
	return nestedSetValues(ctx, cols, traceIds, spanIds, "selected_nested_set")
}

// nestedSetKV is a listed span's (key, value) pairs of the nested-set fields.
func nestedSetKV(fields []string) string {
	kv := make([]string, len(fields))
	for i, f := range fields {
		kv[i] = fmt.Sprintf("('%s', toString(%s))", f, nestedSetCols[f].valueIn("selected_nested_set"))
	}
	return "[" + strings.Join(kv, ", ") + "]"
}

// selectedAttrs reads the attributes in attrKeys of the listed spans in
// spanIds, of the traces in traceIds: one selected_attrs row per span with
// its keys and values.
func selectedAttrs(ctx *shared.PlannerContext, attrKeys []string, traceIds, spanIds sql.SQLObject) (
	withValues, withSelected *sql.With) {
	keys := make([]sql.SQLObject, len(attrKeys))
	for i, key := range attrKeys {
		keys[i] = sql.NewStringVal(key)
	}
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	values := sql.NewSelect().
		Select(
			sql.NewSimpleCol("traces_idx.trace_id", "trace_id"),
			sql.NewSimpleCol("traces_idx.span_id", "span_id"),
			sql.NewSimpleCol("traces_idx.key", "key"),
			sql.NewSimpleCol("any(traces_idx.val)", "val")).
		From(sql.NewSimpleCol(attrsTable(ctx), "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("traces_idx.date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("traces_idx.date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			tsFrom,
			tsTo,
			inSubquery(ctx, sql.NewRawObject("traces_idx.trace_id"), traceIds),
			inSubquery(ctx, sql.NewRawObject("(traces_idx.trace_id, traces_idx.span_id)"), spanIds),
			sql.NewIn(sql.NewRawObject("key"), keys...)).
		GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id"), sql.NewRawObject("key"))
	withValues = sql.NewWith(values, "selected_values")
	withSelected = sql.NewWith(
		sql.NewSelect().
			Select(
				sql.NewRawObject("trace_id"),
				sql.NewRawObject("span_id"),
				sql.NewSimpleCol("arraySort(groupArray((key, val)))", "kv"),
				sql.NewSimpleCol("arrayMap(x -> x.1, kv)", "keys"),
				sql.NewSimpleCol("arrayMap(x -> x.2, kv)", "vals")).
			From(sql.NewWithRef(withValues)).
			GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")),
		"selected_attrs")
	return withValues, withSelected
}
