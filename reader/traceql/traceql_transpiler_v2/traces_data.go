package traceql_transpiler_v2

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/plugins"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// TracesDataPlanner wraps the main query to get full trace data.
type TracesDataPlanner struct {
	Main        shared.SQLRequestPlanner
	SelectAttrs []string // Attributes to fetch for select() operator
}

// NewTracesDataPlanner creates a new TracesDataPlanner.
func NewTracesDataPlanner(main shared.SQLRequestPlanner) shared.SQLRequestPlanner {
	p := plugins.GetTracesDataPlugin()
	if p != nil {
		return (*p)(main)
	}
	return &TracesDataPlanner{Main: main}
}

// NewTracesDataPlannerWithAttrs creates a TracesDataPlanner with select attributes.
func NewTracesDataPlannerWithAttrs(main shared.SQLRequestPlanner, selectAttrs []string) shared.SQLRequestPlanner {
	p := plugins.GetTracesDataPlugin()
	if p != nil {
		return (*p)(main)
	}
	return &TracesDataPlanner{Main: main, SelectAttrs: selectAttrs}
}

func (t *TracesDataPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := t.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	table := ctx.TracesTable
	if ctx.IsCluster {
		table = ctx.TracesDistTable
	}

	withMain := sql.NewWith(main, "index_grouped")
	withTraceIds := sql.NewWith(
		sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(withMain)),
		"trace_ids")
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
				sql.NewSimpleCol("toFloat64(max(traces.timestamp_ns + traces.duration_ns) - min(traces.timestamp_ns)) / 1000000", "_duration_ms"),
				sql.NewSimpleCol("argMin(traces.service_name, traces.timestamp_ns)", "_root_service_name"),
				sql.NewSimpleCol("argMin(traces.name, traces.timestamp_ns)", "_root_trace_name")).
			From(sql.NewSimpleCol(ctx.TracesTable, "traces")).
			AndWhere(sql.NewIn(sql.NewRawObject("traces.trace_id"), withTraceIdsRef)).
			GroupBy(sql.NewRawObject("traces.trace_id")),
		"traces_info")

	// Build WHERE conditions
	whereConditions := []sql.SQLCondition{
		sql.NewIn(sql.NewRawObject("traces.trace_id"), withTraceIdsRef),
		sql.NewIn(sql.NewRawObject("(traces.trace_id, traces.span_id)"), withTraceIdsSpanIdsRef),
	}

	// Apply nestedSetParent filter if set
	if ctx.NestedSetParentFilter == "root" {
		// Root spans have empty parent_id
		whereConditions = append(whereConditions, sql.Eq(sql.NewRawObject("traces.parent_id"), sql.NewStringVal("")))
	} else if ctx.NestedSetParentFilter == "non-root" {
		// Non-root spans have non-empty parent_id
		whereConditions = append(whereConditions, sql.Neq(sql.NewRawObject("traces.parent_id"), sql.NewStringVal("")))
	}

	return sql.NewSelect().
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
		).
		From(sql.NewSimpleCol(table, "traces")).
		Join(sql.NewJoin(
			"any left",
			sql.NewWithRef(withTracesInfo),
			sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject(withTracesInfo.GetAlias()+".trace_id"))),
		).
		AndWhere(whereConditions...).
		GroupBy(sql.NewRawObject("traces.trace_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("start_time_unix_nano"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
