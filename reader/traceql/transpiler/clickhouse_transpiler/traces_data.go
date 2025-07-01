package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"strings"
)

type TracesDataPlanner struct {
	Main shared.SQLRequestPlanner

	returnAttrs []string
}

func NewTracesDataPlanner(main shared.SQLRequestPlanner, returnAttrs []string) shared.SQLRequestPlanner {
	p := plugins.GetTracesDataPlugin()
	if p != nil {
		return (*p)(main, returnAttrs)
	}
	return &TracesDataPlanner{Main: main, returnAttrs: returnAttrs}
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
		).
		From(sql.NewSimpleCol(table, "traces")).
		Join(sql.NewJoin(
			"any left",
			sql.NewWithRef(withTracesInfo),
			sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject(withTracesInfo.GetAlias()+".trace_id"))),
		).
		AndWhere(
			sql.NewIn(sql.NewRawObject("traces.trace_id"), withTraceIdsRef),
			sql.NewIn(sql.NewRawObject("(traces.trace_id, traces.span_id)"), withTraceIdsSpanIdsRef)).
		GroupBy(sql.NewRawObject("traces.trace_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("start_time_unix_nano"), sql.ORDER_BY_DIRECTION_DESC))
	res = t.returnAttributes(res, withTraceIdsSpanIds, ctx)
	return res, nil
}

func (t *TracesDataPlanner) processSelector(name string) string {
	if strings.HasPrefix(name, "resource.") {
		name = name[9:]
	} else if strings.HasPrefix(name, "span.") {
		name = name[5:]
	} else if strings.HasPrefix(name, ".") {
		name = name[1:]
	}
	return name
}

func (t *TracesDataPlanner) returnAttributes(req sql.ISelect, withTraceIDsSpanIDs *sql.With,
	ctx *shared.PlannerContext) sql.ISelect {
	if len(t.returnAttrs) == 0 {
		req.Select(append(
			req.GetSelect(),
			sql.NewSimpleCol("[]::Array(Array(Tuple(String, String)))", "attrs"))...)
		return req
	}

	returnAttrs := make([]string, len(t.returnAttrs))
	for i, attr := range t.returnAttrs {
		returnAttrs[i] = t.processSelector(attr)
	}

	attrsReq := sql.NewSelect().Select(
		sql.NewSimpleCol("trace_id", "trace_id"),
		sql.NewSimpleCol("span_id", "span_id"),
		sql.NewSimpleCol("arrayZip(groupArray(key), groupArray(val))", "attrs"),
	).From(sql.NewRawObject(ctx.TracesAttrsTable)).
		AndWhere(
			sql.NewIn(sql.NewRawObject("(trace_id, span_id)"), sql.NewWithRef(withTraceIDsSpanIDs)),
			sql.NewIn(sql.NewRawObject("key"), sql.NewCustomCol(
				func(ctx *sql.Ctx, options ...int) (string, error) {
					strVals := make([]string, len(t.returnAttrs))
					var err error
					for i, attr := range returnAttrs {
						strVals[i], err = sql.NewStringVal(attr).String(ctx, options...)
						if err != nil {
							return "", err
						}
					}
					return fmt.Sprintf("%s", strings.Join(strVals, ", ")), nil
				})),
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02")))).
		GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id"))
	withAttrsReq := sql.NewWith(attrsReq, "attrs")
	req.With(append(req.GetWith(), withAttrsReq)...).
		Select(append(req.GetSelect(), sql.NewSimpleCol("groupArray(attrs.attrs)", "attrs"))...).
		AddJoin(sql.NewJoin(
			"any left",
			sql.NewWithRef(withAttrsReq),
			sql.And(
				sql.Eq(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("attrs.trace_id")),
				sql.Eq(sql.NewRawObject("traces.span_id"), sql.NewRawObject("attrs.span_id")))))
	return req
}
