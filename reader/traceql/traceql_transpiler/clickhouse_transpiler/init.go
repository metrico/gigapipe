package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type InitIndexPlanner struct {
	dist bool
}

func NewInitIndexPlanner(dist bool) shared.SQLRequestPlanner {
	p := plugins.GetInitIndexPlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &InitIndexPlanner{dist: dist}
}

func (i *InitIndexPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	table := ctx.TracesAttrsTable
	if i.dist {
		table = ctx.TracesAttrsDistTable
	}
	dateFrom, dateTo := timerange.SpanDate("date", ctx.From, ctx.To)
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().Select(
		sql.NewSimpleCol("trace_id", "trace_id"),
		sql.NewSimpleCol("span_id", "span_id"),
		sql.NewSimpleCol("any(duration)", "duration"),
		sql.NewSimpleCol("any(timestamp_ns)", "timestamp_ns")).
		From(sql.NewSimpleCol(table, "traces_idx")).
		AndWhere(sql.And(
			dateFrom, dateTo,
			tsFrom,
			tsTo,
		)).GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
