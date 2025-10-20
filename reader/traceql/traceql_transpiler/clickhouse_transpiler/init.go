package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/plugins"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
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
	return sql.NewSelect().Select(
		sql.NewSimpleCol("trace_id", "trace_id"),
		sql.NewSimpleCol("span_id", "span_id"),
		sql.NewSimpleCol("any(duration)", "duration"),
		sql.NewSimpleCol("any(timestamp_ns)", "timestamp_ns")).
		From(sql.NewSimpleCol(table, "traces_idx")).
		AndWhere(sql.And(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
		)).GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)), nil
}
