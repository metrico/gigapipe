package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SelectTagsPlanner reads the attribute keys of the spans listed in
// SearchResultTable.
type SelectTagsPlanner struct {
	Prefix string
}

func (s *SelectTagsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return selectAttrs(ctx, s.Prefix, "key"), nil
}

// selectAttrs reads the distinct values of col, key or val, of the attribute
// rows of the spans listed in SearchResultTable that pass where, in order and
// up to ctx.Limit.
func selectAttrs(ctx *shared.PlannerContext, prefix string, col string,
	where ...sql.SQLCondition) sql.ISelect {
	withRows, withTraceIds, withSpanIds := idsOf(SearchResultTable, prefix)
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	res := sql.NewSelect().
		With(withRows, withTraceIds, withSpanIds).
		Select(sql.NewSimpleCol(col, col)).
		From(sql.NewSimpleCol(attrsTable(ctx), "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			tsFrom,
			tsTo,
			plainIn(sql.NewRawObject("traces_idx.trace_id"), sql.NewWithRef(withTraceIds)),
			plainIn(sql.NewRawObject("(traces_idx.trace_id, traces_idx.span_id)"), sql.NewWithRef(withSpanIds))).
		AndWhere(where...).
		GroupBy(sql.NewRawObject(col)).
		OrderBy(sql.NewOrderBy(sql.NewRawObject(col), sql.ORDER_BY_DIRECTION_ASC))
	if ctx.Limit > 0 {
		res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res
}
