package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SelectTagsPlanner reads the attribute keys of the result spans of Result,
// an evaluated spanset expression.
type SelectTagsPlanner struct {
	Result shared.SQLRequestPlanner
	Prefix string
}

func (s *SelectTagsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return selectAttrs(ctx, s.Result, s.Prefix, "key")
}

// selectAttrs reads the distinct values of col, key or val, of the attribute
// rows of result's spans that pass where, in order and up to ctx.Limit.
func selectAttrs(ctx *shared.PlannerContext, result shared.SQLRequestPlanner, prefix string, col string,
	where ...sql.SQLCondition) (sql.ISelect, error) {
	withResult, withIds, err := resultSpanIds(ctx, result, prefix)
	if err != nil {
		return nil, err
	}
	dateFrom, dateTo := timerange.SpanDate("date", ctx.From, ctx.To)
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	res := sql.NewSelect().
		With(withResult, withIds).
		Select(sql.NewSimpleCol(col, col)).
		From(sql.NewSimpleCol(attrsTable(ctx), "traces_idx")).
		AndWhere(
			dateFrom, dateTo,
			tsFrom,
			tsTo,
			inSubquery(ctx, sql.NewRawObject("traces_idx.trace_id"),
				sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(withIds))),
			inSubquery(ctx, sql.NewRawObject("(traces_idx.trace_id, traces_idx.span_id)"), sql.NewWithRef(withIds))).
		AndWhere(where...).
		GroupBy(sql.NewRawObject(col)).
		OrderBy(sql.NewOrderBy(sql.NewRawObject(col), sql.ORDER_BY_DIRECTION_ASC))
	if ctx.Limit > 0 {
		res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}
