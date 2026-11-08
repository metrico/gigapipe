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

// RowAttrsPlanner reads the distinct values of Col, key or val, of the
// attribute rows in range that pass Where.
type RowAttrsPlanner struct {
	Col   string
	Where []sql.SQLCondition
}

func (r *RowAttrsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return selectRows(ctx, r.Col, r.Where...), nil
}

// selectAttrs reads the distinct values of col, key or val, of the attribute
// rows of the spans listed in SearchResultTable that pass where, in order and
// up to ctx.Limit.
func selectAttrs(ctx *shared.PlannerContext, prefix string, col string,
	where ...sql.SQLCondition) sql.ISelect {
	withRows, withTraceIds, withSpanIds := idsOf(SearchResultTable, prefix)
	conds := append([]sql.SQLCondition{
		plainIn(sql.NewRawObject("traces_idx.trace_id"), sql.NewWithRef(withTraceIds)),
		plainIn(sql.NewRawObject("(traces_idx.trace_id, traces_idx.span_id)"), sql.NewWithRef(withSpanIds)),
	}, where...)
	return selectRows(ctx, col, conds...).With(withRows, withTraceIds, withSpanIds)
}

// selectRows reads the distinct values of col of the attribute rows in range
// that pass where, in order and up to ctx.Limit.
func selectRows(ctx *shared.PlannerContext, col string, where ...sql.SQLCondition) sql.ISelect {
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	res := sql.NewSelect().
		Select(sql.NewSimpleCol(col, col)).
		From(sql.NewSimpleCol(attrsTable(ctx), "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			tsFrom,
			tsTo).
		AndWhere(where...).
		GroupBy(sql.NewRawObject(col)).
		OrderBy(sql.NewOrderBy(sql.NewRawObject(col), sql.ORDER_BY_DIRECTION_ASC))
	keepPortion(ctx, res)
	if ctx.Limit > 0 {
		res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res
}
