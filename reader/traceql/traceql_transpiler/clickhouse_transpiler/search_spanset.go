package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// DefaultSpansPerSpanSet is the spanset span cap when the request sets none.
const DefaultSpansPerSpanSet = 3

// SearchSpansetPlanner groups the result span rows into spansets, one per
// trace and group key, each listing its earliest-starting spss spans (ties by
// span id) and matched, the count of all. A trace lists its spansets by group
// key; traces come newest-span first.
type SearchSpansetPlanner struct {
	Main shared.SQLRequestPlanner
}

func (s *SearchSpansetPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	spss := ctx.SpansPerSpanSet
	if spss <= 0 {
		spss = DefaultSpansPerSpanSet
	}

	withSpans := sql.NewWith(main, "result_spans")
	withSpansets := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("result_spans.trace_id", "trace_id"),
			sql.NewSimpleCol("result_spans.grp", "grp"),
			sql.NewSimpleCol(fmt.Sprintf(
				"arrayMap(x -> x.2, arraySlice(arraySort(groupUniqArray((result_spans.timestamp_ns, result_spans.span_id))), 1, %d))", spss),
				"span_ids"),
			sql.NewSimpleCol("toInt64(count(DISTINCT result_spans.span_id))", "matched"),
			sql.NewSimpleCol("any(result_spans.attrs)", "attrs"),
			sql.NewSimpleCol("max(result_spans.timestamp_ns)", "newest")).
		From(sql.NewWithRef(withSpans)).
		GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("grp")), "result_spansets")
	res := sql.NewSelect().
		With(withSpans, withSpansets).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("arrayFlatten(arrayMap(x -> x.2, spansets))", "span_id"),
			sql.NewSimpleCol("arraySort(x -> x.1, groupArray((grp, span_ids, matched, attrs)))", "spansets")).
		From(sql.NewWithRef(withSpansets)).
		GroupBy(sql.NewRawObject("trace_id")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("max(newest)"), sql.ORDER_BY_DIRECTION_DESC),
			sql.NewOrderBy(sql.NewRawObject("trace_id"), sql.ORDER_BY_DIRECTION_DESC))
	return res, nil
}
