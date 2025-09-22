package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SelectTagsPlanner struct {
	Main shared.SQLRequestPlanner
}

func NewInitTagsPlanner() shared.SQLRequestPlanner {
	//TODO: add this to plugins
	/*p := plugins.GetInitIndexPlannerPlugin()
	if p != nil {
		return (*p)()
	}*/
	return &SelectTagsPlanner{}
}

func (i *SelectTagsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := i.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(main, "select_spans")

	preSelectTags := sql.NewSelect().Select(sql.NewRawObject("span_id")).From(sql.NewWithRef(withMain))
	withPreSelectTags := sql.NewWith(preSelectTags, "pre_select_tags")

	res := sql.NewSelect().
		With(withMain, withPreSelectTags).
		Select(sql.NewSimpleCol("key", "key")).
		From(sql.NewSimpleCol(ctx.TracesAttrsDistTable, "traces_idx")).
		AndWhere(sql.And(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("span_id"), sql.NewWithRef(withPreSelectTags)),
		)).GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id"))
	if ctx.Limit > 0 {
		res.OrderBy(sql.NewOrderBy(sql.NewRawObject("key"), sql.ORDER_BY_DIRECTION_ASC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}
