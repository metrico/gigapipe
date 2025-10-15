package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type IndexGroupByPlanner struct {
	Main   shared.SQLRequestPlanner
	Prefix string
}

func (i *IndexGroupByPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := i.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(main, i.Prefix+"index_search")
	return sql.NewSelect().
		With(withMain).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("groupArray(100)(span_id)", "span_id")).
		From(sql.NewWithRef(withMain)).
		GroupBy(sql.NewRawObject("trace_id")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject(
				fmt.Sprintf("max(%sindex_search.timestamp_ns)", i.Prefix)),
				sql.ORDER_BY_DIRECTION_DESC),
		), nil
}
