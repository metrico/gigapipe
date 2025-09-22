package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SelectValuesRequestPlanner struct {
	SelectTagsPlanner
	Key string
}

func (i *SelectValuesRequestPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := i.SelectTagsPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	main.Select(sql.NewSimpleCol("val", "val")).
		AndWhere(sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(i.Key)))
	if ctx.Limit > 0 {
		main.OrderBy(sql.NewOrderBy(sql.NewRawObject("val"), sql.ORDER_BY_DIRECTION_ASC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}
	return main, nil
}
