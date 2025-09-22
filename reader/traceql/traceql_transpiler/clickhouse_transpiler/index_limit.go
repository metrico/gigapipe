package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type IndexLimitPlanner struct {
	Main shared.SQLRequestPlanner
}

func (i *IndexLimitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := i.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	if ctx.Limit == 0 {
		return main, nil
	}

	return main.Limit(sql.NewIntVal(ctx.Limit)), nil
}
