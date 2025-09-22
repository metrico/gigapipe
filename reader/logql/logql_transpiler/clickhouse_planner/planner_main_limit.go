package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MainLimitPlanner struct {
	Main shared.SQLRequestPlanner
}

func (m *MainLimitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if ctx.Limit == 0 {
		return req, nil
	}
	return req.Limit(sql.NewIntVal(ctx.Limit)), nil
}
