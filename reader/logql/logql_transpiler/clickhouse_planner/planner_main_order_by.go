package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MainOrderByPlanner struct {
	Cols []string
	Main shared.SQLRequestPlanner
}

func (m *MainOrderByPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	dir := sql.ORDER_BY_DIRECTION_DESC
	if ctx.OrderASC {
		dir = sql.ORDER_BY_DIRECTION_ASC
	}

	cols := make([]sql.SQLObject, len(m.Cols))
	for i, c := range m.Cols {
		cols[i] = sql.NewOrderBy(sql.NewRawObject(c), dir)
	}
	return req.OrderBy(cols...), nil
}
