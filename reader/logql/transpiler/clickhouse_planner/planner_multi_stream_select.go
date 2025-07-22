package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MultiStreamSelectPlanner struct {
	Mains []shared.SQLRequestPlanner
}

func (m *MultiStreamSelectPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if len(m.Mains) == 1 {
		return m.Mains[0].Process(ctx)
	}
	var err error
	selects := make([]sql.ISelect, len(m.Mains))
	for i, main := range m.Mains {
		selects[i], err = main.Process(ctx)
		if err != nil {
			return nil, err
		}
	}
	return &UnionAll{selects[0], selects[1:]}, nil
}
