package transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MergeAggregatedPlanner struct {
	// MergeJoinedPlanner, potentially having "WITH raw as (MergeRawPlanner)"
	Main shared.SQLRequestPlanner
}

func (m *MergeAggregatedPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(main, "joined")
	res := sql.NewSelect().
		With(withMain).
		Select(
			sql.NewSimpleCol("(select groupArray(tree) from joined)", "_tree"),
			sql.NewSimpleCol("(select groupUniqArrayArray(functions) from raw )", "_functions"))
	return res, nil
}
