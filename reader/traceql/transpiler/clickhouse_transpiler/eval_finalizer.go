package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type EvalFinalizerPlanner struct {
	Main shared.SQLRequestPlanner
}

func (e *EvalFinalizerPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := e.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withPrefinal := sql.NewWith(main, "pre_final")
	main = sql.NewSelect().With(withPrefinal).
		Select(sql.NewSimpleCol("_count", "_count")).
		From(sql.NewWithRef(withPrefinal))
	return main, nil
}
