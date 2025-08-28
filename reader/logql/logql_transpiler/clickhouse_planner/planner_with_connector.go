package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type WithConnectorPlanner struct {
	Main  shared.SQLRequestPlanner
	With  shared.SQLRequestPlanner
	Alias string

	ProcessFn func(q sql.ISelect, w *sql.With) (sql.ISelect, error)

	WithCache **sql.With
}

func (w *WithConnectorPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := w.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	var with *sql.With
	if w.WithCache != nil && *w.WithCache != nil {
		with = *w.WithCache
	} else {
		withReq, err := w.With.Process(ctx)
		if err != nil {
			return nil, err
		}
		with = sql.NewWith(withReq, w.Alias)
		*w.WithCache = with
	}

	return w.ProcessFn(main.With(with), with)
}
