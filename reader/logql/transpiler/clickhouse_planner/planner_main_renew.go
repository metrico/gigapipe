package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MainRenewPlanner struct {
	Main      shared.SQLRequestPlanner
	UseLabels bool
}

func (m *MainRenewPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(main, fmt.Sprintf("subsel_%d", ctx.Id()))

	req := sql.NewSelect().
		With(withMain).
		Select(
			sql.NewSimpleCol("samples.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("samples.fingerprint", "fingerprint")).
		From(sql.NewCol(sql.NewWithRef(withMain), "samples"))

	if m.UseLabels {
		req.Select(append(req.GetSelect(), sql.NewSimpleCol("samples.labels", "labels"))...)
	}

	req.Select(append(req.GetSelect(),
		sql.NewSimpleCol("samples.string", "string"),
		sql.NewSimpleCol("samples.value", "value"))...)
	return req, nil
}
