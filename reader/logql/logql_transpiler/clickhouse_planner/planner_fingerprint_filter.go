package clickhouse_planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type FingerprintFilterPlanner struct {
	FingerprintsSelectPlanner shared.SQLRequestPlanner
	MainRequestPlanner        shared.SQLRequestPlanner

	FingerprintSelectWithCache **sql.With
}

func (s *FingerprintFilterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	withPlanner := WithConnectorPlanner{
		Main:  s.MainRequestPlanner,
		With:  s.FingerprintsSelectPlanner,
		Alias: "fp_sel",
		ProcessFn: func(q sql.ISelect, w *sql.With) (sql.ISelect, error) {
			return q.AndWhere(sql.NewIn(sql.NewRawObject("samples.fingerprint"), sql.NewWithRef(w))), nil
		},
		WithCache: s.FingerprintSelectWithCache,
	}
	return withPlanner.Process(ctx)
}
