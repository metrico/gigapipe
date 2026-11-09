package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SelectValuesPlanner reads the values of attribute Key of the spans listed in
// SearchResultTable.
type SelectValuesPlanner struct {
	Prefix string
	Key    string
}

func (s *SelectValuesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return selectAttrs(ctx, s.Prefix, "val", sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(s.Key))), nil
}
