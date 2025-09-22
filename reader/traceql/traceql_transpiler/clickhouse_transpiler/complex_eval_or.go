package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type ComplexEvalOrPlanner struct {
	Operands []shared.SQLRequestPlanner
	Prefix   string
}

func (c ComplexEvalOrPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var err error
	selects := make([]sql.ISelect, len(c.Operands))
	for i, op := range c.Operands {
		selects[i], err = op.Process(ctx)
		if err != nil {
			return nil, err
		}
	}
	res := sql.NewSelect().
		Select(sql.NewRawObject("*")).
		From(sql.NewCol(&union{selects: selects}, c.Prefix+"a"))
	return res, nil
}
