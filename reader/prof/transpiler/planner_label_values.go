package transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type LabelValuesPlanner struct {
	GenericLabelsPlanner
	Label string
}

func (l *LabelValuesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	sel, err := l._process(ctx, "val")
	if err != nil {
		return nil, err
	}
	sel = sel.AndWhere(sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(l.Label)))
	return sel, nil
}
