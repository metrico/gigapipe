package transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type LabelNamesPlanner struct {
	GenericLabelsPlanner
}

func (l *LabelNamesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return l._process(ctx, "key")
}
