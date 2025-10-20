package prof_transpiler

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type LabelNamesPlanner struct {
	GenericLabelsPlanner
}

func (l *LabelNamesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return l._process(ctx, "key")
}
