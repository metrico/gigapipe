package transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type UnionAllPlanner struct {
	Mains []shared.SQLRequestPlanner
}

func (u *UnionAllPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if len(u.Mains) == 0 {
		return nil, fmt.Errorf("no planners provided for UNION ALL operator")
	}
	mains := make([]sql.ISelect, len(u.Mains))
	var err error
	for i, p := range u.Mains {
		mains[i], err = p.Process(ctx)
		if err != nil {
			return nil, err
		}
	}
	return &unionAll{mains[0], mains[1:]}, nil
}

type unionAll struct {
	sql.ISelect
	subSelects []sql.ISelect
}

func (u *unionAll) String(ctx *sql.Ctx, options ...int) (string, error) {
	strSubSelects := make([]string, len(u.subSelects)+1)
	var err error
	strSubSelects[0], err = u.ISelect.String(ctx, options...)
	if err != nil {
		return "", err
	}
	for i, s := range u.subSelects {
		strSubSelects[i+1], err = s.String(ctx, options...)
		if err != nil {
			return "", err
		}
	}
	return "(" + strings.Join(strSubSelects, ") UNION ALL (") + ")", nil
}
