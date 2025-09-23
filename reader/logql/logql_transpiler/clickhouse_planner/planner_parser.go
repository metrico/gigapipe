package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type ParserPlanner struct {
	Op     string
	labels []string
	Vals   []string
	Main   shared.SQLRequestPlanner
}

func (p *ParserPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var (
		req sql.ISelect
		err error
	)
	switch p.Op {
	case "regexp":
		req, err = p.regexp(ctx)
	case "json":
		req, err = p.json(ctx)
	default:
		return nil, &shared.NotSupportedError{Msg: fmt.Sprintf("%s not supported", p.Op)}
	}
	if err != nil {
		return nil, err
	}

	sel, err := patchCol(req.GetSelect(), "fingerprint", func(object sql.SQLObject) (sql.SQLObject, error) {
		return sql.NewRawObject(`cityHash64(arraySort(arrayZip(mapKeys(labels),mapValues(labels))))`), nil
	})
	if err != nil {
		return nil, err
	}

	return req.Select(sel...), nil
}
