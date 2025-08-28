package clickhouse_planner

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

func (p *ParserPlanner) json(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := p.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	jsonPaths := make([][]string, len(p.Vals))
	for i, val := range p.Vals {
		jsonPaths[i], err = shared.JsonPathParamToArray(val)
		if err != nil {
			return nil, err
		}
	}

	sel, err := patchCol(req.GetSelect(), "labels", func(object sql.SQLObject) (sql.SQLObject, error) {
		return &sqlMapUpdate{
			object,
			&sqlJsonParser{
				col:    sql.NewRawObject("string"),
				labels: p.labels,
				paths:  jsonPaths,
			},
		}, nil
	})
	// if err != nil {
	// 	return nil, err
	// }

	return req.Select(sel...), nil
}

type sqlJsonParser struct {
	col    sql.SQLObject
	labels []string
	paths  [][]string
}

func (s *sqlJsonParser) String(ctx *sql.Ctx, opts ...int) (string, error) {
	strLabels := make([]string, len(s.labels))
	strVals := make([]string, len(s.labels))
	for i, l := range s.labels {
		var err error
		strLabels[i], err = (sql.NewStringVal(l)).String(ctx, opts...)
		if err != nil {
			return "", err
		}

		strVals[i], err = s.path2Sql(s.paths[i], ctx, opts...)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("mapFromArrays([%s], [%s])",
		strings.Join(strLabels, ","),
		strings.Join(strVals, ",")), nil
}

func (s *sqlJsonParser) path2Sql(path []string, ctx *sql.Ctx, opts ...int) (string, error) {
	colName, err := s.col.String(ctx, opts...)
	if err != nil {
		return "", err
	}

	res := make([]string, len(path))
	for i, part := range path {
		var err error
		res[i], err = (sql.NewStringVal(part)).String(ctx, opts...)
		if err != nil {
			return "", err
		}
	}
	partId := fmt.Sprintf("jp_%d", ctx.Id())

	return fmt.Sprintf(`if(JSONType(%[3]s, %[1]s as %[2]s) == 'String', `+
		`JSONExtractString(%[3]s, %[2]s), `+
		`JSONExtractRaw(%[3]s, %[2]s)`+
		`)`, strings.Join(res, ","), partId, colName), nil
}
