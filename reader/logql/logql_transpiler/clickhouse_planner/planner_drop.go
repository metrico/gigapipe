package clickhouse_planner

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type PlannerDrop struct {
	Labels             []string
	Vals               []string
	UseTimeSeriesTable bool
	LabelsCache        **sql.With
	fpCache            **sql.With
	Main               shared.SQLRequestPlanner
}

func (d *PlannerDrop) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := d.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	cols, err := patchCol(main.GetSelect(), "labels", func(labels sql.SQLObject) (sql.SQLObject, error) {
		return &mapDropFilter{
			col:    labels,
			labels: d.Labels,
			values: d.Vals,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	main.Select(cols...)
	return main, nil
}

type mapDropFilter struct {
	col    sql.SQLObject
	labels []string
	values []string
}

func (m mapDropFilter) String(ctx *sql.Ctx, options ...int) (string, error) {
	str, err := m.col.String(ctx, options...)
	if err != nil {
		return "", err
	}
	fn, err := m.genFilterFn(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("mapFilter(%s, %s)", fn, str), nil
}

func (m mapDropFilter) genFilterFn(ctx *sql.Ctx, options ...int) (string, error) {
	clauses := make([]string, len(m.labels))
	for i, l := range m.labels {
		quoteKey, err := sql.NewStringVal(l).String(ctx, options...)
		if err != nil {
			return "", err
		}
		if m.values[i] == "" {
			clauses[i] = fmt.Sprintf("k!=%s", quoteKey)
			continue
		}
		quoteVal, err := sql.NewStringVal(m.values[i]).String(ctx, options...)
		if err != nil {
			return "", err
		}
		clauses[i] = fmt.Sprintf("(k, v)!=(%s, %s)", quoteKey, quoteVal)
	}
	return fmt.Sprintf("(k,v) -> %s",
		strings.Join(clauses, " and ")), nil
}
