package clickhouse_planner

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type PlannerKeep struct {
	Labels      []string
	Vals        []string
	LabelsCache **sql.With
	fpCache     **sql.With
	Main        shared.SQLRequestPlanner
}

func (k *PlannerKeep) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := k.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	cols, err := patchCol(main.GetSelect(), "labels", func(labels sql.SQLObject) (sql.SQLObject, error) {
		return &mapKeepFilter{
			col:    labels,
			labels: k.Labels,
			values: k.Vals,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	main.Select(cols...)
	return main, nil
}

type mapKeepFilter struct {
	col    sql.SQLObject
	labels []string
	values []string
}

func (m mapKeepFilter) String(ctx *sql.Ctx, options ...int) (string, error) {
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

func (m mapKeepFilter) genFilterFn(ctx *sql.Ctx, options ...int) (string, error) {
	clauses := make([]string, 0, len(m.labels)+2)

	// Preserve synthetic parser-error labels regardless of the keep list,
	// mirroring the in-memory KeepPlanner and Loki semantics. Otherwise a
	// `| json | keep <label>` chain would silently strip the error context
	// emitted by the parser on a failed line.
	for _, name := range []string{shared.ErrorLabel, shared.ErrorDetailsLabel} {
		q, err := sql.NewStringVal(name).String(ctx, options...)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, fmt.Sprintf("k==%s", q))
	}

	for i, l := range m.labels {
		quoteKey, err := sql.NewStringVal(l).String(ctx, options...)
		if err != nil {
			return "", err
		}
		if m.values[i] == "" {
			clauses = append(clauses, fmt.Sprintf("k==%s", quoteKey))
			continue
		}
		quoteVal, err := sql.NewStringVal(m.values[i]).String(ctx, options...)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, fmt.Sprintf("(k, v)==(%s, %s)", quoteKey, quoteVal))
	}
	return fmt.Sprintf("(k,v) -> %s",
		strings.Join(clauses, " or ")), nil
}
