package clickhouse_planner

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type ByWithoutPlanner struct {
	Main               shared.SQLRequestPlanner
	Labels             []string
	By                 bool
	UseTimeSeriesTable bool
	LabelsCache        **sql.With
	FPCache            **sql.With
}

func (b *ByWithoutPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := b.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if b.UseTimeSeriesTable {
		return b.processTSTable(ctx, main)
	}
	return b.processSimple(ctx, main)
}

func (b *ByWithoutPlanner) processSimple(ctx *shared.PlannerContext,
	main sql.ISelect) (sql.ISelect, error) {
	withMain := sql.NewWith(main, fmt.Sprintf("pre_by_without_%d", ctx.Id()))
	return sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("cityHash64(labels)", "fingerprint"),
			sql.NewCol(&byWithoutFilterCol{
				labelsCol: sql.NewRawObject(withMain.GetAlias() + ".labels"),
				labels:    b.Labels,
				by:        b.By,
			}, "labels"),
			sql.NewSimpleCol("string", "string"),
			sql.NewSimpleCol("value", "value")).
		From(sql.NewWithRef(withMain)), nil
}

func (b *ByWithoutPlanner) processTSTable(ctx *shared.PlannerContext,
	main sql.ISelect) (sql.ISelect, error) {
	var labels sql.ISelect
	if b.LabelsCache != nil && *b.LabelsCache != nil {
		labels = sql.NewSelect().Select(
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol("cityHash64(labels)", "new_fingerprint"),
			sql.NewCol(&byWithoutFilterCol{
				labelsCol: sql.NewRawObject("a.labels"),
				labels:    b.Labels,
				by:        b.By,
			}, "labels"),
		).From(sql.NewCol(sql.NewWithRef(*b.LabelsCache), "a"))
	} else {
		from, err := labelsFromScratch(ctx, *b.FPCache)
		if err != nil {
			return nil, err
		}
		cols, err := patchCol(from.GetSelect(), "labels", func(object sql.SQLObject) (sql.SQLObject, error) {
			return &byWithoutFilterCol{
				labelsCol: object,
				labels:    b.Labels,
				by:        b.By,
			}, nil
		})
		if err != nil {
			return nil, err
		}
		labels = from.Select(append(cols, sql.NewSimpleCol("cityHash64(labels)", "new_fingerprint"))...)
	}

	withLabels := sql.NewWith(labels, fmt.Sprintf("labels_%d", ctx.Id()))

	if b.LabelsCache != nil {
		*b.LabelsCache = withLabels
	}

	withMain := sql.NewWith(main, fmt.Sprintf("pre_without_%d", ctx.Id()))

	joinType := "ANY LEFT "
	if ctx.IsCluster {
		joinType = "GLOBAL ANY LEFT "
	}

	return sql.NewSelect().With(withMain, withLabels).
		Select(
			sql.NewSimpleCol(withLabels.GetAlias()+".new_fingerprint", "fingerprint"),
			sql.NewSimpleCol(withMain.GetAlias()+".timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol(withMain.GetAlias()+".value", "value"),
			sql.NewSimpleCol("''", "string"),
			sql.NewSimpleCol(withLabels.GetAlias()+".labels", "labels"),
		).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin(joinType, sql.NewWithRef(withLabels),
			sql.Eq(
				sql.NewRawObject(withMain.GetAlias()+".fingerprint"),
				sql.NewRawObject(withLabels.GetAlias()+".fingerprint"),
			))), nil
}

type byWithoutFilterCol struct {
	labelsCol sql.SQLObject
	labels    []string
	by        bool
}

func (b *byWithoutFilterCol) String(ctx *sql.Ctx, opts ...int) (string, error) {
	str, err := b.labelsCol.String(ctx, opts...)
	if err != nil {
		return "", err
	}

	sqlLabels := make([]string, len(b.labels))
	for i, label := range b.labels {
		sqlLabels[i], err = sql.NewStringVal(label).String(ctx, opts...)
		if err != nil {
			return "", err
		}
	}

	fn := "IN"
	if !b.by {
		fn = "NOT IN"
	}

	return fmt.Sprintf("mapFilter((k,v) -> k %s (%s), %s)", fn, strings.Join(sqlLabels, ","), str), nil
}
