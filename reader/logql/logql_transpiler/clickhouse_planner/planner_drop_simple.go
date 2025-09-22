package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type PlannerDropSimple struct {
	Labels []string
	Vals   []string

	LabelsCache **sql.With
	FPCache     **sql.With

	Main shared.SQLRequestPlanner
}

func (d *PlannerDropSimple) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	mainReq, err := d.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(mainReq, fmt.Sprintf("pre_drop_%d", ctx.Id()))
	var labels sql.ISelect

	if d.LabelsCache != nil && *d.LabelsCache != nil {
		labels = sql.NewSelect().Select(
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol("cityHash64(labels)", "new_fingerprint"),
			sql.NewCol(&mapDropFilter{
				col:    sql.NewRawObject("a.labels"),
				labels: d.Labels,
				values: d.Vals,
			}, "labels"),
		).From(sql.NewCol(sql.NewWithRef(withMain), "a"))
	} else {
		labels, err = labelsFromScratch(ctx, *d.FPCache)
		if err != nil {
			return nil, err
		}
		sel, err := patchCol(labels.GetSelect(), "labels", func(c sql.SQLObject) (sql.SQLObject, error) {
			return &mapDropFilter{
				col:    c,
				labels: d.Labels,
				values: d.Vals,
			}, nil
		})
		if err != nil {
			return nil, err
		}
		sel = append(sel, sql.NewSimpleCol("cityHash64(labels)", "new_fingerprint"))
		labels.Select(sel...)
	}

	withLabels := sql.NewWith(labels, fmt.Sprintf("labels_%d", ctx.Id()))

	*d.LabelsCache = withLabels

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
