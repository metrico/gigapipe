package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type UnwrapPlanner struct {
	Main               shared.SQLRequestPlanner
	Label              string
	UseTimeSeriesTable bool
	labelsCache        **sql.With
	fpCache            **sql.With
}

func (u *UnwrapPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	mainReq, err := u.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if u.UseTimeSeriesTable {
		return u.processTimeSeries(ctx, mainReq)
	} else {
		return u.processSimple(ctx, mainReq)
	}
}

func (u *UnwrapPlanner) processSimple(_ *shared.PlannerContext, main sql.ISelect) (sql.ISelect, error) {
	sel := main.GetSelect()
	labels := getCol(main, "labels")
	strCol := getCol(main, "string")
	if labels == nil {
		return nil, fmt.Errorf("labels col not inited")
	}
	label := u.Label

	sel, err := patchCol(sel, "value", func(object sql.SQLObject) (sql.SQLObject, error) {
		return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			var strLabel string
			if u.Label != "_entry" {
				strLabels, err := labels.String(ctx, options...)
				if err != nil {
					return "", err
				}
				val, err := sql.NewStringVal(label).String(ctx, options...)
				if err != nil {
					return "", err
				}
				strLabel = fmt.Sprintf("%s[%s]", strLabels, val)
			} else {
				var err error
				strLabel, err = strCol.String(ctx, options...)
				if err != nil {
					return "", err
				}
			}
			return fmt.Sprintf("toFloat64OrZero(%s)", strLabel), nil
		}), nil
	})
	if err != nil {
		return nil, err
	}

	return main.Select(sel...), nil
}

func (u *UnwrapPlanner) processTimeSeries(ctx *shared.PlannerContext, main sql.ISelect) (sql.ISelect, error) {
	var from sql.SQLObject
	if *u.labelsCache != nil {
		from = sql.NewWithRef(*u.labelsCache)
	} else {
		var err error
		from, err = labelsFromScratch(ctx, *u.fpCache)
		if err != nil {
			return nil, err
		}
	}
	// TODO:  remove this
	subSelect := sql.NewSelect().Select(
		sql.NewRawObject("fingerprint"),
		sql.NewRawObject("labels"),
	).From(from)

	subSelect, err := u.processSimple(ctx, main)
	if err != nil {
		return nil, err
	}

	joinType := "ANY LEFT "
	if ctx.IsCluster {
		joinType = "GLOBAL ANY LEFT "
	}

	main.Select(append(main.GetSelect(),
		sql.NewSimpleCol("sub_value.value", "value"),
		sql.NewSimpleCol("sub_value.labels", "labels"))...).
		Join(sql.NewJoin(joinType, sql.NewCol(subSelect, "sub_value"),
			sql.Eq(sql.NewRawObject("fingerprint"), sql.NewRawObject("sub_value.fingerprint"))))
	return main, nil
}
