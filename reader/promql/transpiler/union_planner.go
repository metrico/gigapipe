package transpiler

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/prometheus/prometheus/storage"
)

type UnionPlanner struct {
	Main1 shared.SQLRequestPlanner
	Main2 shared.SQLRequestPlanner
	Hints *storage.SelectHints
}

func (u *UnionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main1, err := u.Main1.Process(ctx)
	if err != nil {
		return nil, err
	}
	main2, err := u.Main2.Process(ctx)
	if err != nil {
		return nil, err
	}
	union := &clickhouse_planner.UnionAll{
		ISelect:  main1,
		Anothers: []sql.ISelect{main2},
	}

	res := sql.NewSelect().
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol((&DownsampleHintsPlanner{}).getValueFinalize(u.Hints.Func), "value"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		).From(sql.NewCol(
		sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			str, err := union.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s)", str), nil
		}), "samples_union")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")).
		OrderBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms"))
	return res, nil
}
