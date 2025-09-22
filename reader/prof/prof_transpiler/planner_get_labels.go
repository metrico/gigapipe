package prof_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/prof/prof_parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type GetLabelsPlanner struct {
	FP        shared.SQLRequestPlanner
	GroupBy   []string
	Selectors []prof_parser.Selector
}

func (g *GetLabelsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := g.FP.Process(ctx)
	if err != nil {
		return nil, err
	}

	matchers, err := (&StreamSelectorPlanner{Selectors: g.Selectors}).getMatchers()
	if err != nil {
		return nil, err
	}

	newFpCol := sql.NewSimpleCol("fingerprint", "new_fingerprint")
	tagsCol := sql.NewSimpleCol("arraySort(p.tags)", "tags")
	if len(g.GroupBy) > 0 {
		newFpCol = sql.NewSimpleCol("cityHash64(tags)", "new_fingerprint")
		tagsCol = sql.NewCol(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			sqlGroupBy := make([]sql.SQLObject, len(g.GroupBy))
			for i, col := range g.GroupBy {
				sqlGroupBy[i] = sql.NewStringVal(col)
			}
			inTags := sql.NewIn(sql.NewRawObject("x.1"), sqlGroupBy...)
			strInTags, err := inTags.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("arrayFilter(x -> %s, p.tags)", strInTags), nil
		}), "tags")
	}

	withFp := sql.NewWith(fp, "fp")
	main := sql.NewSelect().
		With(withFp).
		Distinct(true).
		Select(
			sql.NewRawObject("fingerprint"),
			tagsCol,
			newFpCol).
		From(sql.NewCol(sql.NewRawObject(ctx.ProfilesSeriesTable), "p")).
		AndWhere(
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp)),
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.To))))
	if len(matchers.globalMatchers) > 0 {
		main = main.AndWhere(matchers.globalMatchers...)
	}
	return main, nil
}
