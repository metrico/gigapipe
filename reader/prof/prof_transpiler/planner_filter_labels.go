package prof_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type FilterLabelsPlanner struct {
	/* SelectTimeSeriesPlanner */
	Main   shared.SQLRequestPlanner
	Labels []string
}

func (f *FilterLabelsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := f.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if len(f.Labels) == 0 {
		return main, nil
	}

	withMain := sql.NewWith(main, "pre_label_filter")

	filterTagsCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		sqlLabels := make([]sql.SQLObject, len(f.Labels))
		for i, label := range f.Labels {
			sqlLabels[i] = sql.NewStringVal(label)
		}
		cond := sql.NewIn(sql.NewRawObject("x.1"), sqlLabels...)
		strCond, err := cond.String(ctx, options...)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("arrayFilter(x -> %s, tags)", strCond), nil
	})

	res := sql.NewSelect().
		With(withMain).
		Select(sql.NewCol(filterTagsCol, "tags"),
			sql.NewSimpleCol("type_id", "type_id"),
			sql.NewSimpleCol("__sample_types_units", "__sample_types_units")).
		From(sql.NewWithRef(withMain))
	return res, nil
}
