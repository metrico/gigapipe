package prof_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type TimeSeriesDistinctPlanner struct {
	/* TimeSeriesSelectPlanner or union */
	Main shared.SQLRequestPlanner
}

func (t TimeSeriesDistinctPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := t.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(main, "pre_distinct")
	return sql.NewSelect().
		With(withMain).
		Distinct(true).
		Select(sql.NewSimpleCol("tags", "tags"),
			sql.NewSimpleCol("type_id", "type_id"),
			sql.NewSimpleCol("__sample_types_units", "__sample_types_units")).
		From(sql.NewWithRef(withMain)), nil
}
