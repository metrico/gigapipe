package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MetricPlanner struct {
	Main shared.SQLRequestPlanner
	Fn   string
}

func (m *MetricPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(main, "pre_metric")
	fn, err := m.fn(ctx)
	if err != nil {
		return nil, err
	}
	var indexSearch *sql.With
	for _, w := range main.GetWith() {
		if w.GetAlias() == "index_search" {
			indexSearch = w
		}
	}
	return sql.NewSelect().With(withMain).Select(
		sql.NewSimpleCol(
			fmt.Sprintf("intDiv(timestamp_ns, %d) * %d",
				ctx.Step.Nanoseconds(), ctx.Step.Nanoseconds()/1000000),
			"timestamp_ms"),
		sql.NewCol(fn, "value")).
		From(sql.NewWithRef(indexSearch)).
		GroupBy(sql.NewRawObject("timestamp_ms")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)), nil
}

func (m *MetricPlanner) fn(ctx *shared.PlannerContext) (sql.SQLObject, error) {
	switch m.Fn {
	case "rate":
		return sql.NewRawObject(
			fmt.Sprintf("toFloat64(count()) / %f", float64(ctx.Step.Milliseconds())/1000)), nil
	case "count_over_time":
		return sql.NewRawObject("count()"), nil
	/*case "sum_over_time"
	case "min_over_time"
	case "max_over_time"
	case "avg_over_time"*/
	default:
		return nil, fmt.Errorf("unsupported function: %s", m.Fn)

	}
}
