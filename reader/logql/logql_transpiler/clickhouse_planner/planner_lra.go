package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type LRAPlanner struct {
	Main       shared.SQLRequestPlanner
	Duration   time.Duration
	Func       string
	WithLabels bool
}

func (l *LRAPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	cols := main.GetSelect()
	for i, c := range cols {
		_c, ok := c.(sql.Aliased)
		if !ok {
			continue
		}
		if _c.GetAlias() == "string" {
			cols[i] = sql.NewCol(_c.GetExpr(), "_string")
		}
	}

	var col sql.SQLObject
	switch l.Func {
	case "rate":
		col = sql.NewRawObject(fmt.Sprintf("toFloat64(COUNT()) / %f",
			float64(l.Duration.Milliseconds())/1000))
	case "count_over_time":
		col = sql.NewRawObject("toFloat64(COUNT())")
	case "bytes_rate":
		col = sql.NewRawObject(fmt.Sprintf("toFloat64(sum(length(_string))) / %f",
			float64(l.Duration.Milliseconds())/1000))
	case "bytes_over_time":
		col = sql.NewRawObject(fmt.Sprintf("toFloat64(sum(length(_string))) / %f",
			float64(l.Duration.Milliseconds())/1000))
	}

	withAgg := sql.NewWith(main, "agg_a")
	res := sql.NewSelect().With(withAgg).
		Select(
			sql.NewSimpleCol(
				fmt.Sprintf("intDiv(time_series.timestamp_ns, %d) * %[1]d", l.Duration.Nanoseconds()),
				"timestamp_ns",
			),
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol(`''`, "string"),
			sql.NewCol(col, "value"),
		).
		From(sql.NewCol(sql.NewWithRef(withAgg), "time_series")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ns"))
	if l.WithLabels {
		res.Select(append(res.GetSelect(), sql.NewSimpleCol("any(labels)", "labels"))...)
	}
	return res, nil
}
