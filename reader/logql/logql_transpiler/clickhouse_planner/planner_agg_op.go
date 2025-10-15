package clickhouse_planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type AggOpPlanner struct {
	Main       shared.SQLRequestPlanner
	Func       string
	WithLabels bool
}

func (b *AggOpPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := b.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	var val sql.SQLObject
	switch b.Func {
	case "sum":
		val = sql.NewRawObject("sum(lra_main.value)")
	case "min":
		val = sql.NewRawObject("min(lra_main.value)")
	case "max":
		val = sql.NewRawObject("max(lra_main.value)")
	case "avg":
		val = sql.NewRawObject("avg(lra_main.value)")
	case "stddev":
		val = sql.NewRawObject("stddevPop(lra_main.value)")
	case "stdvar":
		val = sql.NewRawObject("varPop(lra_main.value)")
	case "count":
		val = sql.NewRawObject("count()")
	}

	withMain := sql.NewWith(main, "lra_main")

	main = sql.NewSelect().With(withMain).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewCol(val, "value"),
		sql.NewSimpleCol("lra_main.timestamp_ns", "timestamp_ns"),
		sql.NewSimpleCol("''", "string"),
	).From(sql.NewWithRef(withMain)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ns"))
	if b.WithLabels {
		main.Select(append(main.GetSelect(), sql.NewSimpleCol("any(lra_main.labels)", "labels"))...)
	}
	return main, nil
}
