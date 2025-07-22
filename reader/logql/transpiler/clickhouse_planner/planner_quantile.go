package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type QuantilePlanner struct {
	Main     shared.SQLRequestPlanner
	Param    float64
	Duration time.Duration
}

func (p *QuantilePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := p.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	quantA := sql.NewWith(main, "quant_a")

	hasLabels := hasColumn(main.GetSelect(), "labels")

	res := sql.NewSelect().
		With(quantA).
		Select(
			sql.NewSimpleCol("quant_a.fingerprint", "fingerprint"),
			sql.NewSimpleCol(fmt.Sprintf("intDiv(quant_a.timestamp_ns, %d) * %[1]d",
				p.Duration.Nanoseconds()), "timestamp_ns"),
			sql.NewSimpleCol(fmt.Sprintf("quantile(%f)(value)", p.Param), "value")).
		From(sql.NewWithRef(quantA)).
		GroupBy(sql.NewRawObject("timestamp_ns"), sql.NewRawObject("fingerprint"))

	if hasLabels {
		res.Select(append(res.GetSelect(), sql.NewSimpleCol("any(quant_a.labels)", "labels"))...)
	}

	return res, nil
}
