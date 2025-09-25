package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type StepFixPlanner struct {
	Main     shared.SQLRequestPlanner
	Duration time.Duration
}

func (s *StepFixPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	if s.Duration.Nanoseconds() >= ctx.Step.Nanoseconds() {
		return main, nil
	}

	witMain := sql.NewWith(main, "pre_step_fix")
	res := sql.NewSelect().With(witMain).
		Select(
			sql.NewSimpleCol(
				fmt.Sprintf("intDiv(pre_step_fix.timestamp_ns, %d) * %[1]d", ctx.Step.Nanoseconds()),
				"timestamp_ns"),
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol(`''`, "string"),
			sql.NewSimpleCol("argMin(pre_step_fix.value, pre_step_fix.timestamp_ns)", "value"),
		).From(sql.NewWithRef(witMain)).
		GroupBy(sql.NewRawObject("timestamp_ns"), sql.NewRawObject("fingerprint"))

	if hasColumn(main.GetSelect(), "labels") {
		res.Select(append(res.GetSelect(), sql.NewSimpleCol("any(labels)", "labels"))...)
	}
	return res, nil
}
