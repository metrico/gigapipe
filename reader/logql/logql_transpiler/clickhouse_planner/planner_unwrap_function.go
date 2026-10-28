package clickhouse_planner

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type UnwrapFunctionPlanner struct {
	Main       shared.SQLRequestPlanner
	Func       string
	Duration   time.Duration
	WithLabels bool
	Offset     time.Duration
}

func (u *UnwrapFunctionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := readWindowRows(ctx, u.Main, u.Duration, u.Offset)
	if err != nil {
		return nil, err
	}

	var fn windowFn
	switch u.Func {
	case "rate":
		fn = windowFn{summand: "value", finite: true, final: perSecond(u.Duration)}
	case "sum_over_time":
		fn = windowFn{summand: "value", finite: true, final: plainSum}
	case "avg_over_time":
		fn = windowFn{summand: "value", finite: true, final: mean}
	case "max_over_time":
		fn = windowFn{agg: "max", args: "value"}
	case "min_over_time":
		fn = windowFn{agg: "min", args: "value"}
	case "first_over_time":
		fn = windowFn{agg: "argMin", args: "value, timestamp_ns"}
	case "last_over_time":
		fn = windowFn{agg: "argMax", args: "value, timestamp_ns"}
	case "stdvar_over_time":
		fn = windowFn{agg: "varPop", args: "value"}
	case "stddev_over_time":
		fn = windowFn{agg: "stddevPop", args: "value"}
	default:
		return nil, &shared.NotSupportedError{Msg: u.Func + " is not supported"}
	}
	return windowSelect(ctx, main, fn, u.Duration, true)
}
