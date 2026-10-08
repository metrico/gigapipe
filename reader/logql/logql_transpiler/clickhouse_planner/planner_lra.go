package clickhouse_planner

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type LRAPlanner struct {
	Main       shared.SQLRequestPlanner
	Duration   time.Duration
	Func       string
	WithLabels bool
	Offset     time.Duration
}

func (l *LRAPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := readWindowRows(ctx, l.Main, l.Duration, l.Offset)
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

	var fn windowFn
	switch l.Func {
	case "rate":
		fn = windowFn{summand: "1", final: perSecond(l.Duration)}
	case "count_over_time":
		fn = windowFn{summand: "1", final: plainSum}
	case "bytes_rate", "bytes_over_time":
		fn = windowFn{summand: "length(_string)", final: perSecond(l.Duration)}
	default:
		return nil, &shared.NotSupportedError{Msg: l.Func + " is not supported"}
	}
	return windowSelect(ctx, main, fn, l.Duration, l.WithLabels)
}
