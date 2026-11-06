package clickhouse_planner

import (
	"fmt"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type QuantilePlanner struct {
	Main     shared.SQLRequestPlanner
	Param    float64
	Duration time.Duration
	Offset   time.Duration
}

func (p *QuantilePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := readWindowRows(ctx, p.Main, p.Duration, p.Offset)
	if err != nil {
		return nil, err
	}
	fn := windowFn{
		agg:    "quantile",
		params: fmt.Sprintf("(%s)", strconv.FormatFloat(p.Param, 'f', -1, 64)),
		args:   "value",
	}
	return windowSelect(ctx, main, fn, p.Duration, hasColumn(main.GetSelect(), "labels"))
}
