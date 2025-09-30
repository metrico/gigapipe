package planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type DownsampleValuesPlanner struct {
	ValuesPlanner
}

func (d *DownsampleValuesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := d.ValuesPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}
	sel := req.GetSelect()
	for i, s := range sel {
		_s := s.(sql.Aliased)
		if _s != nil && _s.GetAlias() == "val" {
			sel[i] = sql.NewSimpleCol("argMaxMerge(last)", "val")
		}
		if _s != nil && _s.GetAlias() == "timestamp_ms" {
			sel[i] = sql.NewSimpleCol(
				fmt.Sprintf("intDiv(timestamp_ns, %d) * %d",
					ctx.Step.Nanoseconds(), ctx.Step.Milliseconds()),
				"timestamp_ms")
		}
	}
	req = req.Select(sel...).
		From(sql.NewSimpleCol(ctx.Metrics15sDistTableName, "samples")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC))

	return req, nil
}
