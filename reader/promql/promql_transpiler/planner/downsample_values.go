package planner

import (
	"fmt"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
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
			// This column is dead in the ordinary case: DownsampleHintsPlanner
			// wraps this planner and patches timestamp_ms on every branch it
			// takes, so what survives into the query is its column, keyed by
			// the ceiling (bucketTimestampCol). It is left floor-keyed here
			// only because nothing reads it.
			//
			// The exception is a zero step. DownsampleHintsPlanner returns
			// before patching anything when Hints.Step == 0, and ctx.Step is
			// derived from that same Hints.Step, so a zero would leave both
			// this keying and an intDiv(x, 0) standing in the rendered query.
			// What prevents it lives in another file: adjustHintsForRate
			// rewrites a zero step to max(Range/2, 15000) before any planner
			// sees it (reader/service/prom_queryable.go). Anything that
			// reaches this planner by another route has to uphold that, or
			// correct both lines below.
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
