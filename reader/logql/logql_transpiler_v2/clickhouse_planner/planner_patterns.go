package clickhouse_planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type PatternsPlanner struct {
	fpPlanner shared.SQLRequestPlanner
}

func (p *PatternsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := p.fpPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFp := sql.NewWith(fp, "fp")
	_req := sql.NewSelect().With(withFp).
		Select(
			sql.NewSimpleCol("pattern_id", "pattern_id"),
			sql.NewSimpleCol("max(iteration_id)", "_iteration_id"),
			sql.NewSimpleCol("argMax(tokens, iteration_id)", "tokens"),
			sql.NewSimpleCol(
				fmt.Sprintf("intDiv(timestamp_s, %d) * %[1]d", int(ctx.Step.Seconds())),
				"timestamp_s"),
			sql.NewSimpleCol("sum(samples_count)", "count")).
		From(sql.NewSimpleCol(ctx.PatternsDistTable, "p")).
		AndWhere(
			sql.Ge(sql.NewRawObject("p.timestamp_10m"), sql.NewIntVal(ctx.From.Unix()/600)),
			sql.Le(sql.NewRawObject("p.timestamp_10m"), sql.NewIntVal(ctx.To.Unix()/600)),
			sql.Ge(sql.NewRawObject("p.timestamp_s"), sql.NewIntVal(ctx.From.Unix())),
			sql.Le(sql.NewRawObject("p.timestamp_s"), sql.NewIntVal(ctx.To.Unix())),
			sql.NewIn(sql.NewRawObject("p.fingerprint"), sql.NewWithRef(withFp))).
		GroupBy(sql.NewRawObject("pattern_id"), sql.NewRawObject("timestamp_s"))
	withReq := sql.NewWith(_req, "pregroup")
	req := sql.NewSelect().With(withReq).
		Select(
			sql.NewSimpleCol("argMax(tokens, _iteration_id)", "tokens"),
			sql.NewSimpleCol("arraySort(groupArray((timestamp_s, count)))", "samples")).
		From(sql.NewWithRef(withReq)).
		GroupBy(sql.NewRawObject("pattern_id")).
		AndHaving(sql.Gt(sql.NewRawObject(`arraySum(arrayMap(x -> x.2, samples))`), sql.NewIntVal(1))).
		OrderBy(sql.NewOrderBy(sql.NewRawObject(`arraySum(arrayMap(x -> x.2, samples))`),
			sql.ORDER_BY_DIRECTION_DESC)).
		Limit(sql.NewIntVal(ctx.Limit))
	return req, nil
}
