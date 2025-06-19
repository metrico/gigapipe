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
	req := sql.NewSelect().With(withFp).
		Select(
			sql.NewSimpleCol("cityHash64(tokens)", "fingerprint"),
			sql.NewSimpleCol("tokens", "tokens"),
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
		GroupBy(sql.NewRawObject("tokens"), sql.NewRawObject("timestamp_s")).
		OrderBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_s"))
	return req, nil
}
