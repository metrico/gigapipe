package planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type ValuesPlanner struct {
	Fp shared.SQLRequestPlanner
}

func (v *ValuesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := v.Fp.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFp := sql.NewWith(fp, "fp")
	res := sql.NewSelect().With(withFp).Select(
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		sql.NewSimpleCol("samples.value", "val"),
		sql.NewSimpleCol("intDiv(samples.timestamp_ns, 1000000)", "timestamp_ms"),
	).From(sql.NewSimpleCol(ctx.SamplesDistTableName, "samples")).AndWhere(
		sql.Gt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
		sql.Le(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
		sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp)),
		clickhouse_planner.GetTypes(ctx),
	).OrderBy(sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
		sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_ASC))
	if ctx.Limit > 0 {
		res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}
