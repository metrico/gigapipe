package clickhouse_planner

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

type DetectLabelsPlanner struct {
	NoStreamSelect bool
	fpPlanner      shared.SQLRequestPlanner
}

func (d *DetectLabelsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	from := ctx.TimeSeriesGinTableName
	if ctx.IsCluster {
		from += distconfig.Suffix()
	}

	req := sql.NewSelect().
		Select(
			sql.NewSimpleCol("key", "key"),
			sql.NewSimpleCol("count(distinct val)", "cardinality")).
		From(sql.NewRawObject(from)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(FormatToDate(ctx.To)))).
		GroupBy(sql.NewRawObject("key"))
	if d.NoStreamSelect || d.fpPlanner == nil {
		return req, nil
	}

	fp, err := d.fpPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFp := sql.NewWith(fp, "fp")
	req = req.With(withFp).
		AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp)))
	return req, nil
}
