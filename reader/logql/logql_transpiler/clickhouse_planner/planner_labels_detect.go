package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type DetectLabelsPlanner struct {
	fpPlanner shared.SQLRequestPlanner
}

func (d *DetectLabelsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	from := ctx.TimeSeriesGinTableName
	if ctx.IsCluster {
		from += "_dist"
	}

	req := sql.NewSelect().
		Select(
			sql.NewSimpleCol("key", "key"),
			sql.NewSimpleCol("count(distinct val)", "cardinality")).
		From(sql.NewRawObject(from)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02")))).
		GroupBy(sql.NewRawObject("key"))
	if d.fpPlanner == nil {
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
