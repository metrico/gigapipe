package planner

import (
	"fmt"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"strings"
)

type LabelsPlanner struct {
	Main shared.SQLRequestPlanner
	// DropMetricName strips __name__, as prometheus does for range functions
	// (rate, increase, *_over_time, ...). A bare selector leaves it false and
	// keeps the metric name.
	DropMetricName bool
}

func (l *LabelsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(main, "values")
	var withFp *sql.With
	for _, with := range main.GetWith() {
		if with.GetAlias() == "fp" {
			withFp = with
			break
		}
	}
	if withFp == nil {
		return nil, fmt.Errorf("could not find fingerprint with alias 'fp'")
	}
	labelsCol := sql.NewSimpleCol("labels", "labels")
	if l.DropMetricName {
		// Range functions produce an instant vector and drop __name__, exactly
		// like prometheus. The aggregation path strips it in
		// AggPlanner.patchLabels; the bare range path passes through here.
		labelsCol = sql.NewSimpleCol("toJSONString("+
			"mapFromArrays("+
			"arrayMap(x -> x.1, arrayFilter(x -> x.1 != '__name__', "+
			"JSONExtractKeysAndValues(labels, 'String')) as a), "+
			"arrayMap(x -> x.2, a)))", "labels")
	}
	values := sql.NewSelect().
		Select(
			sql.NewSimpleCol("1", "type"),
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("val", "val"),
			sql.NewSimpleCol("''", "labels")).
		From(sql.NewWithRef(withMain))
	labels := sql.NewSelect().
		Distinct(true).
		Select(
			sql.NewSimpleCol("2", "type"),
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("0", "timestamp_ms"),
			sql.NewSimpleCol("toFloat64(0)", "val"),
			labelsCol).
		From(sql.NewRawObject(ctx.TimeSeriesDistTableName)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp)))
	res := sql.NewSelect().
		With(withMain).
		Select(sql.NewRawObject("*")).
		From(&unionAll{values, []sql.ISelect{labels}})
	return res, nil
}

type unionAll struct {
	sql.ISelect
	unions []sql.ISelect
}

func (u *unionAll) String(ctx *sql.Ctx, options ...int) (string, error) {
	selects := make([]string, len(u.unions)+1)
	var err error
	selects[0], err = u.ISelect.String(ctx, options...)
	if err != nil {
		return "", err
	}
	for i, union := range u.unions {
		selects[i+1], err = union.String(ctx, options...)
		if err != nil {
			return "", err
		}
	}
	return "(" + strings.Join(selects, ") UNION ALL (") + ")", nil
}
