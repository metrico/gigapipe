package transpiler

import (
	"fmt"

	logql_transpiler_shared "github.com/metrico/qryn/reader/logql/transpiler/shared"
	"github.com/metrico/qryn/reader/model"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type TranspileResponse struct {
	MapResult func(samples []model.Sample) []model.Sample
	Query     sql.ISelect
}

func TranspileLabelMatchers(hints *storage.SelectHints,
	ctx *logql_transpiler_shared.PlannerContext, matchers ...*labels.Matcher) (*TranspileResponse, error) {
	query, err := (NewInitClickhousePlanner()).Process(ctx)
	if err != nil {
		return nil, err
	}
	fpQuery, err := fingerprintsQuery(ctx, matchers...)
	if err != nil {
		return nil, err
	}
	withFingerprints := sql.NewWith(fpQuery, "fp_sel")
	query = query.AddWith(withFingerprints)
	query.AndWhere(sql.NewIn(sql.NewRawObject("samples.fingerprint"), sql.NewWithRef(withFingerprints)))
	if hints.Step != 0 {
		query = processHints(query, hints)
	}
	return &TranspileResponse{nil, query}, nil
}

func processHints(query sql.ISelect, hints *storage.SelectHints) sql.ISelect {
	instantVectors := map[string]bool{
		"abs": true, "absent": true, "ceil": true, "exp": true, "floor": true,
		"ln": true, "log2": true, "log10": true, "round": true, "scalar": true, "sgn": true, "sort": true, "sqrt": true,
		"timestamp": true, "atan": true, "cos": true, "cosh": true, "sin": true, "sinh": true, "tan": true, "tanh": true,
		"deg": true, "rad": true,
	}
	rangeVectors := map[string]bool{
		"absent_over_time": true /*"changes": true,*/, "deriv": true, "idelta": true, "irate": true,
		"rate": true, "resets": true, "min_over_time": true, "max_over_time": true, "sum_over_time": true,
		"count_over_time": true, "stddev_over_time": true, "stdvar_over_time": true, "last_over_time": true,
		"present_over_time": true, "delta": true, "increase": true, "avg_over_time": true,
	}
	if instantVectors[hints.Func] || hints.Func == "" {
		withQuery := sql.NewWith(query, "spls")
		query = sql.NewSelect().With(withQuery).Select(
			sql.NewRawObject("fingerprint"),
			//sql.NewSimpleCol("spls.labels", "labels"),
			sql.NewSimpleCol("argMax(spls.value, spls.timestamp_ms)", "value"),
			sql.NewSimpleCol(fmt.Sprintf("intDiv(spls.timestamp_ms - %d + %d - 1, %d) * %d + %d",
				hints.Start, hints.Step, hints.Step, hints.Step, hints.Start), "timestamp_ms"),
		).From(
			sql.NewWithRef(withQuery),
		).GroupBy(
			sql.NewRawObject("timestamp_ms"),
			sql.NewRawObject("fingerprint"),
		).OrderBy(
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC),
		)
	}
	if rangeVectors[hints.Func] && hints.Step > hints.Range {
		msInStep := sql.NewRawObject(fmt.Sprintf("timestamp_ms %% %d", hints.Step))
		query.AndWhere(sql.Or(
			sql.Eq(msInStep, sql.NewIntVal(0)),
			sql.Ge(msInStep, sql.NewIntVal(hints.Step-hints.Range)),
		))
	}
	/*aggregators := map[string]string{
		"sum":   "sum(spls.value)",
		"min":   "min(spls.value)",
		"max":   "max(spls.value)",
		"group": "1",
	}
	if _, ok := aggregators[hints.Func]; ok && len(hints.Grouping) > 0 {
		query = trimLabels(query, hints)
		withPoints := sql.NewWith(query, "spls")
		query = sql.NewSelect().With(withPoints).Select(
			sql.NewSimpleCol("cityHash64(toString(arraySort(spls.labels)))", "fingerprint"),
			sql.NewSimpleCol("spls.labels", "labels"),
			sql.NewSimpleCol(aggregators[hints.Func], "value"),
			sql.NewSimpleCol("spls.timestamp_ms", "timestamp_ms"),
		).From(
			sql.NewWithRef(withPoints),
		).GroupBy(
			sql.NewRawObject("timestamp_ms"),
			sql.NewRawObject("fingerprint"),
		).OrderBy(
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC),
		)
	}*/

	return query
}
