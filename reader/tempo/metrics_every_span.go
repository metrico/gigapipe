package tempo

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	ct "github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// spanHashCount counts a shard's distinct spans.
const spanHashCount = "uniqExact(cityHash64(main.trace_id, main.span_id))"

// everySpanKey is a key column of a shard count.
type everySpanKey struct{ alias, expr string }

// counts counts spans on each shard per keys and by-labels, and sums them into
// a column alias with value of the summed count. Results read keys, the value, by-labels.
func counts(req MetricsRequest, keys []everySpanKey, alias string, value func(count string) string,
	labels *[]string) ct.ShardBuild {
	var group []sql.SQLObject
	return ct.ShardBuild{
		Shard: func(read ct.SpanRead, attrs string) (sql.ISelect, error) {
			columns := make([]sql.SQLObject, 0, len(keys)+1)
			group = group[:0]
			for _, key := range keys {
				columns = append(columns, sql.NewCol(sql.NewRawObject(key.expr), key.alias))
				group = append(group, sql.NewRawObject(key.alias))
			}
			columns = append(columns, sql.NewCol(sql.NewRawObject(spanHashCount), "n"))
			query := read().Select(columns...)
			resolved := addByLabels(query, &group, req.By, req.FromNS, req.ToNS, attrs)
			if labels != nil {
				*labels = resolved
			}
			if len(group) > 0 {
				query.GroupBy(group...)
			}
			return query, nil
		},
		Result: func(rows *sql.With) (sql.ISelect, error) {
			columns := make([]sql.SQLObject, 0, len(group)+1)
			order := make([]sql.SQLObject, 0, len(keys))
			for _, key := range keys {
				columns = append(columns, sql.NewRawObject(key.alias))
				order = append(order, sql.NewOrderBy(sql.NewRawObject(key.alias), sql.ORDER_BY_DIRECTION_ASC))
			}
			columns = append(columns, sql.NewCol(sql.NewRawObject(value("sum(n)")), alias))
			columns = append(columns, group[len(keys):]...)
			query := sql.NewSelect().With(rows).Select(columns...).From(sql.NewWithRef(rows))
			if len(group) > 0 {
				query.GroupBy(group...)
			}
			if len(order) > 0 {
				query.OrderBy(order...)
			}
			return query, nil
		},
	}
}

// isSpanCount reports whether fn only counts a bucket's spans.
func isSpanCount(fn string) bool {
	return fn == "rate" || fn == "count_over_time"
}

// RangeSeries is req's range statement under plan, and sets labels to the
// statement's by-labels in column order when it runs.
func RangeSeries(plan *ct.MetricsPlan, req MetricsRequest, labels *[]string) (shared.SQLRequestPlanner, error) {
	if plan.EverySpan() && isSpanCount(req.Fn) {
		divisor := float64(req.StepNS) / 1e9
		return plan.OverShards(counts(req, []everySpanKey{{"ts", tsBucket(&req)}}, "value",
			func(count string) string { return countExpr(req.Fn, count, divisor, req.Scale) }, labels)), nil
	}
	return plan.Metrics, nil
}

// InstantSeries is req's instant statement under plan; its window is its one bucket.
func InstantSeries(plan *ct.MetricsPlan, req MetricsRequest, labels *[]string) (shared.SQLRequestPlanner, error) {
	if plan.EverySpan() && isSpanCount(req.Fn) {
		return plan.OverShards(counts(req, nil, "value", func(count string) string {
			return countExpr(req.Fn, count, instantWindowS(&req), req.Scale)
		}, labels)), nil
	}
	return plan.Metrics, nil
}

// HistogramRange is req's histogram statement under plan.
func HistogramRange(plan *ct.MetricsPlan, req MetricsRequest) (shared.SQLRequestPlanner, error) {
	if !plan.EverySpan() {
		return plan.Metrics, nil
	}
	bucket, err := histogramBucket(req.Attr)
	if err != nil {
		return nil, err
	}
	req.By = nil
	return plan.OverShards(counts(req, []everySpanKey{{"ts", tsBucket(&req)}, {"bucket_ns", bucket}}, "cnt",
		func(count string) string { return scaledExpr(count, req.Scale) }, nil)), nil
}

// GenericExemplars is the statement of req's exemplars under plan.
func GenericExemplars(plan *ct.MetricsPlan, req MetricsRequest, labels *[]string) (shared.SQLRequestPlanner, error) {
	return plan.Over(func(spans *sql.With) (sql.ISelect, error) {
		req.Spans = spans
		query, resolved, err := BuildGenericExemplarsQuery(&req)
		*labels = resolved
		return query, err
	})
}

// HistogramExemplars is the statement of req's histogram exemplars under plan.
func HistogramExemplars(plan *ct.MetricsPlan, req MetricsRequest) (shared.SQLRequestPlanner, error) {
	return plan.Over(func(spans *sql.With) (sql.ISelect, error) {
		req.Spans = spans
		return BuildHistogramExemplarsQuery(&req)
	})
}

// tsBucket is req's bucket start of a span.
func tsBucket(req *MetricsRequest) string {
	return fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)
}
