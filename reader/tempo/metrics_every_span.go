package tempo

import (
	"fmt"
	"slices"

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

// everySpanShape is how a statement reads a lone `{}`.
type everySpanShape int

const (
	// groupedSpans reads one row per span pair through the plan's CTE.
	groupedSpans everySpanShape = iota
	// shardCounts counts span hashes per bucket on each shard.
	shardCounts
	// spanRows reads the rows as stored.
	spanRows
	// distinctSpans reads one row per span hash.
	distinctSpans
)

// shapeOf is the shape of req's series under plan. A custom by-label on a
// function that reads a span's value takes the grouped read.
func shapeOf(plan *ct.MetricsPlan, req *MetricsRequest) everySpanShape {
	if !plan.EverySpan() {
		return groupedSpans
	}
	switch req.Fn {
	case "rate", "count_over_time":
		return shardCounts
	case "min_over_time", "max_over_time":
		if !hasCustomBy(req.By) {
			return spanRows
		}
	case "avg_over_time", "sum_over_time", "quantile_over_time":
		if !hasCustomBy(req.By) {
			return distinctSpans
		}
	}
	return groupedSpans
}

// hasCustomBy reports whether a by-label needs the attributes table.
func hasCustomBy(by []string) bool {
	return slices.ContainsFunc(by, func(label string) bool { return resolveAttrColumn(label) == "" })
}

// byAliases are the by-label columns addByLabels adds, in column order.
func byAliases(by []string) []sql.SQLObject {
	res := make([]sql.SQLObject, 0, len(by))
	for i, label := range by {
		if resolveAttrColumn(label) != "" {
			res = append(res, sql.NewRawObject(fmt.Sprintf("by_%d", i)))
		}
	}
	custom := 0
	for _, label := range by {
		if resolveAttrColumn(label) == "" {
			res = append(res, sql.NewRawObject(fmt.Sprintf("by_custom_%d", custom)))
			custom++
		}
	}
	return res
}

// spanColumns are the traces columns req's value and by-labels read.
func spanColumns(req *MetricsRequest, bucketed bool) []string {
	var cols []string
	add := func(col string) {
		if col != "" && !slices.Contains(cols, col) {
			cols = append(cols, col)
		}
	}
	add(IntrinsicAttr[req.Attr])
	for _, label := range req.By {
		add(resolveAttrColumn(label))
	}
	if bucketed {
		add("timestamp_ns")
	}
	return cols
}

// RangeSeries is req's range statement under plan, and sets labels to the
// statement's by-labels in column order when it runs.
func RangeSeries(plan *ct.MetricsPlan, req MetricsRequest, labels *[]string) (shared.SQLRequestPlanner, error) {
	build := func(read ct.SpanRead) (sql.ISelect, error) {
		r := req
		r.Read = read
		query, resolved, err := BuildMetricsRangeQuery(&r)
		*labels = resolved
		return query, err
	}
	if req.Fn == "quantile_over_time" && shapeOf(plan, &req) == distinctSpans {
		return plan.OverDistinctSpanShards(spanColumns(&req, true), quantileStates(req, labels)), nil
	}
	switch shapeOf(plan, &req) {
	case shardCounts:
		divisor := float64(req.StepNS) / 1e9
		return plan.OverShards(counts(req, []everySpanKey{{"ts", tsBucket(&req)}}, "value",
			func(count string) string { return countExpr(req.Fn, count, divisor, req.Scale) }, labels)), nil
	case spanRows:
		return plan.OverRows(build), nil
	case distinctSpans:
		return plan.OverDistinctSpans(spanColumns(&req, true), build), nil
	}
	return plan.Metrics, nil
}

// InstantSeries is req's instant statement under plan; its window is its one bucket.
func InstantSeries(plan *ct.MetricsPlan, req MetricsRequest, labels *[]string) (shared.SQLRequestPlanner, error) {
	build := func(read ct.SpanRead) (sql.ISelect, error) {
		r := req
		r.Read = read
		query, resolved, err := BuildMetricsInstantQuery(&r)
		*labels = resolved
		return query, err
	}
	switch shapeOf(plan, &req) {
	case shardCounts:
		return plan.OverShards(counts(req, nil, "value", func(count string) string {
			return countExpr(req.Fn, count, instantWindowS(&req), req.Scale)
		}, labels)), nil
	case spanRows:
		return plan.OverRows(build), nil
	case distinctSpans:
		return plan.OverDistinctSpans(spanColumns(&req, false), build), nil
	}
	return plan.Metrics, nil
}

// quantileStates takes a quantile state per bucket and by-labels from each shard,
// merged per bucket and over the whole query. Result reads ts, value, query_wide, by-labels.
func quantileStates(req MetricsRequest, labels *[]string) ct.ShardBuild {
	col := resolveAttrForAgg(req.Attr)
	levels := quantileLevels(req.Quantiles)
	return ct.ShardBuild{
		Shard: func(read ct.SpanRead, attrs string) (sql.ISelect, error) {
			if col == "" {
				return nil, fmt.Errorf("quantile_over_time requires an attribute argument")
			}
			query := read().Select(
				sql.NewCol(sql.NewRawObject(tsBucket(&req)), "ts"),
				sql.NewCol(sql.NewRawObject(fmt.Sprintf("quantilesState(%s)(%s)", levels, col)), "q_state"))
			group := []sql.SQLObject{sql.NewRawObject("ts")}
			*labels = addByLabels(query, &group, req.By, req.FromNS, req.ToNS, attrs)
			return query.GroupBy(group...), nil
		},
		Result: func(rows *sql.With) (sql.ISelect, error) {
			by := byAliases(req.By)
			value := quantilesInSeconds(col, fmt.Sprintf("quantilesMerge(%s)(q_state)", levels))
			columns := append([]sql.SQLObject{
				sql.NewRawObject("ts"),
				sql.NewCol(sql.NewRawObject(value), "value"),
				sql.NewCol(sql.NewRawObject("grouping(ts)"), "query_wide"),
			}, by...)
			return sql.NewSelect().With(rows).Select(columns...).From(sql.NewWithRef(rows)).
				GroupBy(groupingWithQueryWide(append([]sql.SQLObject{sql.NewRawObject("ts")}, by...))).
				OrderBy(sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC)), nil
		},
	}
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

// GenericExemplars is the statement of req's exemplars under plan. A lone `{}`
// picks a span per group from the rows; with a custom by-label each shard picks
// one and the coordinator keeps the first per group.
func GenericExemplars(plan *ct.MetricsPlan, req MetricsRequest, labels *[]string) (shared.SQLRequestPlanner, error) {
	if !plan.EverySpan() {
		return plan.Over(func(spans *sql.With) (sql.ISelect, error) {
			req.Spans = spans
			query, resolved, err := BuildGenericExemplarsQuery(&req)
			*labels = resolved
			return query, err
		})
	}
	if hasCustomBy(req.By) {
		return plan.OverShards(ct.ShardBuild{
			Shard: func(read ct.SpanRead, attrs string) (sql.ISelect, error) {
				r := req
				r.Read, r.AttrsTable = read, attrs
				query, resolved, err := BuildGenericExemplarsQuery(&r)
				*labels = resolved
				return query, err
			},
			Result: func(rows *sql.With) (sql.ISelect, error) {
				by := byAliases(req.By)
				columns := []sql.SQLObject{sql.NewRawObject("ts"), sql.NewRawObject("trace_id"), sql.NewRawObject("dur_ns"),
					sql.NewRawObject("span_ts"), sql.NewRawObject("arg_value")}
				return sql.NewSelect().With(rows).Select(append(columns, by...)...).From(sql.NewWithRef(rows)).
					OrderBy(sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC)).
					Limit(sql.NewLimitBy(sql.NewIntVal(1), append([]sql.SQLObject{sql.NewRawObject("ts")}, by...)...)), nil
			},
		}), nil
	}
	return plan.OverRows(func(read ct.SpanRead) (sql.ISelect, error) {
		r := req
		r.Read = read
		query, resolved, err := BuildGenericExemplarsQuery(&r)
		*labels = resolved
		return query, err
	}), nil
}

// HistogramExemplars is the statement of req's histogram exemplars under plan.
func HistogramExemplars(plan *ct.MetricsPlan, req MetricsRequest) (shared.SQLRequestPlanner, error) {
	if plan.EverySpan() {
		return plan.OverRows(func(read ct.SpanRead) (sql.ISelect, error) {
			r := req
			r.Read = read
			return BuildHistogramExemplarsQuery(&r)
		}), nil
	}
	return plan.Over(func(spans *sql.With) (sql.ISelect, error) {
		req.Spans = spans
		return BuildHistogramExemplarsQuery(&req)
	})
}

// tsBucket is req's bucket start of a span.
func tsBucket(req *MetricsRequest) string {
	return fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)
}
