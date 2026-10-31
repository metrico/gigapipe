package tempo

import (
	"fmt"
	"strings"
	"time"

	model "github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// IntrinsicAttr maps TraceQL intrinsic attribute names to tempo_traces columns.
var IntrinsicAttr = map[string]string{
	"span:duration": "duration_ns",
	"duration":      "duration_ns",
	"span:name":     "name",
	"name":          "name",
}

// metricsAggFn maps a metrics function name to a ClickHouse aggregation expression template.
// The placeholder %s is replaced with the column name.
var metricsAggFn = map[string]string{
	"count_over_time": "count(*)",
	"avg_over_time":   "avg(%s)",
	"min_over_time":   "min(%s)",
	"max_over_time":   "max(%s)",
	"sum_over_time":   "sum(%s)",
}

// MetricsRequest is a metrics function over Spans, a CTE with one row per
// span it counts: trace_id, span_id, timestamp_ns, duration_ns, name,
// service_name and the MetricsNestedSetColumn of each nested-set field it reads.
type MetricsRequest struct {
	Spans      *sql.With
	Fn         string
	Attr       string
	By         []string
	Percentile *float64
	FromNS     int64
	ToNS       int64
	StepNS     int64
	AttrsTable string
}

// BuildMetricsRangeQuery builds a ClickHouse SQL query for a metrics range request.
// It returns the query and the list of by-label keys (for result grouping).
func BuildMetricsRangeQuery(req *MetricsRequest) (sql.ISelect, []string, error) {
	valueExpr, err := buildAggExpr(req.Fn, req.Attr, float64(req.StepNS)/1e9, req.Percentile)
	if err != nil {
		return nil, nil, err
	}
	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)
	query := spansQuery(req.Spans).Select(
		sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
		sql.NewCol(sql.NewRawObject(valueExpr), "value"),
	)
	groupBy := []sql.SQLObject{sql.NewRawObject("ts")}
	resolvedByLabels := addByLabels(query, &groupBy, req.By, req.FromNS, req.ToNS, req.AttrsTable)
	query.GroupBy(groupBy...).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC))
	return query, resolvedByLabels, nil
}

// BuildMetricsInstantQuery builds a ClickHouse SQL query for an instant metrics query.
func BuildMetricsInstantQuery(req *MetricsRequest) (sql.ISelect, []string, error) {
	windowS := float64(req.ToNS-req.FromNS) / 1e9
	if windowS <= 0 {
		windowS = 1
	}
	valueExpr, err := buildAggExpr(req.Fn, req.Attr, windowS, req.Percentile)
	if err != nil {
		return nil, nil, err
	}
	query := spansQuery(req.Spans).Select(sql.NewCol(sql.NewRawObject(valueExpr), "value"))
	var groupBy []sql.SQLObject
	resolvedByLabels := addByLabels(query, &groupBy, req.By, req.FromNS, req.ToNS, req.AttrsTable)
	if len(groupBy) > 0 {
		query.GroupBy(groupBy...)
	}
	return query, resolvedByLabels, nil
}

// spansQuery selects from spans as main.
func spansQuery(spans *sql.With) sql.ISelect {
	return sql.NewSelect().
		With(spans).
		From(sql.NewCol(sql.NewWithRef(spans), "main"))
}

// addByLabels adds a column and a group-by key to query per by-label:
// intrinsics from main, then other attributes pivoted from the attributes
// table. It returns the labels in column order.
func addByLabels(query sql.ISelect, groupBy *[]sql.SQLObject, byLabels []string,
	fromNS, toNS int64, attrsTable string) []string {
	resolved := make([]string, 0, len(byLabels))
	var custom []string
	for i, label := range byLabels {
		if col := resolveAttrColumn(label); col != "" {
			resolved = append(resolved, label)
			alias := fmt.Sprintf("by_%d", i)
			query.Select(append(query.GetSelect(), sql.NewCol(sql.NewRawObject("main."+col), alias))...)
			*groupBy = append(*groupBy, sql.NewRawObject(alias))
		} else {
			custom = append(custom, label)
		}
	}
	resolved = append(resolved, custom...)
	if len(custom) == 0 {
		return resolved
	}
	fromDate := time.Unix(fromNS/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(toNS/1e9, 0).Format("2006-01-02")
	byWith, byAliases := buildByPivotCTE(custom, fromDate, toDate, attrsTable)
	query.AddWith(byWith)
	query.AddJoin(sql.NewJoin("LEFT",
		sql.NewCol(sql.NewWithRef(byWith), "by_attrs"),
		sql.And(
			sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("by_attrs.trace_id")),
			sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("by_attrs.span_id")),
		),
	))
	for i, alias := range byAliases {
		displayAlias := fmt.Sprintf("by_custom_%d", i)
		query.Select(append(query.GetSelect(), sql.NewCol(sql.NewRawObject("by_attrs."+alias), displayAlias))...)
		*groupBy = append(*groupBy, sql.NewRawObject(displayAlias))
	}
	return resolved
}

// buildAggExpr returns the ClickHouse aggregation expression for a metrics function.
func buildAggExpr(fn string, attr string, divisor float64, percentile *float64) (string, error) {
	if fn == "rate" {
		return fmt.Sprintf("toFloat64(count(*)) / %f", divisor), nil
	}
	if fn == "quantile_over_time" {
		col := resolveAttrForAgg(attr)
		if col == "" {
			return "", fmt.Errorf("quantile_over_time requires an attribute argument")
		}
		p := 0.5
		if percentile != nil {
			p = *percentile
		}
		expr := fmt.Sprintf("quantile(%g)(%s)", p, col)
		if col == "main.duration_ns" {
			expr += " / 1e9"
		}
		return expr, nil
	}
	if fn == "histogram_over_time" {
		col := resolveAttrForAgg(attr)
		if col == "" {
			return "", fmt.Errorf("histogram_over_time requires an attribute argument")
		}
		return fmt.Sprintf("histogram(20)(%s)", col), nil
	}
	if fn == "compare" {
		// compare() is handled at the service layer, not the generic metrics query builder
		return "", fmt.Errorf("compare() should be handled by BuildCompareQuery")
	}
	tmpl, ok := metricsAggFn[fn]
	if !ok {
		return "", fmt.Errorf("unsupported metrics function: %s", fn)
	}
	if !strings.Contains(tmpl, "%s") {
		return tmpl, nil
	}
	col := resolveAttrForAgg(attr)
	if col == "" {
		return "", fmt.Errorf("metrics function %s requires an attribute argument", fn)
	}
	expr := fmt.Sprintf(tmpl, col)
	// duration_ns is stored in nanoseconds; Tempo API returns seconds
	if col == "main.duration_ns" {
		expr += " / 1e9"
	}
	return expr, nil
}

// resolveAttrForAgg resolves a TraceQL attribute name to a ClickHouse column for aggregation.
func resolveAttrForAgg(attr string) string {
	if col, ok := IntrinsicAttr[attr]; ok {
		return "main." + col
	}
	if col, ok := clickhouse_transpiler.MetricsNestedSetColumn(attr); ok {
		return "main." + col
	}
	return ""
}

// resolveAttrColumn resolves a TraceQL attribute to a column of the spans
// (intrinsics and nested-set fields only).
func resolveAttrColumn(label string) string {
	if col, ok := IntrinsicAttr[label]; ok {
		return col
	}
	if col, ok := clickhouse_transpiler.MetricsNestedSetColumn(label); ok {
		return col
	}
	if label == "resource.service.name" || label == "service.name" || label == "rootServiceName" {
		return "service_name"
	}
	return ""
}

// stripAttrPrefix removes "span.", "resource." prefixes from attribute names for GIN lookup.
func stripAttrPrefix(label string) string {
	if strings.HasPrefix(label, "span.") {
		return label[5:]
	}
	if strings.HasPrefix(label, "resource.") {
		return label[9:]
	}
	return label
}

// buildByPivotCTE builds a CTE that pivots attribute values into columns, one row per span.
func buildByPivotCTE(labels []string, fromDate string, toDate string, attrsTable string) (*sql.With, []string) {
	ginKeys := make([]string, len(labels))
	aliases := make([]string, len(labels))

	selectCols := []sql.SQLObject{
		sql.NewRawObject("trace_id"),
		sql.NewRawObject("span_id"),
	}

	for i, label := range labels {
		ginKeys[i] = stripAttrPrefix(label)
		aliases[i] = fmt.Sprintf("by_lbl_%d", i)
		selectCols = append(selectCols,
			sql.NewCol(
				sql.NewRawObject(fmt.Sprintf("anyIf(val, key = '%s')",
					strings.ReplaceAll(ginKeys[i], "'", "\\'"))),
				aliases[i],
			),
		)
	}

	pivot := sql.NewSelect().
		Select(selectCols...).
		From(sql.NewRawObject(attrsTable+" FINAL")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", fromDate))),
			sql.Le(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", toDate))),
		).
		AndWhere(sql.NewIn(sql.NewRawObject("key"), stringsToSQLObjects(ginKeys)...)).
		GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id"))

	return sql.NewWith(pivot, "by_attrs"), aliases
}

func stringsToSQLObjects(vals []string) []sql.SQLObject {
	res := make([]sql.SQLObject, len(vals))
	for i, v := range vals {
		res[i] = sql.NewStringVal(v)
	}
	return res
}

// histogramBucket is the power-of-2 upper bound of the attribute's column.
func histogramBucket(attr string) (string, error) {
	col := resolveAttrForAgg(attr)
	if col == "" {
		return "", fmt.Errorf("histogram_over_time requires an attribute argument")
	}
	return fmt.Sprintf("if(%s > 0, bitShiftLeft(toUInt64(1), toUInt8(ceil(log2(%s)))), toUInt64(0))", col, col), nil
}

// BuildHistogramRangeQuery builds a ClickHouse SQL query for histogram_over_time().
// Returns rows of (ts, bucket_ns, count) where bucket_ns is the power-of-2
// upper bound of each span's attribute value, computed inline.
func BuildHistogramRangeQuery(req *MetricsRequest) (sql.ISelect, error) {
	bucket, err := histogramBucket(req.Attr)
	if err != nil {
		return nil, err
	}
	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)
	return spansQuery(req.Spans).
		Select(
			sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
			sql.NewCol(sql.NewRawObject(bucket), "bucket_ns"),
			sql.NewCol(sql.NewRawObject("count(*)"), "cnt"),
		).
		GroupBy(sql.NewRawObject("ts"), sql.NewRawObject("bucket_ns")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("bucket_ns"), sql.ORDER_BY_DIRECTION_ASC),
		), nil
}

// BuildHistogramExemplarsQuery builds a query to sample one span per (ts, bucket_ns)
// for use as exemplars in the histogram response.
func BuildHistogramExemplarsQuery(req *MetricsRequest) (sql.ISelect, error) {
	bucket, err := histogramBucket(req.Attr)
	if err != nil {
		return nil, err
	}
	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)
	return spansQuery(req.Spans).
		Select(
			sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
			sql.NewCol(sql.NewRawObject(bucket), "bucket_ns"),
			exemplarCols[0], exemplarCols[1], exemplarCols[2],
		).
		GroupBy(sql.NewRawObject("ts"), sql.NewRawObject("bucket_ns")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("bucket_ns"), sql.ORDER_BY_DIRECTION_ASC),
		), nil
}

// exemplarSpan picks one span of a group; exemplarCols read its trace id,
// duration and start time.
const exemplarSpan = "any((lower(hex(main.trace_id)), main.duration_ns, main.timestamp_ns))"

var exemplarCols = []sql.SQLObject{
	sql.NewCol(sql.NewRawObject("tupleElement("+exemplarSpan+", 1)"), "trace_id"),
	sql.NewCol(sql.NewRawObject("tupleElement("+exemplarSpan+", 2)"), "dur_ns"),
	sql.NewCol(sql.NewRawObject("tupleElement("+exemplarSpan+", 3)"), "span_ts"),
}

// HistogramExemplar represents a single exemplar data point for histogram series.
type HistogramExemplar struct {
	Labels      []model.MetricsKeyValue `json:"labels"`
	Value       float64                 `json:"value"`
	TimestampMs string                  `json:"timestampMs"`
}

// BuildGenericExemplarsQuery builds a query that samples one span per time
// bucket and by-label group for use as exemplars in rate/quantile/avg/etc.
// responses. It returns the by-labels in column order.
func BuildGenericExemplarsQuery(req *MetricsRequest) (sql.ISelect, []string, error) {
	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)
	query := spansQuery(req.Spans).Select(
		sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
		exemplarCols[0], exemplarCols[1], exemplarCols[2],
	)
	groupBy := []sql.SQLObject{sql.NewRawObject("ts")}
	resolvedByLabels := addByLabels(query, &groupBy, req.By, req.FromNS, req.ToNS, req.AttrsTable)
	query.GroupBy(groupBy...).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC))
	return query, resolvedByLabels, nil
}

// CompareRequest holds the parameters of compare(): Baseline holds the spans
// over the baseline window and Selection the selected spans, or is nil when
// every baseline span is selected. Both are CTEs shaped like
// MetricsRequest.Spans.
type CompareRequest struct {
	Baseline       *sql.With
	Selection      *sql.With
	FromNS         int64
	ToNS           int64
	BaselineFromNS int64
	BaselineToNS   int64
	AttrsTable     string
	TopN           int
}

// CompareResultRow is one row from the compare() query.
type CompareResultRow struct {
	Key            string
	Val            string
	SelectionCount uint64
	BaselineCount  uint64
}

// BuildCompareQuery builds a ClickHouse SQL query for compare().
func BuildCompareQuery(req *CompareRequest) (sql.ISelect, error) {
	// Expand to the widest date range for partition pruning.
	fromDate := time.Unix(min(req.FromNS, req.BaselineFromNS)/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(max(req.ToNS, req.BaselineToNS)/1e9, 0).Format("2006-01-02")

	topN := req.TopN
	if topN <= 0 {
		topN = 10
	}

	// CTE: all_spans — the baseline spans
	allSpansWith := sql.NewWith(
		sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
			From(sql.NewWithRef(req.Baseline)),
		"all_spans")

	// CTE: selection — the selected spans, every baseline span without a selection
	selected := allSpansWith
	if req.Selection != nil {
		selected = req.Selection
	}
	selectionWith := sql.NewWith(
		sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
			From(sql.NewWithRef(selected)),
		"selection")

	// CTE: attrs_sub — filtered attrs_gin rows
	ginDateWhere := sql.And(
		sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", fromDate))),
		sql.Le(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", toDate))),
	)

	attrsSubquery := sql.NewSelect().
		Select(sql.NewRawObject("*")).
		From(sql.NewRawObject(req.AttrsTable + " FINAL")).
		AndWhere(ginDateWhere)
	attrsSubWith := sql.NewWith(attrsSubquery, "attrs_sub")

	// CTE: counted — enumerate attrs, classify, count
	countedQuery := sql.NewSelect().
		Select(
			sql.NewCol(sql.NewRawObject("attrs.key"), "attr_key"),
			sql.NewCol(sql.NewRawObject("attrs.val"), "attr_val"),
			sql.NewCol(sql.NewRawObject("countIf(sel.trace_id != '')"), "selection_count"),
			sql.NewCol(sql.NewRawObject("countIf(sel.trace_id = '')"), "baseline_count"),
		).
		From(sql.NewCol(sql.NewWithRef(allSpansWith), "asp")).
		AddJoin(sql.NewJoin("INNER",
			sql.NewCol(sql.NewWithRef(attrsSubWith), "attrs"),
			sql.And(
				sql.Eq(sql.NewRawObject("asp.trace_id"), sql.NewRawObject("attrs.trace_id")),
				sql.Eq(sql.NewRawObject("asp.span_id"), sql.NewRawObject("attrs.span_id")),
			),
		)).
		AddJoin(sql.NewJoin("LEFT",
			sql.NewCol(sql.NewWithRef(selectionWith), "sel"),
			sql.And(
				sql.Eq(sql.NewRawObject("asp.trace_id"), sql.NewRawObject("sel.trace_id")),
				sql.Eq(sql.NewRawObject("asp.span_id"), sql.NewRawObject("sel.span_id")),
			),
		)).
		GroupBy(sql.NewRawObject("attr_key"), sql.NewRawObject("attr_val")).
		AndHaving(sql.Or(
			sql.Gt(sql.NewRawObject("selection_count"), sql.NewIntVal(0)),
			sql.Gt(sql.NewRawObject("baseline_count"), sql.NewIntVal(0)),
		))
	countedWith := sql.NewWith(countedQuery, "counted")

	// CTE: ranked — apply topN per key
	rankedQuery := sql.NewSelect().
		Select(
			sql.NewRawObject("*"),
			sql.NewCol(sql.NewRawObject("row_number() OVER (PARTITION BY attr_key ORDER BY (selection_count + baseline_count) DESC)"), "rn"),
		).
		From(sql.NewWithRef(countedWith))
	rankedWith := sql.NewWith(rankedQuery, "ranked")

	// Final query
	query := sql.NewSelect().
		Select(
			sql.NewRawObject("attr_key"),
			sql.NewRawObject("attr_val"),
			sql.NewRawObject("selection_count"),
			sql.NewRawObject("baseline_count"),
		).
		From(sql.NewWithRef(rankedWith)).
		With(req.Baseline).
		AndWhere(sql.Le(sql.NewRawObject("rn"), sql.NewIntVal(int64(topN)))).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("attr_key"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("selection_count + baseline_count"), sql.ORDER_BY_DIRECTION_DESC),
		)
	if req.Selection != nil {
		query.AddWith(req.Selection)
	}
	query.AddWith(allSpansWith, selectionWith, attrsSubWith, countedWith, rankedWith)

	return query, nil
}
