package tempo

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	model "github.com/metrico/qryn/v4/reader/model"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
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

// BuildMetricsRangeQuery builds a ClickHouse SQL query for a metrics range request.
// It returns the query and the list of by-label keys (for result grouping).
func BuildMetricsRangeQuery(
	fn string, attr string, byLabels []string,
	filterKeys []string, filterOps []string, filterVals []string,
	fromNS int64, toNS int64, stepNS int64,
	tracesTable string, attrsTable string,
	distributed bool, percentile *float64,
) (sql.ISelect, []string, error) {

	stepS := float64(stepNS) / 1e9

	// Resolve the value expression (what we aggregate over)
	valueExpr, err := buildAggExpr(fn, attr, stepS, percentile)
	if err != nil {
		return nil, nil, err
	}

	fromDate := time.Unix(fromNS/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(toNS/1e9, 0).Format("2006-01-02")

	// Time bucket expression
	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", stepNS, stepNS)

	// Base select columns
	selectCols := []sql.SQLObject{
		sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
		sql.NewCol(sql.NewRawObject(valueExpr), "value"),
	}

	// Group by columns
	groupByCols := []sql.SQLObject{
		sql.NewRawObject("ts"),
	}

	// Build the main query on tempo_traces
	query := sql.NewSelect().
		From(sql.NewCol(sql.NewRawObject(tracesTable), "main")).
		AndWhere(
			sql.Ge(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(fromNS)),
			sql.Lt(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(toNS)),
		)

	// Add intrinsic filters (e.g., nestedSetParent<0 → root spans) directly on tempo_traces
	for _, cond := range buildIntrinsicConditions(filterKeys, filterOps, filterVals) {
		query.AndWhere(cond)
	}

	// Add attribute filter join if we have filters
	if len(filterKeys) > 0 {
		filterWith := buildFilterCTE(filterKeys, filterOps, filterVals, fromDate, toDate, attrsTable)
		if filterWith != nil {
			query.AddWith(filterWith)
			query.AddJoin(sql.NewJoin("INNER",
				sql.NewCol(sql.NewWithRef(filterWith), "filtered"),
				sql.And(
					sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("filtered.trace_id")),
					sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("filtered.span_id")),
				),
			))
		}
	}

	// Add by-label pivoted join if we have group-by labels
	resolvedByLabels := make([]string, 0, len(byLabels))
	for i, label := range byLabels {
		colName := resolveAttrColumn(label)
		if colName != "" {
			// Intrinsic attribute — select directly from main table
			alias := fmt.Sprintf("by_%d", i)
			selectCols = append(selectCols, sql.NewCol(sql.NewRawObject("main."+colName), alias))
			groupByCols = append(groupByCols, sql.NewRawObject(alias))
			resolvedByLabels = append(resolvedByLabels, label)
		} else {
			// Custom attribute — need GIN table pivot
			resolvedByLabels = append(resolvedByLabels, label)
		}
	}

	// Custom by-labels requiring GIN join
	customByLabels := make([]string, 0)
	for _, label := range byLabels {
		if resolveAttrColumn(label) == "" {
			customByLabels = append(customByLabels, label)
		}
	}
	if len(customByLabels) > 0 {
		byWith, byAliases := buildByPivotCTE(customByLabels, fromDate, toDate, attrsTable)
		query.AddWith(byWith)
		query.AddJoin(sql.NewJoin("LEFT",
			sql.NewCol(sql.NewWithRef(byWith), "by_attrs"),
			sql.And(
				sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("by_attrs.trace_id")),
				sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("by_attrs.span_id")),
			),
		))
		for i, alias := range byAliases {
			colRef := fmt.Sprintf("by_attrs.%s", alias)
			displayAlias := fmt.Sprintf("by_custom_%d", i)
			selectCols = append(selectCols, sql.NewCol(sql.NewRawObject(colRef), displayAlias))
			groupByCols = append(groupByCols, sql.NewRawObject(displayAlias))
		}
	}

	query.Select(selectCols...).
		GroupBy(groupByCols...).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC))

	return query, resolvedByLabels, nil
}

// BuildMetricsInstantQuery builds a ClickHouse SQL query for an instant metrics query.
func BuildMetricsInstantQuery(
	fn string, attr string, byLabels []string,
	filterKeys []string, filterOps []string, filterVals []string,
	fromNS int64, toNS int64,
	tracesTable string, attrsTable string,
	distributed bool, percentile *float64,
) (sql.ISelect, []string, error) {

	windowS := float64(toNS-fromNS) / 1e9
	if windowS <= 0 {
		windowS = 1
	}

	valueExpr, err := buildAggExpr(fn, attr, windowS, percentile)
	if err != nil {
		return nil, nil, err
	}

	fromDate := time.Unix(fromNS/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(toNS/1e9, 0).Format("2006-01-02")

	selectCols := []sql.SQLObject{
		sql.NewCol(sql.NewRawObject(valueExpr), "value"),
	}
	groupByCols := []sql.SQLObject{}

	query := sql.NewSelect().
		From(sql.NewCol(sql.NewRawObject(tracesTable), "main")).
		AndWhere(
			sql.Ge(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(fromNS)),
			sql.Lt(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(toNS)),
		)

	// Add intrinsic filters
	for _, cond := range buildIntrinsicConditions(filterKeys, filterOps, filterVals) {
		query.AndWhere(cond)
	}

	if len(filterKeys) > 0 {
		filterWith := buildFilterCTE(filterKeys, filterOps, filterVals, fromDate, toDate, attrsTable)
		if filterWith != nil {
			query.AddWith(filterWith)
			query.AddJoin(sql.NewJoin("INNER",
				sql.NewCol(sql.NewWithRef(filterWith), "filtered"),
				sql.And(
					sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("filtered.trace_id")),
					sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("filtered.span_id")),
				),
			))
		}
	}

	resolvedByLabels := make([]string, 0, len(byLabels))
	for i, label := range byLabels {
		colName := resolveAttrColumn(label)
		if colName != "" {
			alias := fmt.Sprintf("by_%d", i)
			selectCols = append(selectCols, sql.NewCol(sql.NewRawObject("main."+colName), alias))
			groupByCols = append(groupByCols, sql.NewRawObject(alias))
			resolvedByLabels = append(resolvedByLabels, label)
		} else {
			resolvedByLabels = append(resolvedByLabels, label)
		}
	}

	customByLabels := make([]string, 0)
	for _, label := range byLabels {
		if resolveAttrColumn(label) == "" {
			customByLabels = append(customByLabels, label)
		}
	}
	if len(customByLabels) > 0 {
		byWith, byAliases := buildByPivotCTE(customByLabels, fromDate, toDate, attrsTable)
		query.AddWith(byWith)
		query.AddJoin(sql.NewJoin("LEFT",
			sql.NewCol(sql.NewWithRef(byWith), "by_attrs"),
			sql.And(
				sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("by_attrs.trace_id")),
				sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("by_attrs.span_id")),
			),
		))
		for i, alias := range byAliases {
			colRef := fmt.Sprintf("by_attrs.%s", alias)
			displayAlias := fmt.Sprintf("by_custom_%d", i)
			selectCols = append(selectCols, sql.NewCol(sql.NewRawObject(colRef), displayAlias))
			groupByCols = append(groupByCols, sql.NewRawObject(displayAlias))
		}
	}

	query.Select(selectCols...)
	if len(groupByCols) > 0 {
		query.GroupBy(groupByCols...)
	}

	return query, resolvedByLabels, nil
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
	return ""
}

// resolveAttrColumn resolves a TraceQL attribute to a direct tempo_traces column (intrinsics only).
func resolveAttrColumn(label string) string {
	if col, ok := IntrinsicAttr[label]; ok {
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

// buildFilterCTE builds a CTE that produces unique (trace_id, span_id) matching all filter conditions.
// The alias parameter controls the CTE name (e.g., "filtered", "outer_filtered").
func buildFilterCTE(keys []string, ops []string, vals []string, fromDate string, toDate string, attrsTable string, alias ...string) *sql.With {
	cteName := "filtered"
	if len(alias) > 0 && alias[0] != "" {
		cteName = alias[0]
	}

	// Copy slices to avoid mutating caller's data
	_keys := make([]string, len(keys))
	copy(_keys, keys)
	_vals := make([]string, len(vals))
	copy(_vals, vals)

	// Build an intersecting query: each filter condition as a subselect, INNER ANY JOINed
	var subqueries []sql.ISelect
	for i := range _keys {
		// Skip TraceQL-internal attributes that don't exist in the GIN table
		if strings.HasPrefix(_keys[i], "nestedSet") {
			continue
		}
		// Map root intrinsics to their span-level attrs_gin equivalents
		if _keys[i] == "rootServiceName" {
			_keys[i] = "service.name"
		} else if _keys[i] == "rootName" {
			_keys[i] = "name"
		}
		// Skip intrinsic keys that live in tempo_traces, not attrs_gin
		if isIntrinsicKey(_keys[i]) {
			continue
		}
		// For != nil, just check key existence (no val filter needed)
		if _vals[i] == "nil" {
			continue
		}
		ginKey := stripAttrPrefix(_keys[i])
		sub := sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
			From(sql.NewRawObject(attrsTable+" FINAL")).
			AndWhere(
				sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", fromDate))),
				sql.Le(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", toDate))),
				sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(ginKey)),
			)
		switch ops[i] {
		case "=":
			sub.AndWhere(sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(_vals[i])))
		case "!=":
			sub.AndWhere(sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(_vals[i])))
		case "=~":
			valCopy := _vals[i]
			sub.AndWhere(sql.Eq(
				sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
					return fmt.Sprintf("match(val, '%s')", strings.ReplaceAll(valCopy, "'", "\\'")), nil
				}),
				sql.NewRawObject("1"),
			))
		case "!~":
			valCopy := _vals[i]
			sub.AndWhere(sql.Neq(
				sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
					return fmt.Sprintf("match(val, '%s')", strings.ReplaceAll(valCopy, "'", "\\'")), nil
				}),
				sql.NewRawObject("1"),
			))
		case ">":
			sub.AndWhere(sql.Gt(sql.NewRawObject("val"), sql.NewStringVal(_vals[i])))
		case ">=":
			sub.AndWhere(sql.Ge(sql.NewRawObject("val"), sql.NewStringVal(_vals[i])))
		case "<":
			sub.AndWhere(sql.Lt(sql.NewRawObject("val"), sql.NewStringVal(_vals[i])))
		case "<=":
			sub.AndWhere(sql.Le(sql.NewRawObject("val"), sql.NewStringVal(_vals[i])))
		}
		subqueries = append(subqueries, sub)
	}

	if len(subqueries) == 0 {
		return nil
	}

	// Single filter: just use it directly
	if len(subqueries) == 1 {
		return sql.NewWith(subqueries[0], cteName)
	}

	// Multiple filters: intersect using INNER ANY JOIN
	result := sql.NewSelect().
		Select(sql.NewRawObject("s0.trace_id"), sql.NewRawObject("s0.span_id")).
		From(sql.NewCol(
			sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
				str, err := subqueries[0].String(ctx, options...)
				if err != nil {
					return "", err
				}
				return "(" + str + ")", nil
			}),
			"s0",
		))
	for i := 1; i < len(subqueries); i++ {
		alias := fmt.Sprintf("s%d", i)
		idx := i
		result.AddJoin(sql.NewJoin("INNER ANY",
			sql.NewCol(
				sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
					str, err := subqueries[idx].String(ctx, options...)
					if err != nil {
						return "", err
					}
					return "(" + str + ")", nil
				}),
				alias,
			),
			sql.And(
				sql.Eq(sql.NewRawObject("s0.trace_id"), sql.NewRawObject(alias+".trace_id")),
				sql.Eq(sql.NewRawObject("s0.span_id"), sql.NewRawObject(alias+".span_id")),
			),
		))
	}
	return sql.NewWith(result, cteName)
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

// HistogramRequest holds parameters for histogram_over_time().
type HistogramRequest struct {
	FromNS      int64
	ToNS        int64
	StepNS      int64
	TracesTable string
	AttrsTable  string
	Attr        string // e.g. "duration"
	FilterKeys  []string
	FilterOps   []string
	FilterVals  []string
}

// BuildHistogramRangeQuery builds a ClickHouse SQL query for histogram_over_time().
// Returns rows of (ts, bucket_ns, count) where bucket_ns is the power-of-2
// upper bound of each span's duration, computed inline.
func BuildHistogramRangeQuery(req *HistogramRequest) (sql.ISelect, error) {
	col := resolveAttrForAgg(req.Attr)
	if col == "" {
		return nil, fmt.Errorf("histogram_over_time requires an attribute argument")
	}
	colName := strings.TrimPrefix(col, "main.")

	fromDate := time.Unix(req.FromNS/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(req.ToNS/1e9, 0).Format("2006-01-02")

	bucketExpr := fmt.Sprintf(
		"if(%s > 0, bitShiftLeft(toUInt64(1), toUInt8(ceil(log2(%s)))), toUInt64(0))",
		colName, colName,
	)
	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)

	query := sql.NewSelect().
		Select(
			sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
			sql.NewCol(sql.NewRawObject(bucketExpr), "bucket_ns"),
			sql.NewCol(sql.NewRawObject("count(*)"), "cnt"),
		).
		From(sql.NewCol(sql.NewRawObject(req.TracesTable), "main")).
		AndWhere(
			sql.Ge(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(req.FromNS)),
			sql.Lt(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(req.ToNS)),
		).
		GroupBy(sql.NewRawObject("ts"), sql.NewRawObject("bucket_ns")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("bucket_ns"), sql.ORDER_BY_DIRECTION_ASC),
		)

	for _, cond := range buildIntrinsicConditions(req.FilterKeys, req.FilterOps, req.FilterVals) {
		query.AndWhere(cond)
	}

	if len(req.FilterKeys) > 0 {
		filterWith := buildFilterCTE(req.FilterKeys, req.FilterOps, req.FilterVals,
			fromDate, toDate, req.AttrsTable)
		if filterWith != nil {
			query.AddWith(filterWith)
			query.AddJoin(sql.NewJoin("INNER",
				sql.NewCol(sql.NewWithRef(filterWith), "filtered"),
				sql.And(
					sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("filtered.trace_id")),
					sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("filtered.span_id")),
				),
			))
		}
	}

	return query, nil
}

// BuildHistogramExemplarsQuery builds a query to sample one span per (ts_bucket, duration_bucket)
// for use as exemplars in the histogram response.
func BuildHistogramExemplarsQuery(req *HistogramRequest) (sql.ISelect, error) {
	col := resolveAttrForAgg(req.Attr)
	if col == "" {
		return nil, fmt.Errorf("histogram_over_time requires an attribute argument")
	}
	colName := strings.TrimPrefix(col, "main.")

	fromDate := time.Unix(req.FromNS/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(req.ToNS/1e9, 0).Format("2006-01-02")

	bucketExpr := fmt.Sprintf(
		"if(%s > 0, bitShiftLeft(toUInt64(1), toUInt8(ceil(log2(%s)))), toUInt64(0))",
		colName, colName,
	)
	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)

	query := sql.NewSelect().
		Select(
			sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
			sql.NewCol(sql.NewRawObject(bucketExpr), "bucket_ns"),
			sql.NewCol(sql.NewRawObject("any(lower(hex(main.trace_id)))"), "trace_id"),
			sql.NewCol(sql.NewRawObject(fmt.Sprintf("any(%s)", colName)), "dur_ns"),
			sql.NewCol(sql.NewRawObject("any(main.timestamp_ns)"), "span_ts"),
		).
		From(sql.NewCol(sql.NewRawObject(req.TracesTable), "main")).
		AndWhere(
			sql.Ge(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(req.FromNS)),
			sql.Lt(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(req.ToNS)),
		).
		GroupBy(sql.NewRawObject("ts"), sql.NewRawObject("bucket_ns")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("bucket_ns"), sql.ORDER_BY_DIRECTION_ASC),
		)

	for _, cond := range buildIntrinsicConditions(req.FilterKeys, req.FilterOps, req.FilterVals) {
		query.AndWhere(cond)
	}

	if len(req.FilterKeys) > 0 {
		filterWith := buildFilterCTE(req.FilterKeys, req.FilterOps, req.FilterVals,
			fromDate, toDate, req.AttrsTable)
		if filterWith != nil {
			query.AddWith(filterWith)
			query.AddJoin(sql.NewJoin("INNER",
				sql.NewCol(sql.NewWithRef(filterWith), "filtered"),
				sql.And(
					sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("filtered.trace_id")),
					sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("filtered.span_id")),
				),
			))
		}
	}

	return query, nil
}

// HistogramExemplar represents a single exemplar data point for histogram series.
type HistogramExemplar struct {
	Labels      []model.MetricsKeyValue `json:"labels"`
	Value       float64                 `json:"value"`
	TimestampMs string                  `json:"timestampMs"`
}

// GenericExemplarsRequest holds parameters for building exemplar queries for
// non-histogram metrics (rate, quantile, avg, min, max, sum, count).
type GenericExemplarsRequest struct {
	FromNS      int64
	ToNS        int64
	StepNS      int64
	TracesTable string
	AttrsTable  string
	FilterKeys  []string
	FilterOps   []string
	FilterVals  []string
	ByLabels    []string // group-by labels for per-series exemplar sampling
}

// BuildGenericExemplarsQuery builds a query that samples one span per time bucket
// for use as exemplars in rate/quantile/avg/etc. responses.
func BuildGenericExemplarsQuery(req *GenericExemplarsRequest) (sql.ISelect, []string, error) {
	fromDate := time.Unix(req.FromNS/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(req.ToNS/1e9, 0).Format("2006-01-02")

	tsBucket := fmt.Sprintf("intDiv(main.timestamp_ns, %d) * %d", req.StepNS, req.StepNS)

	selectCols := []sql.SQLObject{
		sql.NewCol(sql.NewRawObject(tsBucket), "ts"),
		sql.NewCol(sql.NewRawObject("any(lower(hex(main.trace_id)))"), "trace_id"),
		sql.NewCol(sql.NewRawObject("any(main.duration_ns)"), "dur_ns"),
		sql.NewCol(sql.NewRawObject("any(main.timestamp_ns)"), "span_ts"),
	}

	groupByCols := []sql.SQLObject{
		sql.NewRawObject("ts"),
	}

	query := sql.NewSelect().
		From(sql.NewCol(sql.NewRawObject(req.TracesTable), "main")).
		AndWhere(
			sql.Ge(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(req.FromNS)),
			sql.Lt(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(req.ToNS)),
		)

	for _, cond := range buildIntrinsicConditions(req.FilterKeys, req.FilterOps, req.FilterVals) {
		query.AndWhere(cond)
	}

	if len(req.FilterKeys) > 0 {
		filterWith := buildFilterCTE(req.FilterKeys, req.FilterOps, req.FilterVals,
			fromDate, toDate, req.AttrsTable)
		if filterWith != nil {
			query.AddWith(filterWith)
			query.AddJoin(sql.NewJoin("INNER",
				sql.NewCol(sql.NewWithRef(filterWith), "filtered"),
				sql.And(
					sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("filtered.trace_id")),
					sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("filtered.span_id")),
				),
			))
		}
	}

	resolvedByLabels := make([]string, 0, len(req.ByLabels))
	var customByLabels []string

	for i, label := range req.ByLabels {
		resolvedByLabels = append(resolvedByLabels, label)
		colName := resolveAttrColumn(label)
		if colName != "" {
			alias := fmt.Sprintf("by_%d", i)
			selectCols = append(selectCols, sql.NewCol(sql.NewRawObject("main."+colName), alias))
			groupByCols = append(groupByCols, sql.NewRawObject(alias))
		} else {
			customByLabels = append(customByLabels, label)
		}
	}

	if len(customByLabels) > 0 {
		byWith, byAliases := buildByPivotCTE(customByLabels, fromDate, toDate, req.AttrsTable)
		query.AddWith(byWith)
		query.AddJoin(sql.NewJoin("LEFT",
			sql.NewCol(sql.NewWithRef(byWith), "by_attrs"),
			sql.And(
				sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("by_attrs.trace_id")),
				sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("by_attrs.span_id")),
			),
		))
		for i, alias := range byAliases {
			colRef := fmt.Sprintf("by_attrs.%s", alias)
			displayAlias := fmt.Sprintf("by_custom_%d", i)
			selectCols = append(selectCols, sql.NewCol(sql.NewRawObject(colRef), displayAlias))
			groupByCols = append(groupByCols, sql.NewRawObject(displayAlias))
		}
	}

	query.Select(selectCols...).
		GroupBy(groupByCols...).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("ts"), sql.ORDER_BY_DIRECTION_ASC))

	return query, resolvedByLabels, nil
}

// CompareRequest holds parsed parameters for the compare() function.
type CompareRequest struct {
	FromNS          int64
	ToNS            int64
	BaselineFromNS  int64 // optional: explicit baseline window start (from compare() args)
	BaselineToNS    int64 // optional: explicit baseline window end (from compare() args)
	TracesTable string
	AttrsTable  string
	Distributed bool
	OuterKeys   []string
	OuterOps    []string
	OuterVals   []string
	InnerKeys   []string
	InnerOps    []string
	InnerVals   []string
	TopN        int
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
	// Baseline window: use explicit baseline args when provided, otherwise fall back to request window.
	baselineFromNS := req.FromNS
	baselineToNS := req.ToNS
	if req.BaselineFromNS > 0 {
		baselineFromNS = req.BaselineFromNS
	}
	if req.BaselineToNS > 0 {
		baselineToNS = req.BaselineToNS
	}

	// Expand to the widest date range for partition pruning.
	fromDate := time.Unix(min(req.FromNS, baselineFromNS)/1e9, 0).Format("2006-01-02")
	toDate := time.Unix(max(req.ToNS, baselineToNS)/1e9, 0).Format("2006-01-02")

	topN := req.TopN
	if topN <= 0 {
		topN = 10
	}

	// CTE: all_spans — baseline set: all candidate spans matching outer filters in the baseline window
	allSpansQuery := sql.NewSelect().
		Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		From(sql.NewCol(sql.NewRawObject(req.TracesTable), "main")).
		AndWhere(
			sql.Ge(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(baselineFromNS)),
			sql.Lt(sql.NewRawObject("main.timestamp_ns"), sql.NewIntVal(baselineToNS)),
		)

	for _, cond := range buildIntrinsicConditions(req.OuterKeys, req.OuterOps, req.OuterVals) {
		allSpansQuery.AndWhere(cond)
	}

	if len(req.OuterKeys) > 0 {
		outerWith := buildFilterCTE(req.OuterKeys, req.OuterOps, req.OuterVals,
			fromDate, toDate, req.AttrsTable, "outer_filtered")
		if outerWith != nil {
			allSpansQuery.AddWith(outerWith)
			allSpansQuery.AddJoin(sql.NewJoin("INNER",
				sql.NewCol(sql.NewWithRef(outerWith), "outer_f"),
				sql.And(
					sql.Eq(sql.NewRawObject("main.trace_id"), sql.NewRawObject("outer_f.trace_id")),
					sql.Eq(sql.NewRawObject("main.span_id"), sql.NewRawObject("outer_f.span_id")),
				),
			))
		}
	}
	allSpansWith := sql.NewWith(allSpansQuery, "all_spans")

	// CTE: selection — spans matching inner filter
	var selectionWith *sql.With
	if len(req.InnerKeys) > 0 {
		selectionQuery := sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
			From(sql.NewCol(sql.NewRawObject(req.TracesTable), "sel_main")).
			AndWhere(
				sql.Ge(sql.NewRawObject("sel_main.timestamp_ns"), sql.NewIntVal(req.FromNS)),
				sql.Lt(sql.NewRawObject("sel_main.timestamp_ns"), sql.NewIntVal(req.ToNS)),
			)

		for _, cond := range buildIntrinsicConditions(req.InnerKeys, req.InnerOps, req.InnerVals, "sel_main") {
			selectionQuery.AndWhere(cond)
		}

		innerWith := buildFilterCTE(req.InnerKeys, req.InnerOps, req.InnerVals,
			fromDate, toDate, req.AttrsTable, "inner_filtered")
		if innerWith != nil {
			selectionQuery.AddWith(innerWith)
			selectionQuery.AddJoin(sql.NewJoin("INNER",
				sql.NewCol(sql.NewWithRef(innerWith), "inner_f"),
				sql.And(
					sql.Eq(sql.NewRawObject("sel_main.trace_id"), sql.NewRawObject("inner_f.trace_id")),
					sql.Eq(sql.NewRawObject("sel_main.span_id"), sql.NewRawObject("inner_f.span_id")),
				),
			))
		}

		selectionWith = sql.NewWith(selectionQuery, "selection")
	}

	// Fallback: if no inner filter, everything is selection
	if selectionWith == nil {
		selectionWith = sql.NewWith(
			sql.NewSelect().
				Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
				From(sql.NewWithRef(allSpansWith)),
			"selection",
		)
	}

	// CTE: attrs_sub — filtered attrs_gin rows
	ginDateWhere := sql.And(
		sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", fromDate))),
		sql.Le(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", toDate))),
	)

	attrsSubquery := sql.NewSelect().
		Select(sql.NewRawObject("*")).
		From(sql.NewRawObject(req.AttrsTable+" FINAL")).
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
		AddWith(allSpansWith, selectionWith, attrsSubWith, countedWith, rankedWith).
		AndWhere(sql.Le(sql.NewRawObject("rn"), sql.NewIntVal(int64(topN)))).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("attr_key"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("selection_count + baseline_count"), sql.ORDER_BY_DIRECTION_DESC),
		)

	return query, nil
}

// parseTimeValNs parses a TraceQL time value string like "268ms", "1.5s", "500ns" to nanoseconds.
func parseTimeValNs(s string) (int64, error) {
	type unit struct {
		suffix string
		mult   int64
	}
	// Ordered longest-suffix-first so "ms" is checked before bare "s"
	units := []unit{
		{"ns", 1},
		{"us", 1_000},
		{"ms", 1_000_000},
		{"m", 60_000_000_000},
		{"s", 1_000_000_000},
		{"h", 3_600_000_000_000},
		{"d", 86_400_000_000_000},
	}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			numStr := strings.TrimSuffix(s, u.suffix)
			f, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid time value %q", s)
			}
			return int64(f * float64(u.mult)), nil
		}
	}
	// No unit: treat as raw nanosecond integer
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid time value %q: no unit and not an integer", s)
	}
	return n, nil
}

// buildIntrinsicConditions returns sql.SQLCondition items for intrinsic attributes.
// tableAlias controls the table prefix used for column references (default: "main").
func buildIntrinsicConditions(keys, ops, vals []string, tableAlias ...string) []sql.SQLCondition {
	prefix := "main"
	if len(tableAlias) > 0 && tableAlias[0] != "" {
		prefix = tableAlias[0]
	}

	stringIntrinsicMap := map[string]string{
		"name":            "name",
		"span:name":       "name",
		"rootName":        "name",
		"service.name":    "service_name",
		"rootServiceName": "service_name",
	}

	var conds []sql.SQLCondition
	for i := range keys {
		// __false__ sentinel: match nothing
		if keys[i] == "__false__" {
			conds = append(conds, sql.Eq(sql.NewRawObject("0"), sql.NewRawObject("1")))
			continue
		}
		// nestedSetParent<0 means root spans: parent_id = ''
		if keys[i] == "nestedSetParent" {
			conds = append(conds, sql.Eq(sql.NewRawObject(prefix+".parent_id"), sql.NewStringVal("")))
			continue
		}
		// duration / span:duration — compare against duration_ns column in nanoseconds
		if keys[i] == "duration" || keys[i] == "span:duration" {
			ns, err := parseTimeValNs(vals[i])
			if err != nil {
				continue
			}
			left := sql.NewRawObject(prefix + ".duration_ns")
			right := sql.NewIntVal(ns)
			switch ops[i] {
			case "=":
				conds = append(conds, sql.Eq(left, right))
			case "!=":
				conds = append(conds, sql.Neq(left, right))
			case ">":
				conds = append(conds, sql.Gt(left, right))
			case ">=":
				conds = append(conds, sql.Ge(left, right))
			case "<":
				conds = append(conds, sql.Lt(left, right))
			case "<=":
				conds = append(conds, sql.Le(left, right))
			}
			continue
		}
		col, ok := stringIntrinsicMap[keys[i]]
		if !ok {
			continue
		}
		val := vals[i]
		left := sql.NewRawObject(prefix + "." + col)
		right := sql.NewStringVal(val)
		switch ops[i] {
		case "=":
			conds = append(conds, sql.Eq(left, right))
		case "!=":
			conds = append(conds, sql.Neq(left, right))
		}
	}
	return conds
}

// isIntrinsicKey returns true if the key maps to a tempo_traces column, not attrs_gin.
func isIntrinsicKey(key string) bool {
	intrinsics := map[string]bool{
		"name": true, "span:name": true, "rootName": true,
		"rootServiceName": true,
		"duration": true, "span:duration": true,
	}
	return intrinsics[key]
}
