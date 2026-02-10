package traceql_metrics

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// DB interface for database operations (matches model.ISqlxDB).
type DB interface {
	QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ClickHouseQuerier implements SpanQuerier for ClickHouse.
type ClickHouseQuerier struct {
	db         DB
	table      string
	attrsTable string
	isCluster  bool
}

// NewClickHouseQuerier creates a new ClickHouse querier.
func NewClickHouseQuerier(db DB, table, attrsTable string, isCluster bool) *ClickHouseQuerier {
	return &ClickHouseQuerier{
		db:         db,
		table:      table,
		attrsTable: attrsTable,
		isCluster:  isCluster,
	}
}

// CountSpans counts spans matching the selector in the time range.
func (q *ClickHouseQuerier) CountSpans(ctx context.Context, selector string, from, to time.Time) (int64, error) {
	return q.CountSpansWithConditions(ctx, nil, from, to)
}

// CountSpansWithConditions counts spans with parsed selector conditions.
func (q *ClickHouseQuerier) CountSpansWithConditions(ctx context.Context, conds *SelectorConditions, from, to time.Time) (int64, error) {
	var query string

	if conds != nil && conds.NeedsJoin && len(conds.Conditions) > 0 {
		// Need to join with attributes table
		whereClause := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		query = fmt.Sprintf(`
			SELECT count() as cnt
			FROM %s t
			INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
			WHERE %s
		`, q.table, q.attrsTable, whereClause)
	} else if conds != nil && len(conds.Conditions) > 0 {
		// No join needed, just add conditions to traces table
		whereClause := fmt.Sprintf("timestamp_ns >= %d AND timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		query = fmt.Sprintf(`
			SELECT count() as cnt
			FROM %s
			WHERE %s
		`, q.table, whereClause)
	} else {
		// No conditions
		query = fmt.Sprintf(`
			SELECT count() as cnt
			FROM %s
			WHERE timestamp_ns >= %d
			  AND timestamp_ns < %d
		`, q.table, from.UnixNano(), to.UnixNano())
	}

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("count query failed: %w", err)
	}
	defer rows.Close()

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, fmt.Errorf("scan failed: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows error: %w", err)
	}

	return count, nil
}

// CountSpansByAttribute counts spans grouped by attribute value.
func (q *ClickHouseQuerier) CountSpansByAttribute(ctx context.Context, selector string, attr string, from, to time.Time) (map[string]int64, error) {
	return q.CountSpansByAttributeWithConditions(ctx, nil, attr, from, to)
}

// CountSpansByAttributeWithConditions counts spans grouped by attribute with selector conditions.
func (q *ClickHouseQuerier) CountSpansByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, attr string, from, to time.Time) (map[string]int64, error) {
	attrKey := normalizeAttrKey(attr)

	// Always need join for grouping by attribute
	baseWhere := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND a.key = '%s'",
		from.UnixNano(), to.UnixNano(), attrKey)

	var whereClause string
	if conds != nil && len(conds.Conditions) > 0 {
		whereClause = baseWhere + " AND " + strings.Join(conds.Conditions, " AND ")
	} else {
		whereClause = baseWhere
	}

	query := fmt.Sprintf(`
		SELECT
			a.val as attr_value,
			count() as cnt
		FROM %s t
		INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
		WHERE %s
		GROUP BY attr_value
	`, q.table, q.attrsTable, whereClause)

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("group by query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var attrValue string
		var count int64
		if err := rows.Scan(&attrValue, &count); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[attrValue] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

// normalizeAttrKey converts attribute references to key names and escapes for SQL.
func normalizeAttrKey(attr string) string {
	prefixes := []string{"resource.", "span.", "."}
	for _, prefix := range prefixes {
		if strings.HasPrefix(attr, prefix) {
			attr = attr[len(prefix):]
			break
		}
	}
	// Escape for safe SQL usage
	return escapeAttrKey(attr)
}

// escapeAttrKey escapes an attribute key for safe use in ClickHouse SQL queries.
func escapeAttrKey(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\x00", "")
	return s
}

// Default histogram buckets in seconds (Prometheus-compatible).
var defaultHistogramBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// GetDurationHistogram computes histogram buckets for span durations.
func (q *ClickHouseQuerier) GetDurationHistogram(ctx context.Context, selector string, from, to time.Time) (map[string]int64, error) {
	return q.GetDurationHistogramWithConditions(ctx, nil, from, to)
}

// GetDurationHistogramWithConditions computes histogram with selector conditions.
func (q *ClickHouseQuerier) GetDurationHistogramWithConditions(ctx context.Context, conds *SelectorConditions, from, to time.Time) (map[string]int64, error) {
	// Build CASE expression for histogram buckets
	caseExpr := "CASE "
	for _, bucket := range defaultHistogramBuckets {
		bucketNs := int64(bucket * 1e9)
		caseExpr += fmt.Sprintf("WHEN duration_ns <= %d THEN '%.3f' ", bucketNs, bucket)
	}
	caseExpr += "ELSE '+Inf' END"

	var query string
	if conds != nil && conds.NeedsJoin && len(conds.Conditions) > 0 {
		whereClause := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		// Use t.duration_ns for joined query
		caseExprT := strings.ReplaceAll(caseExpr, "duration_ns", "t.duration_ns")
		query = fmt.Sprintf(`
			SELECT
				%s as le,
				count() as cnt
			FROM %s t
			INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
			WHERE %s
			GROUP BY le
			ORDER BY le
		`, caseExprT, q.table, q.attrsTable, whereClause)
	} else if conds != nil && len(conds.Conditions) > 0 {
		whereClause := fmt.Sprintf("timestamp_ns >= %d AND timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		query = fmt.Sprintf(`
			SELECT
				%s as le,
				count() as cnt
			FROM %s
			WHERE %s
			GROUP BY le
			ORDER BY le
		`, caseExpr, q.table, whereClause)
	} else {
		query = fmt.Sprintf(`
			SELECT
				%s as le,
				count() as cnt
			FROM %s
			WHERE timestamp_ns >= %d
			  AND timestamp_ns < %d
			GROUP BY le
			ORDER BY le
		`, caseExpr, q.table, from.UnixNano(), to.UnixNano())
	}

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("histogram query failed: %w", err)
	}
	defer rows.Close()

	// Initialize all buckets to 0
	result := make(map[string]int64)
	for _, bucket := range defaultHistogramBuckets {
		result[fmt.Sprintf("%.3f", bucket)] = 0
	}
	result["+Inf"] = 0

	for rows.Next() {
		var le string
		var count int64
		if err := rows.Scan(&le, &count); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[le] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// Convert to cumulative histogram
	cumulative := make(map[string]int64)
	var total int64
	for _, bucket := range defaultHistogramBuckets {
		key := fmt.Sprintf("%.3f", bucket)
		total += result[key]
		cumulative[key] = total
	}
	total += result["+Inf"]
	cumulative["+Inf"] = total

	return cumulative, nil
}

// GetDurationHistogramByAttributeWithConditions computes histogram grouped by attribute with selector conditions.
func (q *ClickHouseQuerier) GetDurationHistogramByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, attr string, from, to time.Time) (map[string]map[string]int64, error) {
	attrKey := normalizeAttrKey(attr)

	// Build CASE expression for histogram buckets
	caseExpr := "CASE "
	for _, bucket := range defaultHistogramBuckets {
		bucketNs := int64(bucket * 1e9)
		caseExpr += fmt.Sprintf("WHEN t.duration_ns <= %d THEN '%.3f' ", bucketNs, bucket)
	}
	caseExpr += "ELSE '+Inf' END"

	baseWhere := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND a.key = '%s'",
		from.UnixNano(), to.UnixNano(), attrKey)

	var whereClause string
	if conds != nil && len(conds.Conditions) > 0 {
		whereClause = baseWhere + " AND " + strings.Join(conds.Conditions, " AND ")
	} else {
		whereClause = baseWhere
	}

	query := fmt.Sprintf(`
		SELECT
			a.val as attr_value,
			%s as le,
			count() as cnt
		FROM %s t
		INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
		WHERE %s
		GROUP BY attr_value, le
		ORDER BY attr_value, le
	`, caseExpr, q.table, q.attrsTable, whereClause)

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("histogram by attribute query failed: %w", err)
	}
	defer rows.Close()

	// Result: map[attrValue]map[bucket]count
	result := make(map[string]map[string]int64)

	for rows.Next() {
		var attrValue, le string
		var count int64
		if err := rows.Scan(&attrValue, &le, &count); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		if result[attrValue] == nil {
			// Initialize all buckets for this attribute
			result[attrValue] = make(map[string]int64)
			for _, bucket := range defaultHistogramBuckets {
				result[attrValue][fmt.Sprintf("%.3f", bucket)] = 0
			}
			result[attrValue]["+Inf"] = 0
		}
		result[attrValue][le] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// Convert to cumulative histogram for each attribute
	for attrValue, histogram := range result {
		cumulative := make(map[string]int64)
		var total int64
		for _, bucket := range defaultHistogramBuckets {
			key := fmt.Sprintf("%.3f", bucket)
			total += histogram[key]
			cumulative[key] = total
		}
		total += histogram["+Inf"]
		cumulative["+Inf"] = total
		result[attrValue] = cumulative
	}

	return result, nil
}

// GetDurationQuantile computes a quantile of span durations.
func (q *ClickHouseQuerier) GetDurationQuantile(ctx context.Context, selector string, quantile float64, from, to time.Time) (float64, error) {
	return q.GetDurationQuantileWithConditions(ctx, nil, quantile, from, to)
}

// GetDurationQuantileWithConditions computes quantile with selector conditions.
func (q *ClickHouseQuerier) GetDurationQuantileWithConditions(ctx context.Context, conds *SelectorConditions, quantile float64, from, to time.Time) (float64, error) {
	var query string

	if conds != nil && conds.NeedsJoin && len(conds.Conditions) > 0 {
		whereClause := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		query = fmt.Sprintf(`
			SELECT COALESCE(quantile(%f)(t.duration_ns) / 1e9, 0) as q
			FROM %s t
			INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
			WHERE %s
		`, quantile, q.table, q.attrsTable, whereClause)
	} else if conds != nil && len(conds.Conditions) > 0 {
		whereClause := fmt.Sprintf("timestamp_ns >= %d AND timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		query = fmt.Sprintf(`
			SELECT COALESCE(quantile(%f)(duration_ns) / 1e9, 0) as q
			FROM %s
			WHERE %s
		`, quantile, q.table, whereClause)
	} else {
		query = fmt.Sprintf(`
			SELECT COALESCE(quantile(%f)(duration_ns) / 1e9, 0) as q
			FROM %s
			WHERE timestamp_ns >= %d
			  AND timestamp_ns < %d
		`, quantile, q.table, from.UnixNano(), to.UnixNano())
	}

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("quantile query failed: %w", err)
	}
	defer rows.Close()

	var result float64
	if rows.Next() {
		if err := rows.Scan(&result); err != nil {
			return 0, fmt.Errorf("scan failed: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

// GetDurationQuantileByAttribute computes quantile of span durations grouped by attribute.
func (q *ClickHouseQuerier) GetDurationQuantileByAttribute(ctx context.Context, selector string, quantile float64, attr string, from, to time.Time) (map[string]float64, error) {
	return q.GetDurationQuantileByAttributeWithConditions(ctx, nil, quantile, attr, from, to)
}

// GetDurationQuantileByAttributeWithConditions computes quantile by attribute with selector conditions.
func (q *ClickHouseQuerier) GetDurationQuantileByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, quantile float64, attr string, from, to time.Time) (map[string]float64, error) {
	attrKey := normalizeAttrKey(attr)

	baseWhere := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND a.key = '%s'",
		from.UnixNano(), to.UnixNano(), attrKey)

	var whereClause string
	if conds != nil && len(conds.Conditions) > 0 {
		whereClause = baseWhere + " AND " + strings.Join(conds.Conditions, " AND ")
	} else {
		whereClause = baseWhere
	}

	query := fmt.Sprintf(`
		SELECT
			a.val as attr_value,
			COALESCE(quantile(%f)(t.duration_ns) / 1e9, 0) as q
		FROM %s t
		INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
		WHERE %s
		GROUP BY attr_value
	`, quantile, q.table, q.attrsTable, whereClause)

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("quantile by attribute query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]float64)
	for rows.Next() {
		var attrValue string
		var qVal float64
		if err := rows.Scan(&attrValue, &qVal); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[attrValue] = qVal
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

// GetDurationStats computes min, max, avg, sum of span durations.
func (q *ClickHouseQuerier) GetDurationStats(ctx context.Context, selector string, from, to time.Time) (min, max, avg, sum float64, err error) {
	return q.GetDurationStatsWithConditions(ctx, nil, from, to)
}

// GetDurationStatsWithConditions computes duration stats with selector conditions.
func (q *ClickHouseQuerier) GetDurationStatsWithConditions(ctx context.Context, conds *SelectorConditions, from, to time.Time) (min, max, avg, sum float64, err error) {
	var query string

	if conds != nil && conds.NeedsJoin && len(conds.Conditions) > 0 {
		whereClause := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		query = fmt.Sprintf(`
			SELECT
				COALESCE(min(t.duration_ns) / 1e9, 0) as min_dur,
				COALESCE(max(t.duration_ns) / 1e9, 0) as max_dur,
				COALESCE(avg(t.duration_ns) / 1e9, 0) as avg_dur,
				COALESCE(sum(t.duration_ns) / 1e9, 0) as sum_dur
			FROM %s t
			INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
			WHERE %s
		`, q.table, q.attrsTable, whereClause)
	} else if conds != nil && len(conds.Conditions) > 0 {
		whereClause := fmt.Sprintf("timestamp_ns >= %d AND timestamp_ns < %d AND %s",
			from.UnixNano(), to.UnixNano(), strings.Join(conds.Conditions, " AND "))
		query = fmt.Sprintf(`
			SELECT
				COALESCE(min(duration_ns) / 1e9, 0) as min_dur,
				COALESCE(max(duration_ns) / 1e9, 0) as max_dur,
				COALESCE(avg(duration_ns) / 1e9, 0) as avg_dur,
				COALESCE(sum(duration_ns) / 1e9, 0) as sum_dur
			FROM %s
			WHERE %s
		`, q.table, whereClause)
	} else {
		query = fmt.Sprintf(`
			SELECT
				COALESCE(min(duration_ns) / 1e9, 0) as min_dur,
				COALESCE(max(duration_ns) / 1e9, 0) as max_dur,
				COALESCE(avg(duration_ns) / 1e9, 0) as avg_dur,
				COALESCE(sum(duration_ns) / 1e9, 0) as sum_dur
			FROM %s
			WHERE timestamp_ns >= %d
			  AND timestamp_ns < %d
		`, q.table, from.UnixNano(), to.UnixNano())
	}

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("stats query failed: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&min, &max, &avg, &sum); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("scan failed: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("rows error: %w", err)
	}

	return min, max, avg, sum, nil
}

// GetDurationStatsByAttribute computes duration stats grouped by attribute.
func (q *ClickHouseQuerier) GetDurationStatsByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, attr string, from, to time.Time) (map[string][4]float64, error) {
	attrKey := normalizeAttrKey(attr)

	baseWhere := fmt.Sprintf("t.timestamp_ns >= %d AND t.timestamp_ns < %d AND a.key = '%s'",
		from.UnixNano(), to.UnixNano(), attrKey)

	var whereClause string
	if conds != nil && len(conds.Conditions) > 0 {
		whereClause = baseWhere + " AND " + strings.Join(conds.Conditions, " AND ")
	} else {
		whereClause = baseWhere
	}

	query := fmt.Sprintf(`
		SELECT
			a.val as attr_value,
			COALESCE(min(t.duration_ns) / 1e9, 0) as min_dur,
			COALESCE(max(t.duration_ns) / 1e9, 0) as max_dur,
			COALESCE(avg(t.duration_ns) / 1e9, 0) as avg_dur,
			COALESCE(sum(t.duration_ns) / 1e9, 0) as sum_dur
		FROM %s t
		INNER JOIN %s a ON t.trace_id = a.trace_id AND t.span_id = a.span_id
		WHERE %s
		GROUP BY attr_value
	`, q.table, q.attrsTable, whereClause)

	rows, err := q.db.QueryCtx(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("stats by attribute query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string][4]float64)
	for rows.Next() {
		var attrValue string
		var min, max, avg, sum float64
		if err := rows.Scan(&attrValue, &min, &max, &avg, &sum); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[attrValue] = [4]float64{min, max, avg, sum}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}
