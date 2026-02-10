package traceql_metrics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/metrico/qryn/v4/reader/traceql/tempo"
)

// Engine executes TraceQL metrics queries.
type Engine struct {
	querier SpanQuerier
}

// SpanQuerier interface for counting spans matching a selector.
type SpanQuerier interface {
	// CountSpans returns the count of spans matching the selector in the time range.
	CountSpans(ctx context.Context, selector string, from, to time.Time) (int64, error)
	// CountSpansByAttribute returns span counts grouped by attribute value.
	CountSpansByAttribute(ctx context.Context, selector string, attr string, from, to time.Time) (map[string]int64, error)
	// GetDurationHistogram returns histogram buckets for span durations.
	GetDurationHistogram(ctx context.Context, selector string, from, to time.Time) (map[string]int64, error)
	// GetDurationQuantile returns a quantile of span durations.
	GetDurationQuantile(ctx context.Context, selector string, quantile float64, from, to time.Time) (float64, error)
	// GetDurationQuantileByAttribute returns quantile of span durations grouped by attribute.
	GetDurationQuantileByAttribute(ctx context.Context, selector string, quantile float64, attr string, from, to time.Time) (map[string]float64, error)
	// GetDurationStats returns min, max, avg, sum of span durations.
	GetDurationStats(ctx context.Context, selector string, from, to time.Time) (min, max, avg, sum float64, err error)
}

// ConditionedSpanQuerier extends SpanQuerier with methods that accept parsed conditions.
type ConditionedSpanQuerier interface {
	SpanQuerier
	CountSpansWithConditions(ctx context.Context, conds *SelectorConditions, from, to time.Time) (int64, error)
	CountSpansByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, attr string, from, to time.Time) (map[string]int64, error)
	GetDurationHistogramWithConditions(ctx context.Context, conds *SelectorConditions, from, to time.Time) (map[string]int64, error)
	GetDurationHistogramByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, attr string, from, to time.Time) (map[string]map[string]int64, error)
	GetDurationQuantileWithConditions(ctx context.Context, conds *SelectorConditions, quantile float64, from, to time.Time) (float64, error)
	GetDurationQuantileByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, quantile float64, attr string, from, to time.Time) (map[string]float64, error)
	GetDurationStatsWithConditions(ctx context.Context, conds *SelectorConditions, from, to time.Time) (min, max, avg, sum float64, err error)
	GetDurationStatsByAttributeWithConditions(ctx context.Context, conds *SelectorConditions, attr string, from, to time.Time) (map[string][4]float64, error)
}

// NewEngine creates a new metrics engine.
func NewEngine(querier SpanQuerier) *Engine {
	return &Engine{querier: querier}
}

// TimeSeries represents a single time series.
type TimeSeries struct {
	Labels map[string]string
	Values []float64
	Times  []int64 // Unix timestamps in milliseconds
}

// QueryRangeResult is the result of a range query.
type QueryRangeResult struct {
	Series []*TimeSeries
}

// QueryResult is the result of an instant query.
type QueryResult struct {
	Series []*TimeSeries
}

// ExecuteRange executes a metrics range query.
func (e *Engine) ExecuteRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (*QueryRangeResult, error) {
	// Parse the query
	ast, err := tempo.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Check if this is a metrics query
	if ast.MetricsPipeline == nil {
		return nil, fmt.Errorf("not a metrics query: missing metrics pipeline (e.g., | rate())")
	}

	// Get the metrics aggregate info
	metricsOp, by, err := extractMetricsInfo(ast)
	if err != nil {
		return nil, err
	}

	// Build selector conditions from the pipeline
	var selectorConds *SelectorConditions
	if len(ast.Pipeline.Elements) > 0 {
		selectorConds, err = BuildSelectorConditions(ast.Pipeline)
		if err != nil {
			return nil, fmt.Errorf("selector error: %w", err)
		}
	}

	// Calculate time windows
	windows := calculateWindows(start, end, step)
	if len(windows) == 0 {
		return &QueryRangeResult{}, nil
	}

	// Check if querier supports conditions
	condQuerier, hasConditions := e.querier.(ConditionedSpanQuerier)
	if !hasConditions {
		// Fallback to legacy interface without conditions
		selectorConds = nil
	}

	// Handle compare() - compares current vs baseline periods
	if metricsOp == OpCompare {
		result, err := e.executeCompare(ctx, ast, condQuerier, selectorConds, start, end, step)
		if err != nil {
			return nil, err
		}
		return e.applySecondStage(ast, result), nil
	}

	// Handle histogram_over_time specially - returns multiple series (one per bucket)
	if metricsOp == OpHistogramOverTime {
		result, err := e.executeRangeHistogramWithConds(ctx, condQuerier, selectorConds, windows, by)
		if err != nil {
			return nil, err
		}
		return e.applySecondStage(ast, result), nil
	}

	// Handle duration-based stats (min, max, avg, sum)
	if metricsOp == OpMinOverTime || metricsOp == OpMaxOverTime ||
		metricsOp == OpAvgOverTime || metricsOp == OpSumOverTime {
		result, err := e.executeRangeStatsWithConds(ctx, condQuerier, selectorConds, windows, metricsOp, by)
		if err != nil {
			return nil, err
		}
		return e.applySecondStage(ast, result), nil
	}

	// Handle quantile_over_time
	if metricsOp == OpQuantileOverTime {
		quantile := extractQuantileValue(ast)
		var result *QueryRangeResult
		var err error
		if len(by) == 0 {
			result, err = e.executeRangeQuantileWithConds(ctx, condQuerier, selectorConds, windows, quantile)
		} else {
			// With grouping - multiple series
			result, err = e.executeRangeQuantileGroupedWithConds(ctx, condQuerier, selectorConds, windows, quantile, by)
		}
		if err != nil {
			return nil, err
		}
		return e.applySecondStage(ast, result), nil
	}

	// Execute query for each window (rate, count_over_time)
	if len(by) == 0 {
		// No grouping - single series
		series, err := e.executeRangeSimpleWithConds(ctx, condQuerier, selectorConds, windows, step, metricsOp)
		if err != nil {
			return nil, err
		}
		result := &QueryRangeResult{Series: []*TimeSeries{series}}
		return e.applySecondStage(ast, result), nil
	}

	// With grouping - multiple series
	seriesMap, err := e.executeRangeGroupedWithConds(ctx, condQuerier, selectorConds, windows, step, metricsOp, by)
	if err != nil {
		return nil, err
	}

	result := &QueryRangeResult{Series: make([]*TimeSeries, 0, len(seriesMap))}
	for _, s := range seriesMap {
		result.Series = append(result.Series, s)
	}
	return e.applySecondStage(ast, result), nil
}

// ExecuteInstant executes a metrics instant query.
func (e *Engine) ExecuteInstant(ctx context.Context, query string, ts time.Time) (*QueryResult, error) {
	// For instant query, use a small window around the timestamp
	step := time.Minute
	start := ts.Add(-step)
	end := ts

	rangeResult, err := e.ExecuteRange(ctx, query, start, end, step)
	if err != nil {
		return nil, err
	}

	// Convert to instant result (take last value from each series)
	result := &QueryResult{Series: make([]*TimeSeries, 0, len(rangeResult.Series))}
	for _, s := range rangeResult.Series {
		if len(s.Values) > 0 {
			instantSeries := &TimeSeries{
				Labels: s.Labels,
				Values: []float64{s.Values[len(s.Values)-1]},
				Times:  []int64{s.Times[len(s.Times)-1]},
			}
			result.Series = append(result.Series, instantSeries)
		}
	}
	return result, nil
}

// Methods with selector conditions support

func (e *Engine) executeRangeSimpleWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, step time.Duration, op MetricsOp) (*TimeSeries, error) {
	series := &TimeSeries{
		Labels: map[string]string{},
		Values: make([]float64, len(windows)),
		Times:  make([]int64, len(windows)),
	}

	for i, w := range windows {
		var count int64
		var err error
		if querier != nil {
			count, err = querier.CountSpansWithConditions(ctx, conds, w.start, w.end)
		} else {
			count, err = e.querier.CountSpans(ctx, "", w.start, w.end)
		}
		if err != nil {
			return nil, fmt.Errorf("query error for window %d: %w", i, err)
		}

		series.Times[i] = w.start.UnixMilli()
		series.Values[i] = calculateValue(count, step, op)
	}

	return series, nil
}

func (e *Engine) executeRangeGroupedWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, step time.Duration, op MetricsOp, by []string) (map[string]*TimeSeries, error) {
	seriesMap := make(map[string]*TimeSeries)
	attr := by[0]

	for i, w := range windows {
		var counts map[string]int64
		var err error
		if querier != nil {
			counts, err = querier.CountSpansByAttributeWithConditions(ctx, conds, attr, w.start, w.end)
		} else {
			counts, err = e.querier.CountSpansByAttribute(ctx, "", attr, w.start, w.end)
		}
		if err != nil {
			return nil, fmt.Errorf("query error for window %d: %w", i, err)
		}

		for attrValue, count := range counts {
			key := fmt.Sprintf("%s=%s", attr, attrValue)
			s, ok := seriesMap[key]
			if !ok {
				s = &TimeSeries{
					Labels: map[string]string{normalizeAttrKey(attr): attrValue},
					Values: make([]float64, len(windows)),
					Times:  make([]int64, len(windows)),
				}
				for j, tw := range windows {
					s.Times[j] = tw.start.UnixMilli()
				}
				seriesMap[key] = s
			}
			s.Values[i] = calculateValue(count, step, op)
		}
	}

	return seriesMap, nil
}

func (e *Engine) executeRangeHistogramWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, by []string) (*QueryRangeResult, error) {
	if len(by) > 0 {
		return e.executeRangeHistogramGroupedWithConds(ctx, querier, conds, windows, by)
	}

	buckets := []string{"0.005", "0.010", "0.025", "0.050", "0.100", "0.250", "0.500", "1.000", "2.500", "5.000", "10.000", "+Inf"}

	seriesMap := make(map[string]*TimeSeries)
	for _, bucket := range buckets {
		seriesMap[bucket] = &TimeSeries{
			Labels: map[string]string{"le": bucket},
			Values: make([]float64, len(windows)),
			Times:  make([]int64, len(windows)),
		}
		for i, w := range windows {
			seriesMap[bucket].Times[i] = w.start.UnixMilli()
		}
	}

	for i, w := range windows {
		var histogram map[string]int64
		var err error
		if querier != nil {
			histogram, err = querier.GetDurationHistogramWithConditions(ctx, conds, w.start, w.end)
		} else {
			histogram, err = e.querier.GetDurationHistogram(ctx, "", w.start, w.end)
		}
		if err != nil {
			return nil, fmt.Errorf("histogram query error for window %d: %w", i, err)
		}

		for bucket, count := range histogram {
			if s, ok := seriesMap[bucket]; ok {
				s.Values[i] = float64(count)
			}
		}
	}

	result := &QueryRangeResult{Series: make([]*TimeSeries, 0, len(seriesMap))}
	for _, bucket := range buckets {
		result.Series = append(result.Series, seriesMap[bucket])
	}
	return result, nil
}

func (e *Engine) executeRangeHistogramGroupedWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, by []string) (*QueryRangeResult, error) {
	attr := by[0]

	// seriesMap key: "attrValue:bucket"
	seriesMap := make(map[string]*TimeSeries)

	for i, w := range windows {
		var histogramByAttr map[string]map[string]int64
		var err error
		if querier != nil {
			histogramByAttr, err = querier.GetDurationHistogramByAttributeWithConditions(ctx, conds, attr, w.start, w.end)
		} else {
			return nil, fmt.Errorf("grouped histogram not supported without conditioned querier")
		}
		if err != nil {
			return nil, fmt.Errorf("histogram grouped query error for window %d: %w", i, err)
		}

		for attrValue, histogram := range histogramByAttr {
			for bucket, count := range histogram {
				key := fmt.Sprintf("%s:%s", attrValue, bucket)
				s, ok := seriesMap[key]
				if !ok {
					s = &TimeSeries{
						Labels: map[string]string{
							normalizeAttrKey(attr): attrValue,
							"le":                   bucket,
						},
						Values: make([]float64, len(windows)),
						Times:  make([]int64, len(windows)),
					}
					for j, tw := range windows {
						s.Times[j] = tw.start.UnixMilli()
					}
					seriesMap[key] = s
				}
				s.Values[i] = float64(count)
			}
		}
	}

	result := &QueryRangeResult{Series: make([]*TimeSeries, 0, len(seriesMap))}
	for _, s := range seriesMap {
		result.Series = append(result.Series, s)
	}
	return result, nil
}

// executeCompare implements compare() which compares span counts between baseline and current periods.
// compare({filter}, topN, baselineStart, baselineEnd)
// - filter: spanset filter to match spans
// - topN: number of top changed items to return (default 10)
// - baselineStart/End: time offsets in seconds for baseline period (0 means use same offset as query range)
func (e *Engine) executeCompare(ctx context.Context, ast *tempo.RootExpr, querier ConditionedSpanQuerier, conds *SelectorConditions, start, end time.Time, step time.Duration) (*QueryRangeResult, error) {
	// Extract compare parameters from AST
	mc, ok := ast.MetricsPipeline.(*tempo.MetricsCompare)
	if !ok {
		return nil, fmt.Errorf("expected MetricsCompare in pipeline")
	}

	topN := mc.TopN()
	if topN <= 0 {
		topN = 10
	}

	// Calculate time ranges
	// Current period: [start, end]
	// Baseline period: if start/end offsets are 0, use previous period of same length
	queryDuration := end.Sub(start)
	baselineEnd := start
	baselineStart := baselineEnd.Add(-queryDuration)

	// If explicit offsets provided, use them (offsets are in seconds from query end time)
	if mc.Start() != 0 || mc.End() != 0 {
		baselineStart = end.Add(-time.Duration(mc.Start()) * time.Second)
		baselineEnd = end.Add(-time.Duration(mc.End()) * time.Second)
	}

	// Build conditions from compare filter if present
	var compareConds *SelectorConditions
	if mc.Filter() != nil && mc.Filter().Expression != nil {
		var err error
		// Create a temporary pipeline with just the filter
		filterPipeline := tempo.Pipeline{Elements: []tempo.PipelineElement{mc.Filter()}}
		compareConds, err = BuildSelectorConditions(filterPipeline)
		if err != nil {
			return nil, fmt.Errorf("compare filter error: %w", err)
		}
		// Merge with existing conditions
		if conds != nil && len(conds.Conditions) > 0 {
			compareConds.Conditions = append(compareConds.Conditions, conds.Conditions...)
			compareConds.NeedsJoin = compareConds.NeedsJoin || conds.NeedsJoin
		}
	} else if conds != nil {
		compareConds = conds
	}

	// Get counts for current and baseline periods
	var currentCount, baselineCount int64
	var err error

	if querier != nil {
		currentCount, err = querier.CountSpansWithConditions(ctx, compareConds, start, end)
		if err != nil {
			return nil, fmt.Errorf("current period count error: %w", err)
		}
		baselineCount, err = querier.CountSpansWithConditions(ctx, compareConds, baselineStart, baselineEnd)
		if err != nil {
			return nil, fmt.Errorf("baseline period count error: %w", err)
		}
	} else {
		currentCount, err = e.querier.CountSpans(ctx, "", start, end)
		if err != nil {
			return nil, fmt.Errorf("current period count error: %w", err)
		}
		baselineCount, err = e.querier.CountSpans(ctx, "", baselineStart, baselineEnd)
		if err != nil {
			return nil, fmt.Errorf("baseline period count error: %w", err)
		}
	}

	// Calculate diff and percentage change
	diff := float64(currentCount - baselineCount)
	var pctChange float64
	if baselineCount > 0 {
		pctChange = (diff / float64(baselineCount)) * 100
	}

	// Return comparison results as time series
	// Series 1: current count
	// Series 2: baseline count
	// Series 3: diff
	// Series 4: percentage change
	result := &QueryRangeResult{
		Series: []*TimeSeries{
			{
				Labels: map[string]string{"metric": "current"},
				Values: []float64{float64(currentCount)},
				Times:  []int64{end.UnixMilli()},
			},
			{
				Labels: map[string]string{"metric": "baseline"},
				Values: []float64{float64(baselineCount)},
				Times:  []int64{end.UnixMilli()},
			},
			{
				Labels: map[string]string{"metric": "diff"},
				Values: []float64{diff},
				Times:  []int64{end.UnixMilli()},
			},
			{
				Labels: map[string]string{"metric": "pct_change"},
				Values: []float64{pctChange},
				Times:  []int64{end.UnixMilli()},
			},
		},
	}

	return result, nil
}

func (e *Engine) executeRangeStatsWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, op MetricsOp, by []string) (*QueryRangeResult, error) {
	if len(by) > 0 {
		return e.executeRangeStatsGroupedWithConds(ctx, querier, conds, windows, op, by)
	}

	series := &TimeSeries{
		Labels: map[string]string{},
		Values: make([]float64, len(windows)),
		Times:  make([]int64, len(windows)),
	}

	for i, w := range windows {
		var minVal, maxVal, avgVal, sumVal float64
		var err error
		if querier != nil {
			minVal, maxVal, avgVal, sumVal, err = querier.GetDurationStatsWithConditions(ctx, conds, w.start, w.end)
		} else {
			minVal, maxVal, avgVal, sumVal, err = e.querier.GetDurationStats(ctx, "", w.start, w.end)
		}
		if err != nil {
			return nil, fmt.Errorf("stats query error for window %d: %w", i, err)
		}

		series.Times[i] = w.start.UnixMilli()
		switch op {
		case OpMinOverTime:
			series.Values[i] = minVal
		case OpMaxOverTime:
			series.Values[i] = maxVal
		case OpAvgOverTime:
			series.Values[i] = avgVal
		case OpSumOverTime:
			series.Values[i] = sumVal
		}
	}

	return &QueryRangeResult{Series: []*TimeSeries{series}}, nil
}

func (e *Engine) executeRangeStatsGroupedWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, op MetricsOp, by []string) (*QueryRangeResult, error) {
	seriesMap := make(map[string]*TimeSeries)
	attr := by[0]

	for i, w := range windows {
		var stats map[string][4]float64
		var err error
		if querier != nil {
			stats, err = querier.GetDurationStatsByAttributeWithConditions(ctx, conds, attr, w.start, w.end)
		} else {
			return nil, fmt.Errorf("grouped stats not supported without conditioned querier")
		}
		if err != nil {
			return nil, fmt.Errorf("stats query error for window %d: %w", i, err)
		}

		for attrValue, statVals := range stats {
			key := fmt.Sprintf("%s=%s", attr, attrValue)
			s, ok := seriesMap[key]
			if !ok {
				s = &TimeSeries{
					Labels: map[string]string{normalizeAttrKey(attr): attrValue},
					Values: make([]float64, len(windows)),
					Times:  make([]int64, len(windows)),
				}
				for j, tw := range windows {
					s.Times[j] = tw.start.UnixMilli()
				}
				seriesMap[key] = s
			}
			switch op {
			case OpMinOverTime:
				s.Values[i] = statVals[0]
			case OpMaxOverTime:
				s.Values[i] = statVals[1]
			case OpAvgOverTime:
				s.Values[i] = statVals[2]
			case OpSumOverTime:
				s.Values[i] = statVals[3]
			}
		}
	}

	result := &QueryRangeResult{Series: make([]*TimeSeries, 0, len(seriesMap))}
	for _, s := range seriesMap {
		result.Series = append(result.Series, s)
	}
	return result, nil
}

func (e *Engine) executeRangeQuantileWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, quantile float64) (*QueryRangeResult, error) {
	series := &TimeSeries{
		Labels: map[string]string{"quantile": fmt.Sprintf("%.2f", quantile)},
		Values: make([]float64, len(windows)),
		Times:  make([]int64, len(windows)),
	}

	for i, w := range windows {
		var val float64
		var err error
		if querier != nil {
			val, err = querier.GetDurationQuantileWithConditions(ctx, conds, quantile, w.start, w.end)
		} else {
			val, err = e.querier.GetDurationQuantile(ctx, "", quantile, w.start, w.end)
		}
		if err != nil {
			return nil, fmt.Errorf("quantile query error for window %d: %w", i, err)
		}

		series.Times[i] = w.start.UnixMilli()
		series.Values[i] = val
	}

	return &QueryRangeResult{Series: []*TimeSeries{series}}, nil
}

func (e *Engine) executeRangeQuantileGroupedWithConds(ctx context.Context, querier ConditionedSpanQuerier, conds *SelectorConditions, windows []timeWindow, quantile float64, by []string) (*QueryRangeResult, error) {
	seriesMap := make(map[string]*TimeSeries)
	attr := by[0]

	for i, w := range windows {
		var quantiles map[string]float64
		var err error
		if querier != nil {
			quantiles, err = querier.GetDurationQuantileByAttributeWithConditions(ctx, conds, quantile, attr, w.start, w.end)
		} else {
			quantiles, err = e.querier.GetDurationQuantileByAttribute(ctx, "", quantile, attr, w.start, w.end)
		}
		if err != nil {
			return nil, fmt.Errorf("quantile grouped query error for window %d: %w", i, err)
		}

		for attrValue, q := range quantiles {
			key := fmt.Sprintf("%s=%s", attr, attrValue)
			s, ok := seriesMap[key]
			if !ok {
				s = &TimeSeries{
					Labels: map[string]string{normalizeAttrKey(attr): attrValue, "quantile": fmt.Sprintf("%.2f", quantile)},
					Values: make([]float64, len(windows)),
					Times:  make([]int64, len(windows)),
				}
				for j, tw := range windows {
					s.Times[j] = tw.start.UnixMilli()
				}
				seriesMap[key] = s
			}
			s.Values[i] = q
		}
	}

	result := &QueryRangeResult{Series: make([]*TimeSeries, 0, len(seriesMap))}
	for _, s := range seriesMap {
		result.Series = append(result.Series, s)
	}
	return result, nil
}

// applySecondStage applies topk/bottomk filtering to results if present in AST.
func (e *Engine) applySecondStage(ast *tempo.RootExpr, result *QueryRangeResult) *QueryRangeResult {
	if ast.MetricsSecondStage == nil {
		return result
	}

	tkbk, ok := ast.MetricsSecondStage.(*tempo.TopKBottomK)
	if !ok {
		return result
	}

	k := tkbk.K()
	if k <= 0 || len(result.Series) <= k {
		return result
	}

	// Calculate average value for each series to rank them
	type seriesWithAvg struct {
		series *TimeSeries
		avg    float64
	}

	seriesAvgs := make([]seriesWithAvg, 0, len(result.Series))
	for _, s := range result.Series {
		var sum float64
		var count int
		for _, v := range s.Values {
			if v != 0 { // Ignore zero values
				sum += v
				count++
			}
		}
		avg := 0.0
		if count > 0 {
			avg = sum / float64(count)
		}
		seriesAvgs = append(seriesAvgs, seriesWithAvg{series: s, avg: avg})
	}

	// Sort based on operator
	if tkbk.OrderBy() == tempo.OpTopK {
		// Sort descending for topk
		sort.Slice(seriesAvgs, func(i, j int) bool {
			return seriesAvgs[i].avg > seriesAvgs[j].avg
		})
	} else {
		// Sort ascending for bottomk
		sort.Slice(seriesAvgs, func(i, j int) bool {
			return seriesAvgs[i].avg < seriesAvgs[j].avg
		})
	}

	// Take first k
	filteredSeries := make([]*TimeSeries, 0, k)
	for i := 0; i < k && i < len(seriesAvgs); i++ {
		filteredSeries = append(filteredSeries, seriesAvgs[i].series)
	}

	return &QueryRangeResult{Series: filteredSeries}
}

// MetricsOp represents the type of metrics operation.
type MetricsOp int

const (
	OpRate MetricsOp = iota
	OpCountOverTime
	OpMinOverTime
	OpMaxOverTime
	OpAvgOverTime
	OpSumOverTime
	OpHistogramOverTime
	OpQuantileOverTime
	OpCompare
)

// extractQuantileValue extracts the quantile value from quantile_over_time(attr, q).
// Returns 0.5 as default if not found.
func extractQuantileValue(ast *tempo.RootExpr) float64 {
	if ast.MetricsPipeline == nil {
		return 0.5
	}
	// Try type assertion to MetricsAggregate
	if ma, ok := ast.MetricsPipeline.(*tempo.MetricsAggregate); ok {
		floats := ma.Floats()
		if len(floats) > 0 {
			return floats[0]
		}
	}
	return 0.5
}

func extractMetricsInfo(ast *tempo.RootExpr) (MetricsOp, []string, error) {
	if ast.MetricsPipeline == nil {
		return 0, nil, fmt.Errorf("no metrics pipeline")
	}

	var op MetricsOp
	var by []string

	switch m := ast.MetricsPipeline.(type) {
	case *tempo.MetricsAggregate:
		// Extract operation from aggregate using String() method
		switch m.Op().String() {
		case "rate":
			op = OpRate
		case "count_over_time":
			op = OpCountOverTime
		case "min_over_time":
			op = OpMinOverTime
		case "max_over_time":
			op = OpMaxOverTime
		case "avg_over_time":
			op = OpAvgOverTime
		case "sum_over_time":
			op = OpSumOverTime
		case "histogram_over_time":
			op = OpHistogramOverTime
		case "quantile_over_time":
			op = OpQuantileOverTime
		default:
			return 0, nil, fmt.Errorf("unsupported metrics operation: %v", m.Op())
		}
		// Convert []Attribute to []string
		for _, attr := range m.By() {
			by = append(by, attr.String())
		}
	case *tempo.MetricsCompare:
		op = OpCompare
	default:
		return 0, nil, fmt.Errorf("unsupported metrics pipeline type: %T", ast.MetricsPipeline)
	}

	return op, by, nil
}

func calculateValue(count int64, step time.Duration, op MetricsOp) float64 {
	switch op {
	case OpRate:
		// Rate = count / step_in_seconds
		return float64(count) / step.Seconds()
	case OpCountOverTime:
		return float64(count)
	default:
		return float64(count)
	}
}

type timeWindow struct {
	start time.Time
	end   time.Time
}

func calculateWindows(start, end time.Time, step time.Duration) []timeWindow {
	var windows []timeWindow
	for t := start; t.Before(end); t = t.Add(step) {
		windowEnd := t.Add(step)
		if windowEnd.After(end) {
			windowEnd = end
		}
		windows = append(windows, timeWindow{start: t, end: windowEnd})
	}
	return windows
}

