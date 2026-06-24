package service

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/tempo"
	traceql_parser "github.com/metrico/qryn/v4/reader/traceql/traceql_parser"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/metrico/qryn/v4/reader/utils/tables"
)

// MetricsQueryRange executes a TraceQL metrics range query and returns time series.
func (t *TempoService) MetricsQueryRange(ctx context.Context, req *model.MetricsQueryRequest) (*model.MetricsQueryRangeResponse, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	distributed := conn.Config.ClusterName != ""
	tracesTable := fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces"))
	attrsTable := fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces_attrs_gin"))
	if distributed {
		tracesTable = fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces_dist"))
		attrsTable = fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces_attrs_gin_dist"))
	}

	script, err := traceql_parser.Parse(req.Query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}

	metricsFn := script.ResolvedMetricsFn()
	if metricsFn == nil {
		return nil, fmt.Errorf("query must contain a metrics function (e.g. | rate())")
	}

	// compare() has its own query and response format
	if metricsFn.Fn == "compare" {
		return t.executeCompareRange(ctx, script, metricsFn, req, conn, tracesTable, attrsTable)
	}

	// histogram_over_time has its own query and response format (heatmap buckets)
	if metricsFn.Fn == "histogram_over_time" {
		return t.executeHistogramRange(ctx, script, metricsFn, req, conn, tracesTable, attrsTable)
	}

	filterKeys, filterOps, filterVals := metricsExtractAllFilters(script)
	var byLabels []string
	if metricsFn.By != nil {
		byLabels = metricsFn.By.Labels
	}

	fromNS := req.From.UnixNano()
	toNS := req.To.UnixNano()
	stepNS := req.Step.Nanoseconds()

	query, resolvedByLabels, err := tempo.BuildMetricsRangeQuery(
		metricsFn.Fn, metricsFn.Attr, byLabels,
		filterKeys, filterOps, filterVals,
		fromNS, toNS, stepNS,
		tracesTable, attrsTable, distributed, metricsFn.Percentile,
	)
	if err != nil {
		return nil, err
	}

	strQuery, err := query.String(&sql.Ctx{})
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(ctx, strQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Group results into time series by label combination
	seriesMap := make(map[string]*model.MetricsTimeSeries)
	byCount := len(resolvedByLabels)
	hasByLabels := byCount > 0

	for rows.Next() {
		var ts int64
		var value float64
		byVals := make([]string, byCount)

		scanArgs := make([]interface{}, 2+byCount)
		scanArgs[0] = &ts
		scanArgs[1] = &value
		for i := range byVals {
			scanArgs[2+i] = &byVals[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		key := strings.Join(byVals, "\x00")
		series, ok := seriesMap[key]
		if !ok {
			var labels []model.MetricsKeyValue
			if !hasByLabels {
				switch metricsFn.Fn {
				case "rate", "count_over_time":
					labels = append(labels, model.MetricsKeyValue{
						Key:   "__name__",
						Value: model.MetricsLabelValue{StringValue: metricsFn.Fn},
					})
				case "quantile_over_time":
					p := 0.5
					if metricsFn.Percentile != nil {
						p = *metricsFn.Percentile
					}
					labels = append(labels, model.MetricsKeyValue{
						Key:   "p",
						Value: model.MetricsLabelValue{DoubleValue: &p},
					})
				default:
					labels = append(labels, model.MetricsKeyValue{
						Key:   "__name__",
						Value: model.MetricsLabelValue{StringValue: metricsFn.Fn},
					})
				}
			}
			for i, label := range resolvedByLabels {
				labels = append(labels, model.MetricsKeyValue{
					Key:   label,
					Value: model.MetricsLabelValue{StringValue: byVals[i]},
				})
			}

			var promParts []string
			for _, lbl := range labels {
				if lbl.Value.DoubleValue != nil {
					promParts = append(promParts, fmt.Sprintf("%s=\"%g\"", lbl.Key, *lbl.Value.DoubleValue))
				} else {
					promParts = append(promParts, fmt.Sprintf("%s=\"%s\"", lbl.Key, lbl.Value.StringValue))
				}
			}
			promLabels := "{" + strings.Join(promParts, ", ") + "}"

			series = &model.MetricsTimeSeries{
				Labels:     labels,
				Samples:    make([]model.MetricsSample, 0),
				PromLabels: promLabels,
			}
			seriesMap[key] = series
		}

		series.Samples = append(series.Samples, model.MetricsSample{
			TimestampMs: strconv.FormatInt(ts/1e6, 10),
			Value:       value,
		})
	}

	// Zero-fill step-aligned timestamps
	stepMs := req.Step.Milliseconds()
	result := make([]model.MetricsTimeSeries, 0, len(seriesMap))
	if stepMs > 0 {
		fromMs := (req.From.UnixMilli() / stepMs) * stepMs
		toMs := req.To.UnixMilli()
		for _, s := range seriesMap {
			existing := make(map[string]model.MetricsSample, len(s.Samples))
			for _, sample := range s.Samples {
				existing[sample.TimestampMs] = sample
			}
			filled := make([]model.MetricsSample, 0)
			for ms := fromMs; ms <= toMs; ms += stepMs {
				tsStr := strconv.FormatInt(ms, 10)
				if sample, ok := existing[tsStr]; ok {
					filled = append(filled, sample)
				} else {
					filled = append(filled, model.MetricsSample{TimestampMs: tsStr})
				}
			}
			s.Samples = filled
			result = append(result, *s)
		}
	} else {
		for _, s := range seriesMap {
			result = append(result, *s)
		}
	}

	// Fetch exemplars — one sampled span per (time bucket, group-by key)
	exReq := &tempo.GenericExemplarsRequest{
		FromNS:      req.From.UnixNano(),
		ToNS:        req.To.UnixNano(),
		StepNS:      req.Step.Nanoseconds(),
		TracesTable: tracesTable,
		AttrsTable:  attrsTable,
		FilterKeys:  filterKeys,
		FilterOps:   filterOps,
		FilterVals:  filterVals,
		ByLabels:    byLabels,
	}
	exQuery, exByLabels, err := tempo.BuildGenericExemplarsQuery(exReq)
	if err == nil {
		exQueryStr, strErr := exQuery.String(sql.DefaultCtx())
		if strErr == nil {
			exRows, exErr := conn.Session.QueryCtx(ctx, exQueryStr)
			if exErr == nil {
				defer exRows.Close()
				type exRow struct {
					traceID  string
					durNs    int64
					spanTs   int64
					groupKey string
				}
				var exemplarRows []exRow
				exByCount := len(exByLabels)

				for exRows.Next() {
					var ts int64
					var traceID string
					var durNs int64
					var spanTs int64
					exByVals := make([]string, exByCount)

					scanArgs := make([]interface{}, 4+exByCount)
					scanArgs[0] = &ts
					scanArgs[1] = &traceID
					scanArgs[2] = &durNs
					scanArgs[3] = &spanTs
					for i := range exByVals {
						scanArgs[4+i] = &exByVals[i]
					}

					if err := exRows.Scan(scanArgs...); err != nil {
						continue
					}
					groupKey := strings.Join(exByVals, "\x00")
					exemplarRows = append(exemplarRows, exRow{traceID, durNs, spanTs, groupKey})
				}

				if len(exemplarRows) > 0 {
					includeDuration := metricsFn.Fn != "rate" && metricsFn.Fn != "count_over_time"

					exByGroup := make(map[string][]exRow)
					for _, ex := range exemplarRows {
						exByGroup[ex.groupKey] = append(exByGroup[ex.groupKey], ex)
					}

					for i := range result {
						var keyParts []string
						for _, lbl := range result[i].Labels {
							isBy := false
							for _, byLabel := range resolvedByLabels {
								if lbl.Key == byLabel {
									isBy = true
									break
								}
							}
							if isBy {
								keyParts = append(keyParts, lbl.Value.StringValue)
							}
						}
						seriesKey := strings.Join(keyParts, "\x00")

						groupExemplars := exByGroup[seriesKey]
						if len(exByLabels) == 0 {
							groupExemplars = exemplarRows
						}

						tsToValue := make(map[int64]float64)
						for _, sample := range result[i].Samples {
							ms, _ := strconv.ParseInt(sample.TimestampMs, 10, 64)
							tsToValue[ms] = sample.Value
						}

						var exList []interface{}
						for _, ex := range groupExemplars {
							labels := []model.MetricsKeyValue{
								{Key: "trace:id", Value: model.MetricsLabelValue{StringValue: ex.traceID}},
							}
							if hasByLabels {
								for _, lbl := range result[i].Labels {
									for _, byLabel := range resolvedByLabels {
										if lbl.Key == byLabel {
											labels = append(labels, lbl)
										}
									}
								}
							} else {
								labels = append(labels, model.MetricsKeyValue{
									Key: "nestedSetParent", Value: model.MetricsLabelValue{IntValue: "-1"},
								})
							}
							exTsMs := ex.spanTs / 1e6
							val := float64(0)
							if stepMs > 0 {
								bucketMs := (exTsMs / stepMs) * stepMs
								val = tsToValue[bucketMs]
							}
							if includeDuration {
								labels = append(labels, model.MetricsKeyValue{
									Key: "duration", Value: model.MetricsLabelValue{StringValue: metricsFormatDuration(ex.durNs)},
								})
								val = float64(ex.durNs) / 1e9
							}
							exList = append(exList, tempo.HistogramExemplar{
								Labels:      labels,
								Value:       val,
								TimestampMs: strconv.FormatInt(exTsMs, 10),
							})
						}
						result[i].Exemplars = exList
					}
				}
			}
		}
	}

	return &model.MetricsQueryRangeResponse{
		Series:  result,
		Metrics: []byte(`{"completedJobs":1}`),
	}, nil
}

// MetricsQueryInstant executes a TraceQL metrics instant query.
func (t *TempoService) MetricsQueryInstant(ctx context.Context, req *model.MetricsQueryRequest) (*model.MetricsQueryInstantResponse, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	distributed := conn.Config.ClusterName != ""
	tracesTable := fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces"))
	attrsTable := fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces_attrs_gin"))
	if distributed {
		tracesTable = fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces_dist"))
		attrsTable = fmt.Sprintf("`%s`.%s", conn.Config.Name, tables.GetTableName("tempo_traces_attrs_gin_dist"))
	}

	script, err := traceql_parser.Parse(req.Query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}

	metricsFn := script.ResolvedMetricsFn()
	if metricsFn == nil {
		return nil, fmt.Errorf("query must contain a metrics function (e.g. | rate())")
	}

	if metricsFn.Fn == "compare" {
		return t.executeCompareInstant(ctx, script, metricsFn, req, conn, tracesTable, attrsTable)
	}

	if metricsFn.Fn == "histogram_over_time" {
		return &model.MetricsQueryInstantResponse{Series: []model.MetricsInstantSeries{}}, nil
	}

	filterKeys, filterOps, filterVals := metricsExtractAllFilters(script)
	var byLabels []string
	if metricsFn.By != nil {
		byLabels = metricsFn.By.Labels
	}

	fromNS := req.From.UnixNano()
	toNS := req.To.UnixNano()

	query, resolvedByLabels, err := tempo.BuildMetricsInstantQuery(
		metricsFn.Fn, metricsFn.Attr, byLabels,
		filterKeys, filterOps, filterVals,
		fromNS, toNS,
		tracesTable, attrsTable, distributed, metricsFn.Percentile,
	)
	if err != nil {
		return nil, err
	}

	strQuery, err := query.String(&sql.Ctx{})
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(ctx, strQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byCount := len(resolvedByLabels)
	var result []model.MetricsInstantSeries

	for rows.Next() {
		var value float64
		byVals := make([]string, byCount)

		scanArgs := make([]interface{}, 1+byCount)
		scanArgs[0] = &value
		for i := range byVals {
			scanArgs[1+i] = &byVals[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		labels := make([]model.MetricsKeyValue, byCount)
		for i, label := range resolvedByLabels {
			labels[i] = model.MetricsKeyValue{
				Key:   label,
				Value: model.MetricsLabelValue{StringValue: byVals[i]},
			}
		}

		result = append(result, model.MetricsInstantSeries{
			Labels:    labels,
			Value:     value,
			Exemplars: make([]interface{}, 0),
		})
	}

	if result == nil {
		result = []model.MetricsInstantSeries{}
	}

	return &model.MetricsQueryInstantResponse{
		Series:  result,
		Metrics: []byte(`{}`),
	}, nil
}

// metricsExtractAllFilters walks both sides of structural queries to collect all filters.
func metricsExtractAllFilters(script *traceql_parser.TraceQLScript) (keys []string, ops []string, vals []string) {
	if script == nil {
		return
	}
	if script.ParenExpr != nil {
		k, o, v := metricsExtractAllFilters(script.ParenExpr)
		keys = append(keys, k...)
		ops = append(ops, o...)
		vals = append(vals, v...)
	}
	if script.Head.AttrSelector != nil {
		metricsWalkAttrSelector(script.Head.AttrSelector, &keys, &ops, &vals)
	}
	if script.Tail != nil {
		k, o, v := metricsExtractAllFilters(script.Tail)
		keys = append(keys, k...)
		ops = append(ops, o...)
		vals = append(vals, v...)
	}
	return
}

func metricsWalkAttrSelector(exp *traceql_parser.AttrSelectorExp, keys *[]string, ops *[]string, vals *[]string) {
	if exp == nil {
		return
	}
	if exp.Head != nil {
		if exp.Head.Op == "" {
			if exp.Head.Label == "false" {
				*keys = append(*keys, "__false__")
				*ops = append(*ops, "=")
				*vals = append(*vals, "0")
			}
		} else {
			valStr := exp.Head.Val.String()
			if exp.Head.Val.StrVal != nil {
				unquoted, err := exp.Head.Val.StrVal.Unquote()
				if err == nil {
					valStr = unquoted
				}
			}
			*keys = append(*keys, exp.Head.Label)
			*ops = append(*ops, exp.Head.Op)
			*vals = append(*vals, valStr)
		}
	}
	if exp.ComplexHead != nil {
		metricsWalkAttrSelector(exp.ComplexHead, keys, ops, vals)
	}
	if exp.Tail != nil {
		metricsWalkAttrSelector(exp.Tail, keys, ops, vals)
	}
}

// executeHistogramRange handles histogram_over_time() queries.
func (t *TempoService) executeHistogramRange(
	ctx context.Context,
	script *traceql_parser.TraceQLScript,
	metricsFn *traceql_parser.MetricsPipelineStage,
	req *model.MetricsQueryRequest,
	conn *model.DataDatabasesMap,
	tracesTable, attrsTable string,
) (*model.MetricsQueryRangeResponse, error) {
	filterKeys, filterOps, filterVals := metricsExtractAllFilters(script)

	histReq := &tempo.HistogramRequest{
		FromNS:      req.From.UnixNano(),
		ToNS:        req.To.UnixNano(),
		StepNS:      req.Step.Nanoseconds(),
		TracesTable: tracesTable,
		AttrsTable:  attrsTable,
		Attr:        metricsFn.Attr,
		FilterKeys:  filterKeys,
		FilterOps:   filterOps,
		FilterVals:  filterVals,
	}

	histQuery, err := tempo.BuildHistogramRangeQuery(histReq)
	if err != nil {
		return nil, err
	}

	queryStr, err := histQuery.String(sql.DefaultCtx())
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Group by bucket_ns (power-of-2 upper bound) into separate series
	seriesMap := make(map[uint64]*model.MetricsTimeSeries)

	for rows.Next() {
		var ts int64
		var bucketNs uint64
		var count uint64
		if err := rows.Scan(&ts, &bucketNs, &count); err != nil {
			return nil, err
		}

		series, ok := seriesMap[bucketNs]
		if !ok {
			bucketVal := float64(bucketNs) / 1e9
			promLabel := fmt.Sprintf("{__bucket=\"%g\"}", bucketVal)
			series = &model.MetricsTimeSeries{
				Labels: []model.MetricsKeyValue{
					{Key: "__bucket", Value: model.MetricsLabelValue{DoubleValue: &bucketVal}},
				},
				PromLabels: promLabel,
				Samples:    make([]model.MetricsSample, 0),
			}
			seriesMap[bucketNs] = series
		}

		series.Samples = append(series.Samples, model.MetricsSample{
			TimestampMs: strconv.FormatInt(ts/1e6, 10),
			Value:       float64(count),
		})
	}

	// Build step-aligned timestamps and zero-fill missing steps
	stepMs := req.Step.Milliseconds()
	result := make([]model.MetricsTimeSeries, 0, len(seriesMap))
	if stepMs > 0 {
		fromMs := (req.From.UnixMilli() / stepMs) * stepMs
		toMs := req.To.UnixMilli()
		for _, s := range seriesMap {
			existing := make(map[string]model.MetricsSample, len(s.Samples))
			for _, sample := range s.Samples {
				existing[sample.TimestampMs] = sample
			}
			filled := make([]model.MetricsSample, 0)
			for ms := fromMs; ms <= toMs; ms += stepMs {
				tsStr := strconv.FormatInt(ms, 10)
				if sample, ok := existing[tsStr]; ok {
					filled = append(filled, sample)
				} else {
					filled = append(filled, model.MetricsSample{TimestampMs: tsStr})
				}
			}
			s.Samples = filled
			result = append(result, *s)
		}
	} else {
		for _, s := range seriesMap {
			result = append(result, *s)
		}
	}

	// Fetch exemplars — one sampled span per (ts, bucket)
	exQuery, err := tempo.BuildHistogramExemplarsQuery(histReq)
	if err == nil {
		exQueryStr, strErr := exQuery.String(sql.DefaultCtx())
		if strErr == nil {
			exRows, exErr := conn.Session.QueryCtx(ctx, exQueryStr)
			if exErr == nil {
				defer exRows.Close()
				type exRow struct {
					bucketNs uint64
					traceID  string
					durNs    int64
					spanTs   int64
				}
				var exemplarRows []exRow
				for exRows.Next() {
					var ts int64
					var bucketNs uint64
					var traceID string
					var durNs int64
					var spanTs int64
					if err := exRows.Scan(&ts, &bucketNs, &traceID, &durNs, &spanTs); err != nil {
						continue
					}
					exemplarRows = append(exemplarRows, exRow{bucketNs, traceID, durNs, spanTs})
				}
				for i := range result {
					if len(result[i].Labels) == 0 || result[i].Labels[0].Value.DoubleValue == nil {
						continue
					}
					bucketSec := *result[i].Labels[0].Value.DoubleValue
					var exList []interface{}
					for _, ex := range exemplarRows {
						if float64(ex.bucketNs)/1e9 != bucketSec {
							continue
						}
						exList = append(exList, tempo.HistogramExemplar{
							Labels: []model.MetricsKeyValue{
								{Key: "trace:id", Value: model.MetricsLabelValue{StringValue: ex.traceID}},
								{Key: "nestedSetParent", Value: model.MetricsLabelValue{IntValue: "-1"}},
								{Key: "duration", Value: model.MetricsLabelValue{StringValue: metricsFormatDuration(ex.durNs)}},
							},
							Value:       1,
							TimestampMs: strconv.FormatInt(ex.spanTs/1e6, 10),
						})
					}
					if len(exList) > 0 {
						result[i].Exemplars = exList
					}
				}
			}
		}
	}

	// Sort series by ascending bucket value
	sort.Slice(result, func(i, j int) bool {
		bi, bj := 0.0, 0.0
		if len(result[i].Labels) > 0 && result[i].Labels[0].Value.DoubleValue != nil {
			bi = *result[i].Labels[0].Value.DoubleValue
		}
		if len(result[j].Labels) > 0 && result[j].Labels[0].Value.DoubleValue != nil {
			bj = *result[j].Labels[0].Value.DoubleValue
		}
		return bi < bj
	})

	return &model.MetricsQueryRangeResponse{
		Series:  result,
		Metrics: []byte(`{"completedJobs":1}`),
	}, nil
}

// executeCompareRange handles compare() queries for range endpoint.
func (t *TempoService) executeCompareRange(
	ctx context.Context,
	script *traceql_parser.TraceQLScript,
	metricsFn *traceql_parser.MetricsPipelineStage,
	req *model.MetricsQueryRequest,
	conn *model.DataDatabasesMap,
	tracesTable, attrsTable string,
) (*model.MetricsQueryRangeResponse, error) {
	compareReq := t.metricsCompareRequest(script, metricsFn, req, tracesTable, attrsTable)

	compareQuery, err := tempo.BuildCompareQuery(compareReq)
	if err != nil {
		return nil, err
	}

	queryStr, err := compareQuery.String(sql.DefaultCtx())
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var series []model.MetricsTimeSeries
	midpointMs := (req.From.UnixMilli() + req.To.UnixMilli()) / 2

	for rows.Next() {
		var row tempo.CompareResultRow
		if err := rows.Scan(&row.Key, &row.Val, &row.SelectionCount, &row.BaselineCount); err != nil {
			return nil, err
		}

		if row.SelectionCount > 0 {
			series = append(series, model.MetricsTimeSeries{
				Labels: []model.MetricsKeyValue{
					{Key: "__meta_type", Value: model.MetricsLabelValue{StringValue: "selection"}},
					{Key: row.Key, Value: model.MetricsLabelValue{StringValue: row.Val}},
				},
				Samples:   []model.MetricsSample{{TimestampMs: strconv.FormatInt(midpointMs, 10), Value: float64(row.SelectionCount)}},
				Exemplars: []interface{}{},
			})
		}
		if row.BaselineCount > 0 {
			series = append(series, model.MetricsTimeSeries{
				Labels: []model.MetricsKeyValue{
					{Key: "__meta_type", Value: model.MetricsLabelValue{StringValue: "baseline"}},
					{Key: row.Key, Value: model.MetricsLabelValue{StringValue: row.Val}},
				},
				Samples:   []model.MetricsSample{{TimestampMs: strconv.FormatInt(midpointMs, 10), Value: float64(row.BaselineCount)}},
				Exemplars: []interface{}{},
			})
		}
	}

	if series == nil {
		series = []model.MetricsTimeSeries{}
	}

	return &model.MetricsQueryRangeResponse{Series: series}, nil
}

// executeCompareInstant handles compare() queries for instant endpoint.
func (t *TempoService) executeCompareInstant(
	ctx context.Context,
	script *traceql_parser.TraceQLScript,
	metricsFn *traceql_parser.MetricsPipelineStage,
	req *model.MetricsQueryRequest,
	conn *model.DataDatabasesMap,
	tracesTable, attrsTable string,
) (*model.MetricsQueryInstantResponse, error) {
	compareReq := t.metricsCompareRequest(script, metricsFn, req, tracesTable, attrsTable)

	compareQuery, err := tempo.BuildCompareQuery(compareReq)
	if err != nil {
		return nil, err
	}

	queryStr, err := compareQuery.String(sql.DefaultCtx())
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(ctx, queryStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var series []model.MetricsInstantSeries

	for rows.Next() {
		var row tempo.CompareResultRow
		if err := rows.Scan(&row.Key, &row.Val, &row.SelectionCount, &row.BaselineCount); err != nil {
			return nil, err
		}

		if row.SelectionCount > 0 {
			series = append(series, model.MetricsInstantSeries{
				Labels: []model.MetricsKeyValue{
					{Key: "__meta_type", Value: model.MetricsLabelValue{StringValue: "selection"}},
					{Key: row.Key, Value: model.MetricsLabelValue{StringValue: row.Val}},
				},
				Value: float64(row.SelectionCount),
			})
		}
		if row.BaselineCount > 0 {
			series = append(series, model.MetricsInstantSeries{
				Labels: []model.MetricsKeyValue{
					{Key: "__meta_type", Value: model.MetricsLabelValue{StringValue: "baseline"}},
					{Key: row.Key, Value: model.MetricsLabelValue{StringValue: row.Val}},
				},
				Value: float64(row.BaselineCount),
			})
		}
	}

	if series == nil {
		series = []model.MetricsInstantSeries{}
	}

	return &model.MetricsQueryInstantResponse{Series: series}, nil
}

// metricsCompareRequest constructs the CompareRequest from parsed query components.
func (t *TempoService) metricsCompareRequest(
	script *traceql_parser.TraceQLScript,
	metricsFn *traceql_parser.MetricsPipelineStage,
	req *model.MetricsQueryRequest,
	tracesTable, attrsTable string,
) *tempo.CompareRequest {
	outerKeys, outerOps, outerVals := metricsExtractAllFilters(script)

	var innerKeys, innerOps, innerVals []string
	if metricsFn.CompSel != nil && metricsFn.CompSel.AttrSelector != nil {
		metricsWalkAttrSelector(metricsFn.CompSel.AttrSelector, &innerKeys, &innerOps, &innerVals)
	}

	topN := 10
	if metricsFn.CompSel != nil && metricsFn.CompSel.Count > 0 {
		topN = metricsFn.CompSel.Count
	}

	baselineFromNS := int64(0)
	baselineToNS := int64(0)
	if metricsFn.CompSel != nil && metricsFn.CompSel.BaselineFrom != nil {
		baselineFromNS = *metricsFn.CompSel.BaselineFrom
	}
	if metricsFn.CompSel != nil && metricsFn.CompSel.BaselineTo != nil {
		baselineToNS = *metricsFn.CompSel.BaselineTo
	}

	return &tempo.CompareRequest{
		FromNS:         req.From.UnixNano(),
		ToNS:           req.To.UnixNano(),
		BaselineFromNS: baselineFromNS,
		BaselineToNS:   baselineToNS,
		TracesTable:    tracesTable,
		AttrsTable:     attrsTable,
		OuterKeys:      outerKeys,
		OuterOps:       outerOps,
		OuterVals:      outerVals,
		InnerKeys:      innerKeys,
		InnerOps:       innerOps,
		InnerVals:      innerVals,
		TopN:           topN,
	}
}

// metricsFormatDuration converts nanoseconds to a human-readable duration string.
func metricsFormatDuration(ns int64) string {
	if ns < 1e6 {
		return fmt.Sprintf("%dns", ns)
	}
	if ns < 1e9 {
		ms := float64(ns) / 1e6
		if ms == float64(int(ms)) {
			return fmt.Sprintf("%dms", int(ms))
		}
		return fmt.Sprintf("%.3gms", ms)
	}
	sec := float64(ns) / 1e9
	if sec < 60 {
		if sec == float64(int(sec)) {
			return fmt.Sprintf("%ds", int(sec))
		}
		return fmt.Sprintf("%.3gs", sec)
	}
	m := int(sec) / 60
	s := sec - float64(m*60)
	if s == 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%dm%gs", m, s)
}
