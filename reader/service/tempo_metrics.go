package service

import (
	"cmp"
	"context"
	stdsql "database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/tempo"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// MetricsQueryRange executes a TraceQL metrics range query and returns time series.
func (t *TempoService) MetricsQueryRange(ctx context.Context, req *model.MetricsQueryRequest) (*model.MetricsQueryRangeResponse, error) {
	tq, planCtx, err := t.metricsPlannerContext(ctx, req)
	if err != nil {
		return nil, err
	}
	metricsFn := tq.Metrics

	// compare() has its own query and response format
	if metricsFn.Fn == "compare" {
		return executeCompareRange(tq, req, planCtx)
	}

	// histogram_over_time has its own query and response format (heatmap buckets)
	if metricsFn.Fn == "histogram_over_time" {
		return executeHistogramRange(tq, req, planCtx)
	}

	mreq := metricsRequest(metricsFn, req, planCtx)
	var resolvedByLabels []string
	planner, err := clickhouse_transpiler.PlanMetrics(tq, func(spans *sql.With) (sql.ISelect, error) {
		query, byLabels, err := tempo.BuildMetricsRangeQuery(over(mreq, spans))
		resolvedByLabels = byLabels
		return query, err
	})
	if err != nil {
		return nil, err
	}

	// Group results into time series by label combination
	seriesMap := make(map[string]*model.MetricsTimeSeries)
	hasByLabels := len(metricsFn.By) > 0

	err = queryMetrics(planCtx, planner, func(rows *stdsql.Rows) error {
		byCount := len(resolvedByLabels)
		var ts int64
		var value float64
		byVals := make([]string, byCount)

		scanArgs := make([]any, 2+byCount)
		scanArgs[0] = &ts
		scanArgs[1] = &value
		for i := range byVals {
			scanArgs[2+i] = &byVals[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return err
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
				labels = append(labels, byLabel(label, byVals[i]))
			}

			var promParts []string
			for _, lbl := range labels {
				if lbl.Value.DoubleValue != nil {
					promParts = append(promParts, fmt.Sprintf("%s=\"%g\"", lbl.Key, *lbl.Value.DoubleValue))
				} else {
					promParts = append(promParts, fmt.Sprintf("%s=\"%s\"", lbl.Key, labelText(lbl.Value)))
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
		return nil
	})
	if err != nil {
		return nil, err
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

	// Fetch exemplars — one result span per (time bucket, group-by key)
	type exRow struct {
		traceID  string
		durNs    int64
		spanTs   int64
		groupKey string
	}
	var exemplarRows []exRow
	var exByLabels []string
	exPlanner, err := clickhouse_transpiler.PlanMetrics(tq, func(spans *sql.With) (sql.ISelect, error) {
		query, byLabels, err := tempo.BuildGenericExemplarsQuery(over(mreq, spans))
		exByLabels = byLabels
		return query, err
	})
	if err == nil && wantsExemplars(tq) {
		_ = queryMetrics(planCtx, exPlanner, func(exRows *stdsql.Rows) error {
			var ts int64
			var traceID string
			var durNs int64
			var spanTs int64
			exByVals := make([]string, len(exByLabels))

			scanArgs := make([]any, 4+len(exByLabels))
			scanArgs[0] = &ts
			scanArgs[1] = &traceID
			scanArgs[2] = &durNs
			scanArgs[3] = &spanTs
			for i := range exByVals {
				scanArgs[4+i] = &exByVals[i]
			}

			if err := exRows.Scan(scanArgs...); err != nil {
				return nil
			}
			groupKey := strings.Join(exByVals, "\x00")
			exemplarRows = append(exemplarRows, exRow{traceID, durNs, spanTs, groupKey})
			return nil
		})
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
				isBy := slices.Contains(resolvedByLabels, lbl.Key)
				if isBy {
					keyParts = append(keyParts, labelText(lbl.Value))
				}
			}
			seriesKey := strings.Join(keyParts, "\x00")

			groupExemplars := exByGroup[seriesKey]
			if len(exByLabels) == 0 {
				groupExemplars = exemplarRows
			}
			groupExemplars = capExemplars(tq, groupExemplars)

			tsToValue := make(map[int64]float64)
			for _, sample := range result[i].Samples {
				ms, _ := strconv.ParseInt(sample.TimestampMs, 10, 64)
				tsToValue[ms] = sample.Value
			}

			var exList []any
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
						Key: traceql_ir.NestedSetParent, Value: model.MetricsLabelValue{IntValue: "-1"},
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

	return &model.MetricsQueryRangeResponse{
		Series:  result,
		Metrics: []byte(`{"completedJobs":1}`),
	}, nil
}

// MetricsQueryInstant executes a TraceQL metrics instant query.
func (t *TempoService) MetricsQueryInstant(ctx context.Context, req *model.MetricsQueryRequest) (*model.MetricsQueryInstantResponse, error) {
	tq, planCtx, err := t.metricsPlannerContext(ctx, req)
	if err != nil {
		return nil, err
	}
	metricsFn := tq.Metrics

	if metricsFn.Fn == "compare" {
		return executeCompareInstant(tq, req, planCtx)
	}

	if metricsFn.Fn == "histogram_over_time" {
		return &model.MetricsQueryInstantResponse{Series: []model.MetricsInstantSeries{}}, nil
	}

	mreq := metricsRequest(metricsFn, req, planCtx)
	var resolvedByLabels []string
	planner, err := clickhouse_transpiler.PlanMetrics(tq, func(spans *sql.With) (sql.ISelect, error) {
		query, byLabels, err := tempo.BuildMetricsInstantQuery(over(mreq, spans))
		resolvedByLabels = byLabels
		return query, err
	})
	if err != nil {
		return nil, err
	}

	var result []model.MetricsInstantSeries
	err = queryMetrics(planCtx, planner, func(rows *stdsql.Rows) error {
		byCount := len(resolvedByLabels)
		var value float64
		byVals := make([]string, byCount)

		scanArgs := make([]any, 1+byCount)
		scanArgs[0] = &value
		for i := range byVals {
			scanArgs[1+i] = &byVals[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		labels := make([]model.MetricsKeyValue, byCount)
		for i, label := range resolvedByLabels {
			labels[i] = byLabel(label, byVals[i])
		}

		result = append(result, model.MetricsInstantSeries{
			Labels:    labels,
			Value:     value,
			Exemplars: make([]any, 0),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	if result == nil {
		result = []model.MetricsInstantSeries{}
	}

	return &model.MetricsQueryInstantResponse{
		Series:  result,
		Metrics: []byte(`{}`),
	}, nil
}

// byLabel is a series label of a by() field: an int for a nested-set field,
// else a string.
func byLabel(key, val string) model.MetricsKeyValue {
	if traceql_ir.IsNestedSet(key) {
		return model.MetricsKeyValue{Key: key, Value: model.MetricsLabelValue{IntValue: val}}
	}
	return model.MetricsKeyValue{Key: key, Value: model.MetricsLabelValue{StringValue: val}}
}

// labelText is a string or int label value as text.
func labelText(v model.MetricsLabelValue) string {
	if v.IntValue != "" {
		return v.IntValue
	}
	return v.StringValue
}

// metricsPlannerContext parses the metrics query of req and resolves the
// planner context for its range.
func (t *TempoService) metricsPlannerContext(ctx context.Context, req *model.MetricsQueryRequest) (
	*traceql_ir.Query, *shared.PlannerContext, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, nil, err
	}
	tq, err := traceql_ir.Parse(req.Query)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid query: %w", err)
	}
	if tq.Metrics == nil {
		return nil, nil, fmt.Errorf("query must contain a metrics function (e.g. | rate())")
	}
	planCtx, err := traceQLPlannerContext(ctx, conn, req.From, req.To, 0)
	if err != nil {
		return nil, nil, err
	}
	return tq, planCtx, nil
}

// metricsRequest is the metrics request of fn over the planner context's range.
func metricsRequest(fn *traceql_ir.MetricsFn, req *model.MetricsQueryRequest, planCtx *shared.PlannerContext) tempo.MetricsRequest {
	return tempo.MetricsRequest{
		Fn:         fn.Fn,
		Attr:       fn.Attr,
		By:         fn.By,
		Percentile: fn.Percentile,
		FromNS:     planCtx.From.UnixNano(),
		ToNS:       planCtx.To.UnixNano(),
		StepNS:     req.Step.Nanoseconds(),
		AttrsTable: attrsTable(planCtx),
	}
}

// over returns req reading spans.
func over(req tempo.MetricsRequest, spans *sql.With) *tempo.MetricsRequest {
	req.Spans = spans
	return &req
}

// attrsTable is the span attributes table metrics read, the distributed one on a cluster.
func attrsTable(planCtx *shared.PlannerContext) string {
	if planCtx.IsCluster {
		return planCtx.TracesAttrsDistTable
	}
	return planCtx.TracesAttrsTable
}

// queryMetrics runs planner's query, passing each row to scan.
func queryMetrics(planCtx *shared.PlannerContext, planner shared.SQLRequestPlanner, scan func(*stdsql.Rows) error) error {
	query, err := planner.Process(planCtx)
	if err != nil {
		return err
	}
	str, err := query.String(sql.DefaultCtx())
	if err != nil {
		return err
	}
	rows, err := planCtx.CHDb.QueryCtx(planCtx.Ctx, str)
	if err != nil {
		return clickhouse_transpiler.UnresolvedAncestryError(planCtx, err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return clickhouse_transpiler.UnresolvedAncestryError(planCtx, rows.Err())
}

// executeHistogramRange handles histogram_over_time() queries.
func executeHistogramRange(
	tq *traceql_ir.Query,
	req *model.MetricsQueryRequest,
	planCtx *shared.PlannerContext,
) (*model.MetricsQueryRangeResponse, error) {
	mreq := metricsRequest(tq.Metrics, req, planCtx)
	planner, err := clickhouse_transpiler.PlanMetrics(tq, func(spans *sql.With) (sql.ISelect, error) {
		return tempo.BuildHistogramRangeQuery(over(mreq, spans))
	})
	if err != nil {
		return nil, err
	}

	// Group by bucket_ns (power-of-2 upper bound) into separate series
	seriesMap := make(map[uint64]*model.MetricsTimeSeries)

	err = queryMetrics(planCtx, planner, func(rows *stdsql.Rows) error {
		var ts int64
		var bucketNs uint64
		var count uint64
		if err := rows.Scan(&ts, &bucketNs, &count); err != nil {
			return err
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
		return nil
	})
	if err != nil {
		return nil, err
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

	// Fetch exemplars — one result span per (ts, bucket)
	type exRow struct {
		bucketNs uint64
		traceID  string
		durNs    int64
		spanTs   int64
	}
	var exemplarRows []exRow
	exPlanner, err := clickhouse_transpiler.PlanMetrics(tq, func(spans *sql.With) (sql.ISelect, error) {
		return tempo.BuildHistogramExemplarsQuery(over(mreq, spans))
	})
	if err == nil && wantsExemplars(tq) {
		_ = queryMetrics(planCtx, exPlanner, func(exRows *stdsql.Rows) error {
			var ts int64
			var bucketNs uint64
			var traceID string
			var durNs int64
			var spanTs int64
			if err := exRows.Scan(&ts, &bucketNs, &traceID, &durNs, &spanTs); err != nil {
				return nil
			}
			exemplarRows = append(exemplarRows, exRow{bucketNs, traceID, durNs, spanTs})
			return nil
		})
	}
	for i := range result {
		if len(result[i].Labels) == 0 || result[i].Labels[0].Value.DoubleValue == nil {
			continue
		}
		bucketSec := *result[i].Labels[0].Value.DoubleValue
		var exList []any
		for _, ex := range exemplarRows {
			if float64(ex.bucketNs)/1e9 != bucketSec {
				continue
			}
			exList = append(exList, tempo.HistogramExemplar{
				Labels: []model.MetricsKeyValue{
					{Key: "trace:id", Value: model.MetricsLabelValue{StringValue: ex.traceID}},
					{Key: traceql_ir.NestedSetParent, Value: model.MetricsLabelValue{IntValue: "-1"}},
					{Key: "duration", Value: model.MetricsLabelValue{StringValue: metricsFormatDuration(ex.durNs)}},
				},
				Value:       1,
				TimestampMs: strconv.FormatInt(ex.spanTs/1e6, 10),
			})
		}
		exList = capExemplars(tq, exList)
		if len(exList) > 0 {
			result[i].Exemplars = exList
		}
	}

	bucket := func(s model.MetricsTimeSeries) float64 {
		if len(s.Labels) > 0 && s.Labels[0].Value.DoubleValue != nil {
			return *s.Labels[0].Value.DoubleValue
		}
		return 0
	}
	// Sort series by ascending bucket value
	slices.SortFunc(result, func(a, b model.MetricsTimeSeries) int {
		return cmp.Compare(bucket(a), bucket(b))
	})

	return &model.MetricsQueryRangeResponse{
		Series:  result,
		Metrics: []byte(`{"completedJobs":1}`),
	}, nil
}

// wantsExemplars reports whether tq's exemplars hint allows any exemplar.
func wantsExemplars(tq *traceql_ir.Query) bool {
	limit, ok := tq.ExemplarLimit()
	return !ok || limit > 0
}

// capExemplars applies tq's exemplars hint to a series' exemplars, in time
// order: N of them, spread evenly, under exemplars=N, and every one without
// the hint.
func capExemplars[T any](tq *traceql_ir.Query, exemplars []T) []T {
	limit, ok := tq.ExemplarLimit()
	if !ok || len(exemplars) <= limit {
		return exemplars
	}
	if limit == 0 {
		return nil
	}
	res := make([]T, limit)
	for i := range res {
		res[i] = exemplars[(2*i+1)*len(exemplars)/(2*limit)]
	}
	return res
}

// executeCompareRange handles compare() queries for range endpoint.
func executeCompareRange(
	tq *traceql_ir.Query,
	req *model.MetricsQueryRequest,
	planCtx *shared.PlannerContext,
) (*model.MetricsQueryRangeResponse, error) {
	var series []model.MetricsTimeSeries
	midpointMs := (req.From.UnixMilli() + req.To.UnixMilli()) / 2

	err := queryCompare(tq, planCtx, func(row tempo.CompareResultRow) {
		if row.SelectionCount > 0 {
			series = append(series, model.MetricsTimeSeries{
				Labels: []model.MetricsKeyValue{
					{Key: "__meta_type", Value: model.MetricsLabelValue{StringValue: "selection"}},
					{Key: row.Key, Value: model.MetricsLabelValue{StringValue: row.Val}},
				},
				Samples:   []model.MetricsSample{{TimestampMs: strconv.FormatInt(midpointMs, 10), Value: float64(row.SelectionCount)}},
				Exemplars: []any{},
			})
		}
		if row.BaselineCount > 0 {
			series = append(series, model.MetricsTimeSeries{
				Labels: []model.MetricsKeyValue{
					{Key: "__meta_type", Value: model.MetricsLabelValue{StringValue: "baseline"}},
					{Key: row.Key, Value: model.MetricsLabelValue{StringValue: row.Val}},
				},
				Samples:   []model.MetricsSample{{TimestampMs: strconv.FormatInt(midpointMs, 10), Value: float64(row.BaselineCount)}},
				Exemplars: []any{},
			})
		}
	})
	if err != nil {
		return nil, err
	}

	if series == nil {
		series = []model.MetricsTimeSeries{}
	}

	return &model.MetricsQueryRangeResponse{Series: series}, nil
}

// executeCompareInstant handles compare() queries for instant endpoint.
func executeCompareInstant(
	tq *traceql_ir.Query,
	req *model.MetricsQueryRequest,
	planCtx *shared.PlannerContext,
) (*model.MetricsQueryInstantResponse, error) {
	var series []model.MetricsInstantSeries

	err := queryCompare(tq, planCtx, func(row tempo.CompareResultRow) {
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
	})
	if err != nil {
		return nil, err
	}

	if series == nil {
		series = []model.MetricsInstantSeries{}
	}

	return &model.MetricsQueryInstantResponse{Series: series}, nil
}

// queryCompare runs the compare() query of tq, passing each row to add.
func queryCompare(tq *traceql_ir.Query, planCtx *shared.PlannerContext, add func(tempo.CompareResultRow)) error {
	creq := compareRequest(tq.Metrics, planCtx)
	var selection traceql_ir.FieldExpr
	if tq.Metrics.Compare != nil {
		selection = tq.Metrics.Compare.Filter
	}
	planner, err := clickhouse_transpiler.PlanCompare(tq, selection,
		time.Unix(0, creq.BaselineFromNS), time.Unix(0, creq.BaselineToNS),
		func(baseline, selection *sql.With) (sql.ISelect, error) {
			creq.Baseline, creq.Selection = baseline, selection
			return tempo.BuildCompareQuery(&creq)
		})
	if err != nil {
		return err
	}
	return queryMetrics(planCtx, planner, func(rows *stdsql.Rows) error {
		var row tempo.CompareResultRow
		if err := rows.Scan(&row.Key, &row.Val, &row.SelectionCount, &row.BaselineCount); err != nil {
			return err
		}
		add(row)
		return nil
	})
}

// compareRequest is the compare() request of fn over the planner context's
// range. The baseline window defaults to that range.
func compareRequest(fn *traceql_ir.MetricsFn, planCtx *shared.PlannerContext) tempo.CompareRequest {
	res := tempo.CompareRequest{
		FromNS:         planCtx.From.UnixNano(),
		ToNS:           planCtx.To.UnixNano(),
		BaselineFromNS: planCtx.From.UnixNano(),
		BaselineToNS:   planCtx.To.UnixNano(),
		AttrsTable:     attrsTable(planCtx),
	}
	if c := fn.Compare; c != nil {
		res.TopN = c.Count
		if c.BaselineFrom != nil && *c.BaselineFrom > 0 {
			res.BaselineFromNS = *c.BaselineFrom
		}
		if c.BaselineTo != nil && *c.BaselineTo > 0 {
			res.BaselineToNS = *c.BaselineTo
		}
	}
	return res
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
