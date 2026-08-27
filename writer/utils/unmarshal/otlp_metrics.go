package unmarshal

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/metrico/qryn/v5/writer/model"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpmetrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

const noRecordedValueMask = 1

const maxRejectionReasons = 5

// OTLPMetricsStats accumulates per-request ingestion outcomes that the OTLP
// partial_success response reports: how many data points were rejected and
// why. Decode runs on a single goroutine and the parser response channel
// provides the happens-before edge, so plain fields suffice: read the stats
// only after the response channel is closed.
type OTLPMetricsStats struct {
	rejected int64
	reasons  []string
}

func (s *OTLPMetricsStats) reject(count int, reason string) {
	s.rejected += int64(count)
	for _, r := range s.reasons {
		if r == reason {
			return
		}
	}
	if len(s.reasons) < maxRejectionReasons {
		s.reasons = append(s.reasons, reason)
	}
}

// RejectedDataPoints returns the number of data points dropped by policy or
// validation, for the partial_success.rejected_data_points response field.
func (s *OTLPMetricsStats) RejectedDataPoints() int64 {
	return s.rejected
}

// ErrorMessage returns a human-readable summary of the distinct rejection
// reasons, for the partial_success.error_message response field.
func (s *OTLPMetricsStats) ErrorMessage() string {
	switch len(s.reasons) {
	case 0:
		return ""
	case 1:
		return s.reasons[0]
	}
	msg := s.reasons[0]
	for _, r := range s.reasons[1:] {
		msg += "; " + r
	}
	return msg
}

// identifyingResourceAttrs are the resource attributes that become the job and
// instance labels instead of target_info labels.
var identifyingResourceAttrs = map[string]bool{
	"service.name":        true,
	"service.namespace":   true,
	"service.instance.id": true,
}

type otlpMetricsDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler
	stats     *OTLPMetricsStats
}

func (d *otlpMetricsDec) SetOnEntries(h onEntriesHandler) {
	d.onEntries = h
}

// resourceScope carries the per-resource and per-scope label context every
// series in that scope inherits: job/instance identity, otel_scope_* labels,
// target_info labels, and the latest accepted sample timestamp used to emit
// target_info.
type resourceScope struct {
	job         string
	instance    string
	targetAttrs map[string]string
	scopeLabels [][2]string
	lastTs      int64
}

// mergeSanitizedAttrs writes attrs into dst keyed by prefix + SanitizeKey.
// Distinct attribute keys that sanitize to the same label name have their
// values concatenated with ";" in lexicographic order of the original keys,
// as the OTel Prometheus compatibility spec requires, instead of silently
// overwriting one another.
func mergeSanitizedAttrs(dst map[string]string, prefix string, attrs []*otlpcommon.KeyValue) {
	grouped := make(map[string][][2]string, len(attrs))
	for _, kv := range attrs {
		if kv.Value == nil {
			continue
		}
		key := prefix + SanitizeKey(kv.Key)
		grouped[key] = append(grouped[key], [2]string{kv.Key, SanitizeValue(kv.Value)})
	}
	for key, entries := range grouped {
		if len(entries) > 1 {
			sort.Slice(entries, func(i, j int) bool { return entries[i][0] < entries[j][0] })
		}
		val := entries[0][1]
		for _, e := range entries[1:] {
			val += ";" + e[1]
		}
		dst[key] = val
	}
}

func (d *otlpMetricsDec) Decode() error {
	md := d.ctx.bodyObject.(*otlpmetrics.MetricsData)

	for _, rm := range md.ResourceMetrics {
		rs := &resourceScope{targetAttrs: map[string]string{}}
		if rm.Resource != nil {
			var serviceName, serviceNamespace string
			var extraAttrs []*otlpcommon.KeyValue
			for _, kv := range rm.Resource.Attributes {
				if kv.Value == nil {
					continue
				}
				switch kv.Key {
				case "service.name":
					serviceName = SanitizeValue(kv.Value)
				case "service.namespace":
					serviceNamespace = SanitizeValue(kv.Value)
				case "service.instance.id":
					rs.instance = SanitizeValue(kv.Value)
				default:
					extraAttrs = append(extraAttrs, kv)
				}
			}
			mergeSanitizedAttrs(rs.targetAttrs, "", extraAttrs)
			rs.job = serviceName
			if serviceNamespace != "" && serviceName != "" {
				rs.job = serviceNamespace + "/" + serviceName
			}
		}

		for _, sm := range rm.ScopeMetrics {
			rs.scopeLabels = rs.scopeLabels[:0]
			if sm.Scope != nil {
				if sm.Scope.Name != "" {
					rs.scopeLabels = append(rs.scopeLabels, [2]string{"otel_scope_name", sm.Scope.Name})
				}
				if sm.Scope.Version != "" {
					rs.scopeLabels = append(rs.scopeLabels, [2]string{"otel_scope_version", sm.Scope.Version})
				}
				scopeAttrs := make(map[string]string, len(sm.Scope.Attributes))
				mergeSanitizedAttrs(scopeAttrs, "otel_scope_", sm.Scope.Attributes)
				for k, v := range scopeAttrs {
					switch k {
					case "otel_scope_name", "otel_scope_version", "otel_scope_schema_url":
						// A scope attribute that collides with the scope's own
						// identity labels MUST be dropped, per the OTel
						// Prometheus compatibility spec.
						continue
					}
					rs.scopeLabels = append(rs.scopeLabels, [2]string{k, v})
				}
			}
			if sm.SchemaUrl != "" {
				rs.scopeLabels = append(rs.scopeLabels, [2]string{"otel_scope_schema_url", sm.SchemaUrl})
			}
			for _, metric := range sm.Metrics {
				if err := d.decodeMetric(metric, rs); err != nil {
					return err
				}
			}
		}

		if err := d.emitTargetInfo(rs); err != nil {
			return err
		}
	}
	return nil
}

func (d *otlpMetricsDec) decodeMetric(metric *otlpmetrics.Metric, rs *resourceScope) error {
	switch data := metric.Data.(type) {
	case *otlpmetrics.Metric_Gauge:
		name := buildMetricName(metric.Name, metric.Unit, false)
		return d.decodeNumberPoints(data.Gauge.DataPoints, name, "gauge", metric, rs)
	case *otlpmetrics.Metric_Sum:
		if !d.checkTemporality(metric.Name, data.Sum.AggregationTemporality, len(data.Sum.DataPoints)) {
			return nil
		}
		metricType := "gauge"
		if data.Sum.IsMonotonic {
			metricType = "counter"
		}
		name := buildMetricName(metric.Name, metric.Unit, data.Sum.IsMonotonic)
		return d.decodeNumberPoints(data.Sum.DataPoints, name, metricType, metric, rs)
	case *otlpmetrics.Metric_Histogram:
		if !d.checkTemporality(metric.Name, data.Histogram.AggregationTemporality, len(data.Histogram.DataPoints)) {
			return nil
		}
		name := buildMetricName(metric.Name, metric.Unit, false)
		return d.decodeHistogram(data.Histogram.DataPoints, name, metric, rs)
	case *otlpmetrics.Metric_ExponentialHistogram:
		if !d.checkTemporality(metric.Name, data.ExponentialHistogram.AggregationTemporality, len(data.ExponentialHistogram.DataPoints)) {
			return nil
		}
		name := buildMetricName(metric.Name, metric.Unit, false)
		return d.decodeExponentialHistogram(data.ExponentialHistogram.DataPoints, name, metric, rs)
	case *otlpmetrics.Metric_Summary:
		name := buildMetricName(metric.Name, metric.Unit, false)
		return d.decodeSummary(data.Summary.DataPoints, name, metric, rs)
	default:
		d.stats.reject(0, fmt.Sprintf("metric %q: unknown data type", metric.Name))
	}
	return nil
}

// checkTemporality enforces the cumulative-only storage model: data points
// with delta or unspecified temporality are rejected and counted, never
// stored, because PromQL rate()/increase() over raw delta values is wrong.
func (d *otlpMetricsDec) checkTemporality(name string, t otlpmetrics.AggregationTemporality, points int) bool {
	if t == otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return true
	}
	d.stats.reject(points, fmt.Sprintf("metric %q: unsupported aggregation temporality %s, only cumulative is supported", name, t.String()))
	return false
}

// seriesLabels assembles the full label set of one stored series. Precedence,
// lowest to highest: scope labels, data point attributes, identity labels
// (__name__, job, instance) and explicitly passed extras (le, quantile),
// metric metadata labels.
func (d *otlpMetricsDec) seriesLabels(name string, rs *resourceScope, pointAttrs []*otlpcommon.KeyValue,
	extra [][2]string, metricType string, metric *otlpmetrics.Metric) [][]string {
	merged := make(map[string]string, len(rs.scopeLabels)+len(pointAttrs)+len(extra)+8)
	for _, p := range rs.scopeLabels {
		merged[p[0]] = p[1]
	}
	mergeSanitizedAttrs(merged, "", pointAttrs)
	merged["__name__"] = name
	if rs.job != "" {
		merged["job"] = rs.job
	}
	if rs.instance != "" {
		merged["instance"] = rs.instance
	}
	for _, p := range extra {
		merged[p[0]] = p[1]
	}
	merged["__metric_type__"] = metricType
	if metric.Description != "" {
		merged["__metric_help__"] = metric.Description
	}
	if u, _ := unitWords(metric.Unit); u != "" {
		merged["__metric_unit__"] = u
	}

	lbls := make([][]string, 0, len(merged))
	for k, v := range merged {
		lbls = append(lbls, []string{k, v})
	}
	sort.Slice(lbls, func(i, j int) bool { return lbls[i][0] < lbls[j][0] })
	return lbls
}

// acceptTimestamp validates a data point timestamp and tracks the resource's
// latest accepted timestamp for target_info emission. Zero timestamps are
// rejected: TimeUnixNano is the canonical sample time and a zero would store
// a bogus epoch row. The name is the raw OTLP metric name — rejection reasons
// are user-facing and consistently name the metric as the exporter sent it.
func (d *otlpMetricsDec) acceptTimestamp(name string, tsNano uint64, rs *resourceScope) (int64, bool) {
	if tsNano == 0 {
		d.stats.reject(1, fmt.Sprintf("metric %q: data point with zero time_unix_nano", name))
		return 0, false
	}
	ts := int64(tsNano)
	if ts > rs.lastTs {
		rs.lastTs = ts
	}
	return ts, true
}

func noRecordedValue(flags uint32) bool {
	return flags&noRecordedValueMask != 0
}

// firstExemplarTraceID returns the hex-encoded trace_id of the first exemplar
// carrying a non-zero 16-byte trace_id, or "" if none exist. The ID is stored
// in the sample's string column for metric-to-trace correlation.
func firstExemplarTraceID(exemplars []*otlpmetrics.Exemplar) string {
	for _, ex := range exemplars {
		if len(ex.TraceId) == 16 && !isZeroBytes(ex.TraceId) {
			return hex.EncodeToString(ex.TraceId)
		}
	}
	return ""
}

func (d *otlpMetricsDec) emit(lbls [][]string, ts int64, str string, val float64) error {
	return d.onEntries(lbls, []int64{ts}, []string{str}, []float64{val},
		[]uint8{model.SAMPLE_TYPE_METRIC})
}

func (d *otlpMetricsDec) decodeNumberPoints(points []*otlpmetrics.NumberDataPoint, name, metricType string,
	metric *otlpmetrics.Metric, rs *resourceScope) error {
	for _, pt := range points {
		if noRecordedValue(pt.Flags) {
			continue
		}
		ts, ok := d.acceptTimestamp(metric.Name, pt.TimeUnixNano, rs)
		if !ok {
			continue
		}
		var val float64
		switch v := pt.Value.(type) {
		case *otlpmetrics.NumberDataPoint_AsDouble:
			val = v.AsDouble
		case *otlpmetrics.NumberDataPoint_AsInt:
			val = float64(v.AsInt)
		default:
			d.stats.reject(1, fmt.Sprintf("metric %q: number data point with no value", metric.Name))
			continue
		}
		lbls := d.seriesLabels(name, rs, pt.Attributes, nil, metricType, metric)
		if err := d.emit(lbls, ts, firstExemplarTraceID(pt.Exemplars), val); err != nil {
			return err
		}
	}
	return nil
}

func (d *otlpMetricsDec) decodeHistogram(points []*otlpmetrics.HistogramDataPoint, name string,
	metric *otlpmetrics.Metric, rs *resourceScope) error {
	for _, dp := range points {
		if noRecordedValue(dp.Flags) {
			continue
		}
		if len(dp.BucketCounts) > 0 && len(dp.BucketCounts) != len(dp.ExplicitBounds)+1 {
			d.stats.reject(1, fmt.Sprintf("metric %q: histogram bucket_counts length %d does not match explicit_bounds length %d + 1",
				metric.Name, len(dp.BucketCounts), len(dp.ExplicitBounds)))
			continue
		}
		ts, ok := d.acceptTimestamp(metric.Name, dp.TimeUnixNano, rs)
		if !ok {
			continue
		}
		traceID := firstExemplarTraceID(dp.Exemplars)

		cumulative := uint64(0)
		infEmitted := false
		for i, count := range dp.BucketCounts {
			cumulative += count
			le := "+Inf"
			if i < len(dp.ExplicitBounds) {
				le = strconv.FormatFloat(dp.ExplicitBounds[i], 'f', -1, 64)
			} else {
				// The +Inf bucket carries the running sum rather than dp.Count.
				// The two disagree whenever a producer is lossy, and taking
				// dp.Count here can place +Inf BELOW the last finite bucket.
				// histogram_quantile does not error on a non-monotonic bucket
				// series, it silently returns a wrong number, so monotonicity
				// has to hold by construction. The producer's own total is
				// still reported by _count, leaving any mismatch visible as
				// _bucket{le="+Inf"} != _count.
				infEmitted = true
			}
			lbls := d.seriesLabels(name+"_bucket", rs, dp.Attributes, [][2]string{{"le", le}}, "histogram", metric)
			if err := d.emit(lbls, ts, traceID, float64(cumulative)); err != nil {
				return err
			}
		}
		if !infEmitted {
			lbls := d.seriesLabels(name+"_bucket", rs, dp.Attributes, [][2]string{{"le", "+Inf"}}, "histogram", metric)
			if err := d.emit(lbls, ts, traceID, float64(dp.Count)); err != nil {
				return err
			}
		}

		if dp.Sum != nil {
			lbls := d.seriesLabels(name+"_sum", rs, dp.Attributes, nil, "histogram", metric)
			if err := d.emit(lbls, ts, "", *dp.Sum); err != nil {
				return err
			}
		}
		lbls := d.seriesLabels(name+"_count", rs, dp.Attributes, nil, "histogram", metric)
		if err := d.emit(lbls, ts, "", float64(dp.Count)); err != nil {
			return err
		}
	}
	return nil
}

func (d *otlpMetricsDec) decodeExponentialHistogram(points []*otlpmetrics.ExponentialHistogramDataPoint, name string,
	metric *otlpmetrics.Metric, rs *resourceScope) error {
	for _, dp := range points {
		if noRecordedValue(dp.Flags) {
			continue
		}
		if dp.Negative != nil && len(dp.Negative.BucketCounts) > 0 {
			d.stats.reject(1, fmt.Sprintf("metric %q: exponential histogram with negative buckets is not supported", metric.Name))
			continue
		}
		ts, ok := d.acceptTimestamp(metric.Name, dp.TimeUnixNano, rs)
		if !ok {
			continue
		}
		traceID := firstExemplarTraceID(dp.Exemplars)

		cumulative := dp.ZeroCount
		if dp.Positive != nil {
			offset := dp.Positive.Offset
			for i, count := range dp.Positive.BucketCounts {
				cumulative += count
				// Bucket index k has upper bound 2^(k / 2^scale). Computing it
				// as Exp2 of one product keeps the bound exact at every power
				// of two, where the equivalent Pow(base, k) with a pre-computed
				// inexact base accumulates floating-point error that would leak
				// into the `le` label and break cross-source aggregation.
				upperBound := math.Exp2(float64(int32(i)+offset+1) * math.Exp2(-float64(dp.Scale)))
				le := strconv.FormatFloat(upperBound, 'f', -1, 64)
				lbls := d.seriesLabels(name+"_bucket", rs, dp.Attributes, [][2]string{{"le", le}}, "histogram", metric)
				if err := d.emit(lbls, ts, traceID, float64(cumulative)); err != nil {
					return err
				}
			}
		}

		// +Inf carries the running sum so the bucket series stays monotonic even
		// when a producer's count disagrees with its bucket counts; _count
		// reports the producer's total. With no positive buckets, dp.Count is
		// the total.
		infValue := cumulative
		if dp.Positive == nil || len(dp.Positive.BucketCounts) == 0 {
			infValue = dp.Count
		}
		lblsInf := d.seriesLabels(name+"_bucket", rs, dp.Attributes, [][2]string{{"le", "+Inf"}}, "histogram", metric)
		if err := d.emit(lblsInf, ts, traceID, float64(infValue)); err != nil {
			return err
		}
		if dp.Sum != nil {
			lbls := d.seriesLabels(name+"_sum", rs, dp.Attributes, nil, "histogram", metric)
			if err := d.emit(lbls, ts, "", *dp.Sum); err != nil {
				return err
			}
		}
		lbls := d.seriesLabels(name+"_count", rs, dp.Attributes, nil, "histogram", metric)
		if err := d.emit(lbls, ts, "", float64(dp.Count)); err != nil {
			return err
		}
	}
	return nil
}

func (d *otlpMetricsDec) decodeSummary(points []*otlpmetrics.SummaryDataPoint, name string,
	metric *otlpmetrics.Metric, rs *resourceScope) error {
	for _, dp := range points {
		if noRecordedValue(dp.Flags) {
			continue
		}
		ts, ok := d.acceptTimestamp(metric.Name, dp.TimeUnixNano, rs)
		if !ok {
			continue
		}
		for _, q := range dp.QuantileValues {
			quantile := strconv.FormatFloat(q.Quantile, 'f', -1, 64)
			lbls := d.seriesLabels(name, rs, dp.Attributes, [][2]string{{"quantile", quantile}}, "summary", metric)
			if err := d.emit(lbls, ts, "", q.Value); err != nil {
				return err
			}
		}
		lbls := d.seriesLabels(name+"_sum", rs, dp.Attributes, nil, "summary", metric)
		if err := d.emit(lbls, ts, "", dp.Sum); err != nil {
			return err
		}
		lbls = d.seriesLabels(name+"_count", rs, dp.Attributes, nil, "summary", metric)
		if err := d.emit(lbls, ts, "", float64(dp.Count)); err != nil {
			return err
		}
	}
	return nil
}

// emitTargetInfo writes the target_info gauge for one resource: a single
// sample of value 1 at the latest accepted data-point timestamp, labeled with
// job/instance identity plus the non-identifying resource attributes — one
// sample per target per export, matching Prometheus's one-per-scrape shape.
// Nothing is emitted when the resource carries no attributes beyond its
// identity or contributed no accepted data points.
func (d *otlpMetricsDec) emitTargetInfo(rs *resourceScope) error {
	if len(rs.targetAttrs) == 0 || rs.lastTs == 0 {
		return nil
	}
	merged := make(map[string]string, len(rs.targetAttrs)+3)
	for k, v := range rs.targetAttrs {
		merged[k] = v
	}
	merged["__name__"] = "target_info"
	if rs.job != "" {
		merged["job"] = rs.job
	}
	if rs.instance != "" {
		merged["instance"] = rs.instance
	}
	merged["__metric_type__"] = "gauge"
	lbls := make([][]string, 0, len(merged))
	for k, v := range merged {
		lbls = append(lbls, []string{k, v})
	}
	sort.Slice(lbls, func(i, j int) bool { return lbls[i][0] < lbls[j][0] })

	return d.emit(lbls, rs.lastTs, "", 1)
}

// OTLPMetricsFromData builds a parser over an already-decoded MetricsData.
// Both transports use it: gRPC passes the framework-decoded request, HTTP
// passes the body it decoded from protobuf or JSON. Rejection counts for the
// partial_success response accumulate into stats, which is safe to read once
// the parser response channel closes.
func OTLPMetricsFromData(md *otlpmetrics.MetricsData, stats *OTLPMetricsStats) ParsingFunction {
	return Build(
		withPreParsedBody(md),
		withLogsParser(func(ctx *ParserCtx) iLogsParser {
			return &otlpMetricsDec{ctx: ctx, stats: stats}
		}),
	)
}
