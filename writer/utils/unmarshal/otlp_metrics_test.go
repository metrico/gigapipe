package unmarshal

import (
	"context"
	"strings"
	"testing"
	"time"
	"unsafe"

	clconfig "github.com/metrico/cloki-config"
	clokiconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestBuildMetricName(t *testing.T) {
	cases := []struct {
		name, unit string
		counter    bool
		want       string
	}{
		{"http.server.duration", "s", false, "http_server_duration_seconds"},
		{"http.server.requests", "{requests}", true, "http_server_requests_total"},
		{"network.io", "By/s", false, "network_io_bytes_per_second"},
		{"process.cpu.time", "s", true, "process_cpu_time_seconds_total"},
		{"request.duration.seconds", "s", false, "request_duration_seconds"},
		{"my.counter.total", "", true, "my_counter_total"},
		{"2xx.count", "", false, "_2xx_count"},
		{"weird..name", "", false, "weird_name"},
		{"queue.size", "1", false, "queue_size"},
		{"temp", "Cel", false, "temp_celsius"},
		{"disk.usage", "MiBy", false, "disk_usage_mebibytes"},
	}
	for _, c := range cases {
		if got := buildMetricName(c.name, c.unit, c.counter); got != c.want {
			t.Errorf("buildMetricName(%q, %q, %v) = %q, want %q", c.name, c.unit, c.counter, got, c.want)
		}
	}
}

type collectedSample struct {
	labels string
	ts     int64
	value  float64
	str    string
}

type metricsCollector struct {
	samples  []collectedSample
	metadata map[string]string
}

// collectMetrics runs the OTLP metrics parser over md and joins emitted
// samples with their series labels via fingerprint.
func collectMetrics(t *testing.T, md *metricsv1.MetricsData, stats *OTLPMetricsStats) *metricsCollector {
	t.Helper()
	// onEntries -> fingerprintLabels reads config.Cloki.Setting.FingerPrintType;
	// install a minimal global config so that read does not nil-pointer panic.
	old := config.Cloki
	config.Cloki = &clconfig.ClokiConfig{Setting: &clokiconfig.ClokiBaseSettingServer{}}
	t.Cleanup(func() { config.Cloki = old })
	cache := numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{})
	defer cache.Stop()

	ch := OTLPMetricsFromData(md, stats)(context.Background(), nil, cache)
	fpLabels := map[uint64]string{}
	fpMetadata := map[uint64]string{}
	type rawSample struct {
		fp    uint64
		ts    int64
		value float64
		str   string
	}
	var raw []rawSample
	for resp := range ch {
		if resp.Error != nil {
			t.Fatalf("unexpected parser error: %v", resp.Error)
		}
		if tsReq, ok := resp.TimeSeriesRequest.(*model.TimeSeriesData); ok && tsReq != nil {
			for i, fp := range tsReq.MFingerprint {
				fpLabels[fp] = tsReq.MLabels[i]
				fpMetadata[fp] = tsReq.MMetadata[i]
			}
		}
		if splReq, ok := resp.SamplesRequest.(*model.TimeSamplesData); ok && splReq != nil {
			for i, fp := range splReq.MFingerprint {
				raw = append(raw, rawSample{fp, splReq.MTimestampNS[i], splReq.MValue[i], splReq.MMessage[i]})
			}
		}
	}
	col := &metricsCollector{metadata: map[string]string{}}
	for _, r := range raw {
		lbls := fpLabels[r.fp]
		col.samples = append(col.samples, collectedSample{lbls, r.ts, r.value, r.str})
		col.metadata[lbls] = fpMetadata[r.fp]
	}
	return col
}

func (c *metricsCollector) find(substrs ...string) []collectedSample {
	var out []collectedSample
	for _, s := range c.samples {
		match := true
		for _, sub := range substrs {
			if !strings.Contains(s.labels, sub) {
				match = false
				break
			}
		}
		if match {
			out = append(out, s)
		}
	}
	return out
}

func kv(key, val string) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: key, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}}}
}

func testResource() *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
		kv("service.name", "checkout"),
		kv("service.namespace", "shop"),
		kv("service.instance.id", "pod-1"),
		kv("host.name", "node-a"),
	}}
}

func wrapMetrics(res *resourcev1.Resource, metrics ...*metricsv1.Metric) *metricsv1.MetricsData {
	return &metricsv1.MetricsData{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource: res,
		ScopeMetrics: []*metricsv1.ScopeMetrics{{
			Scope:   &commonv1.InstrumentationScope{Name: "test-lib", Version: "1.2.3"},
			Metrics: metrics,
		}},
	}}}
}

const testTS = uint64(1700000000_000000000)

func TestOTLPMetrics_GaugeWithResourceAndScope(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "queue.depth", Unit: "1", Description: "queue depth",
		Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
			TimeUnixNano: testTS,
			Attributes:   []*commonv1.KeyValue{kv("queue", "q1")},
			Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: 42.5},
		}}}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)

	got := col.find(`"__name__":"queue_depth"`)
	if len(got) != 1 {
		t.Fatalf("expected 1 queue_depth sample, got %d; all: %+v", len(got), col.samples)
	}
	s := got[0]
	if s.value != 42.5 || s.ts != int64(testTS) {
		t.Errorf("bad sample: %+v", s)
	}
	for _, want := range []string{`"job":"shop/checkout"`, `"instance":"pod-1"`, `"queue":"q1"`,
		`"otel_scope_name":"test-lib"`, `"otel_scope_version":"1.2.3"`} {
		if !strings.Contains(s.labels, want) {
			t.Errorf("labels missing %s: %s", want, s.labels)
		}
	}
	if strings.Contains(s.labels, "__metric_type__") {
		t.Errorf("metadata label leaked into stored labels: %s", s.labels)
	}
	if !strings.Contains(col.metadata[s.labels], `"type":"gauge"`) {
		t.Errorf("metadata missing gauge type: %s", col.metadata[s.labels])
	}
	if stats.RejectedDataPoints() != 0 {
		t.Errorf("expected 0 rejected, got %d", stats.RejectedDataPoints())
	}
}

func TestOTLPMetrics_MonotonicSumBecomesCounter(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "http.requests", Unit: "{requests}",
		Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
			DataPoints: []*metricsv1.NumberDataPoint{{
				TimeUnixNano: testTS,
				Value:        &metricsv1.NumberDataPoint_AsInt{AsInt: 7},
			}},
		}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)
	got := col.find(`"__name__":"http_requests_total"`)
	if len(got) != 1 || got[0].value != 7 {
		t.Fatalf("expected http_requests_total=7, got %+v", got)
	}
	if !strings.Contains(col.metadata[got[0].labels], `"type":"counter"`) {
		t.Errorf("metadata missing counter type: %s", col.metadata[got[0].labels])
	}
}

func TestOTLPMetrics_DeltaSumRejected(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "delta.counter",
		Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			IsMonotonic:            true,
			DataPoints: []*metricsv1.NumberDataPoint{
				{TimeUnixNano: testTS, Value: &metricsv1.NumberDataPoint_AsInt{AsInt: 1}},
				{TimeUnixNano: testTS + 1, Value: &metricsv1.NumberDataPoint_AsInt{AsInt: 2}},
			},
		}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)
	if len(col.samples) != 0 {
		t.Fatalf("expected no samples for delta sum, got %+v", col.samples)
	}
	if stats.RejectedDataPoints() != 2 {
		t.Errorf("expected 2 rejected, got %d", stats.RejectedDataPoints())
	}
	if !strings.Contains(stats.ErrorMessage(), "temporality") {
		t.Errorf("error message should mention temporality: %q", stats.ErrorMessage())
	}
}

func TestOTLPMetrics_NoRecordedValueDroppedSilently(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "sparse.gauge",
		Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
			TimeUnixNano: testTS,
			Flags:        noRecordedValueMask,
		}}}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)
	if len(col.samples) != 0 {
		t.Fatalf("expected no samples, got %+v", col.samples)
	}
	if stats.RejectedDataPoints() != 0 {
		t.Errorf("staleness markers must not count as rejected, got %d", stats.RejectedDataPoints())
	}
}

func TestOTLPMetrics_ZeroTimestampRejected(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "bad.ts",
		Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
			Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 1},
		}}}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)
	if len(col.samples) != 0 {
		t.Fatalf("expected no samples, got %+v", col.samples)
	}
	if stats.RejectedDataPoints() != 1 {
		t.Errorf("expected 1 rejected, got %d", stats.RejectedDataPoints())
	}
}

func float64p(f float64) *float64 { return &f }

func TestOTLPMetrics_HistogramFlattening(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "req.duration", Unit: "s",
		Data: &metricsv1.Metric_Histogram{Histogram: &metricsv1.Histogram{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricsv1.HistogramDataPoint{{
				TimeUnixNano:   testTS,
				Count:          10,
				Sum:            float64p(9.5),
				ExplicitBounds: []float64{0.1, 1},
				BucketCounts:   []uint64{4, 3, 3},
			}},
		}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)

	checks := []struct {
		sub  []string
		want float64
	}{
		{[]string{`"__name__":"req_duration_seconds_bucket"`, `"le":"0.1"`}, 4},
		{[]string{`"__name__":"req_duration_seconds_bucket"`, `"le":"1"`}, 7},
		{[]string{`"__name__":"req_duration_seconds_bucket"`, `"le":"+Inf"`}, 10},
		{[]string{`"__name__":"req_duration_seconds_sum"`}, 9.5},
		{[]string{`"__name__":"req_duration_seconds_count"`}, 10},
	}
	for _, c := range checks {
		got := col.find(c.sub...)
		if len(got) != 1 || got[0].value != c.want {
			t.Errorf("series %v: want value %v, got %+v", c.sub, c.want, got)
		}
	}
	if stats.RejectedDataPoints() != 0 {
		t.Errorf("expected 0 rejected, got %d", stats.RejectedDataPoints())
	}
}

func TestOTLPMetrics_HistogramEmptyBucketsStillEmitsInf(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "sparse.hist",
		Data: &metricsv1.Metric_Histogram{Histogram: &metricsv1.Histogram{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricsv1.HistogramDataPoint{{
				TimeUnixNano: testTS,
				Count:        5,
			}},
		}},
	})
	col := collectMetrics(t, md, &OTLPMetricsStats{})
	got := col.find(`"__name__":"sparse_hist_bucket"`, `"le":"+Inf"`)
	if len(got) != 1 || got[0].value != 5 {
		t.Fatalf("expected +Inf bucket with count 5, got %+v", got)
	}
}

func TestOTLPMetrics_HistogramBucketBoundsMismatchRejected(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "broken.hist",
		Data: &metricsv1.Metric_Histogram{Histogram: &metricsv1.Histogram{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricsv1.HistogramDataPoint{{
				TimeUnixNano:   testTS,
				Count:          3,
				ExplicitBounds: []float64{1, 2},
				BucketCounts:   []uint64{1, 2},
			}},
		}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)
	if len(col.samples) != 0 {
		t.Fatalf("expected no samples, got %+v", col.samples)
	}
	if stats.RejectedDataPoints() != 1 {
		t.Errorf("expected 1 rejected, got %d", stats.RejectedDataPoints())
	}
}

func TestOTLPMetrics_ExponentialHistogramConversion(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "exp.duration", Unit: "s",
		Data: &metricsv1.Metric_ExponentialHistogram{ExponentialHistogram: &metricsv1.ExponentialHistogram{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricsv1.ExponentialHistogramDataPoint{{
				TimeUnixNano: testTS,
				Count:        7,
				Sum:          float64p(20),
				Scale:        0,
				ZeroCount:    1,
				Positive: &metricsv1.ExponentialHistogramDataPoint_Buckets{
					Offset:       0,
					BucketCounts: []uint64{2, 4},
				},
			}},
		}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)

	// scale 0 -> base 2; bucket 0 upper bound 2^1=2, bucket 1 upper 2^2=4.
	checks := []struct {
		sub  []string
		want float64
	}{
		{[]string{`"__name__":"exp_duration_seconds_bucket"`, `"le":"2"`}, 3},
		{[]string{`"__name__":"exp_duration_seconds_bucket"`, `"le":"4"`}, 7},
		{[]string{`"__name__":"exp_duration_seconds_bucket"`, `"le":"+Inf"`}, 7},
		{[]string{`"__name__":"exp_duration_seconds_sum"`}, 20},
		{[]string{`"__name__":"exp_duration_seconds_count"`}, 7},
	}
	for _, c := range checks {
		got := col.find(c.sub...)
		if len(got) != 1 || got[0].value != c.want {
			t.Errorf("series %v: want value %v, got %+v", c.sub, c.want, got)
		}
	}
}

func TestOTLPMetrics_ExponentialHistogramNegativeBucketsRejected(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "neg.exp",
		Data: &metricsv1.Metric_ExponentialHistogram{ExponentialHistogram: &metricsv1.ExponentialHistogram{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricsv1.ExponentialHistogramDataPoint{{
				TimeUnixNano: testTS,
				Count:        3,
				Negative: &metricsv1.ExponentialHistogramDataPoint_Buckets{
					BucketCounts: []uint64{3},
				},
			}},
		}},
	})
	stats := &OTLPMetricsStats{}
	col := collectMetrics(t, md, stats)
	if len(col.samples) != 0 {
		t.Fatalf("expected no samples, got %+v", col.samples)
	}
	if stats.RejectedDataPoints() != 1 {
		t.Errorf("expected 1 rejected, got %d", stats.RejectedDataPoints())
	}
}

func TestOTLPMetrics_Summary(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "gc.pause",
		Data: &metricsv1.Metric_Summary{Summary: &metricsv1.Summary{
			DataPoints: []*metricsv1.SummaryDataPoint{{
				TimeUnixNano: testTS,
				Count:        100,
				Sum:          12.5,
				QuantileValues: []*metricsv1.SummaryDataPoint_ValueAtQuantile{
					{Quantile: 0.5, Value: 0.1},
					{Quantile: 0.99, Value: 0.9},
				},
			}},
		}},
	})
	col := collectMetrics(t, md, &OTLPMetricsStats{})
	checks := []struct {
		sub  []string
		want float64
	}{
		{[]string{`"__name__":"gc_pause"`, `"quantile":"0.5"`}, 0.1},
		{[]string{`"__name__":"gc_pause"`, `"quantile":"0.99"`}, 0.9},
		{[]string{`"__name__":"gc_pause_sum"`}, 12.5},
		{[]string{`"__name__":"gc_pause_count"`}, 100},
	}
	for _, c := range checks {
		got := col.find(c.sub...)
		if len(got) != 1 || got[0].value != c.want {
			t.Errorf("series %v: want value %v, got %+v", c.sub, c.want, got)
		}
	}
}

func TestOTLPMetrics_TargetInfo(t *testing.T) {
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "some.gauge",
		Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
			TimeUnixNano: testTS,
			Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: 1},
		}}}},
	})
	col := collectMetrics(t, md, &OTLPMetricsStats{})
	got := col.find(`"__name__":"target_info"`)
	if len(got) != 1 {
		t.Fatalf("expected 1 target_info sample, got %+v", got)
	}
	s := got[0]
	if s.value != 1 || s.ts != int64(testTS) {
		t.Errorf("bad target_info sample: %+v", s)
	}
	for _, want := range []string{`"host_name":"node-a"`, `"job":"shop/checkout"`, `"instance":"pod-1"`} {
		if !strings.Contains(s.labels, want) {
			t.Errorf("target_info missing %s: %s", want, s.labels)
		}
	}
	// The shared insert pipeline injects a service_name label on every stored
	// series (discoverServiceName), so it is expected here alongside job.
	if !strings.Contains(s.labels, `"service_name"`) {
		t.Errorf("expected pipeline-injected service_name label: %s", s.labels)
	}
	if strings.Contains(s.labels, "otel_scope") {
		t.Errorf("target_info must not carry scope labels: %s", s.labels)
	}
}

func TestOTLPMetrics_NoTargetInfoWithoutExtraAttrs(t *testing.T) {
	res := &resourcev1.Resource{Attributes: []*commonv1.KeyValue{kv("service.name", "svc")}}
	md := wrapMetrics(res, &metricsv1.Metric{
		Name: "g",
		Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
			TimeUnixNano: testTS,
			Value:        &metricsv1.NumberDataPoint_AsDouble{AsDouble: 1},
		}}}},
	})
	col := collectMetrics(t, md, &OTLPMetricsStats{})
	if got := col.find(`"__name__":"target_info"`); len(got) != 0 {
		t.Fatalf("expected no target_info, got %+v", got)
	}
}

func TestOTLPMetrics_ExemplarTraceIDStored(t *testing.T) {
	traceID := []byte("0123456789abcdef")
	md := wrapMetrics(testResource(), &metricsv1.Metric{
		Name: "with.exemplar",
		Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{
			AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
			DataPoints: []*metricsv1.NumberDataPoint{{
				TimeUnixNano: testTS,
				Value:        &metricsv1.NumberDataPoint_AsInt{AsInt: 1},
				Exemplars:    []*metricsv1.Exemplar{{TraceId: traceID}},
			}},
		}},
	})
	col := collectMetrics(t, md, &OTLPMetricsStats{})
	got := col.find(`"__name__":"with_exemplar_total"`)
	if len(got) != 1 {
		t.Fatalf("expected 1 sample, got %+v", got)
	}
	if got[0].str != "30313233343536373839616263646566" {
		t.Errorf("expected hex trace id in string column, got %q", got[0].str)
	}
}
