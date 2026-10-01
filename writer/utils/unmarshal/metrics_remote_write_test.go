package unmarshal

import (
	"bytes"
	"context"
	"math"
	"testing"

	clconfig "github.com/metrico/cloki-config"
	clokiconfig "github.com/metrico/cloki-config/config"
	clcwriter "github.com/metrico/cloki-config/config/writer"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/metric"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/metriccache"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"
)

type stagingRow struct {
	fp        uint64
	tsMs      int64
	value     float64
	prevTsMs  int64
	prevValue float64
	aggregate uint8
}

type seriesRow struct {
	name        string
	fp          uint64
	labels      map[string]string
	firstSeenMs int64
	lastSeenMs  int64
}

type metadataRow struct {
	name, typ, help, unit string
}

type exemplarRow struct {
	fp      uint64
	tsMs    int64
	value   float64
	traceID string
	labels  string
}

type metricRows struct {
	staging   []stagingRow
	series    []seriesRow
	metadata  []metadataRow
	exemplars []exemplarRow
}

func withCityHashFingerprints(t *testing.T) {
	t.Helper()
	old := config.Cloki
	config.Cloki = &clconfig.ClokiConfig{Setting: &clokiconfig.ClokiBaseSettingServer{
		FingerPrintType: clcwriter.FINGERPRINT_CityHash,
	}}
	t.Cleanup(func() { config.Cloki = old })
}

// pushRemoteWrite runs one remote-write request through the decoder against
// the node's metric caches and collects the rows it hands the insert services.
func pushRemoteWrite(t *testing.T, node *metriccache.Node, req *prompb.WriteRequest) metricRows {
	t.Helper()
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	ctx := metriccache.NewContext(context.Background(), node)
	var rows metricRows
	for resp := range UnmarshallMetricsWriteProtoV2(ctx, bytes.NewReader(body), nil) {
		if resp.Error != nil {
			t.Fatalf("parser error: %v", resp.Error)
		}
		if resp.TimeSeriesRequest != nil || resp.SamplesRequest != nil {
			t.Fatal("remote write must not produce time_series or samples_v3 rows")
		}
		if d, ok := resp.MetricSamplesRequest.(*model.MetricSamplesData); ok {
			for i := range d.MFingerprint {
				rows.staging = append(rows.staging, stagingRow{d.MFingerprint[i], d.MTimestampMs[i], d.MValue[i],
					d.MPrevTimestampMs[i], d.MPrevValue[i], d.MAggregate[i]})
			}
		}
		if d, ok := resp.MetricSeriesRequest.(*model.MetricSeriesData); ok {
			for i := range d.MFingerprint {
				rows.series = append(rows.series, seriesRow{d.MName[i], d.MFingerprint[i], d.MLabels[i],
					d.MFirstSeenMs[i], d.MLastSeenMs[i]})
			}
		}
		if d, ok := resp.MetricMetadataRequest.(*model.MetricMetadataData); ok {
			for i := range d.MName {
				rows.metadata = append(rows.metadata, metadataRow{d.MName[i], d.MType[i], d.MHelp[i], d.MUnit[i]})
			}
		}
		if d, ok := resp.MetricExemplarsRequest.(*model.MetricExemplarsData); ok {
			for i := range d.MFingerprint {
				rows.exemplars = append(rows.exemplars, exemplarRow{d.MFingerprint[i], d.MTimestampMs[i], d.MValue[i],
					d.MTraceID[i], d.MLabels[i]})
			}
		}
	}
	return rows
}

func newNode(t *testing.T) *metriccache.Node {
	c := metriccache.New()
	t.Cleanup(c.Stop)
	return c.Node("test")
}

func lbls(kv ...string) []*prompb.Label {
	res := make([]*prompb.Label, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		res = append(res, &prompb.Label{Name: kv[i], Value: kv[i+1]})
	}
	return res
}

func samples(tv ...float64) []*prompb.Sample {
	res := make([]*prompb.Sample, 0, len(tv)/2)
	for i := 0; i < len(tv); i += 2 {
		res = append(res, &prompb.Sample{Timestamp: int64(tv[i]), Value: tv[i+1]})
	}
	return res
}

// Fingerprints computed with master's fingerprintLabels (CityHash) over the
// label set with service_name added.
const (
	fpHTTPRequestsAPI uint64 = 10423250724175581884
	fpUpUnknown       uint64 = 6552610143372409600
)

func TestRemoteWriteSeriesLabelsAndFingerprint(t *testing.T) {
	withCityHashFingerprints(t)
	rows := pushRemoteWrite(t, newNode(t), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{
			Labels: lbls("__name__", "http_requests_total", "job", "api", "code", "200",
				"__ttl_days__", "7", "__metric_type__", "counter", "__metric_help__", "Requests.", "__metric_unit__", "requests"),
			Samples: samples(1000, 1, 2000, 2),
		},
		{Labels: lbls("__name__", "up"), Samples: samples(1000, 1)},
	}})

	want := []seriesRow{
		{"http_requests_total", fpHTTPRequestsAPI, map[string]string{
			"__name__": "http_requests_total", "job": "api", "code": "200", "service_name": "api"}, 1000, 2000},
		{"up", fpUpUnknown, map[string]string{"__name__": "up", "service_name": "unknown"}, 1000, 1000},
	}
	if len(rows.series) != len(want) {
		t.Fatalf("series rows: got %+v, want %+v", rows.series, want)
	}
	for i := range want {
		g, w := rows.series[i], want[i]
		if g.name != w.name || g.fp != w.fp || g.firstSeenMs != w.firstSeenMs || g.lastSeenMs != w.lastSeenMs ||
			len(g.labels) != len(w.labels) {
			t.Fatalf("series row %d: got %+v, want %+v", i, g, w)
		}
		for k, v := range w.labels {
			if g.labels[k] != v {
				t.Fatalf("series row %d label %s: got %q, want %q", i, k, g.labels[k], v)
			}
		}
	}
	if got := rows.staging[0].fp; got != fpHTTPRequestsAPI {
		t.Fatalf("staging fingerprint: got %d, want %d", got, fpHTTPRequestsAPI)
	}
	wantMeta := []metadataRow{{"http_requests_total", "counter", "Requests.", "requests"}}
	if len(rows.metadata) != 1 || rows.metadata[0] != wantMeta[0] {
		t.Fatalf("metadata rows: got %+v, want %+v", rows.metadata, wantMeta)
	}
}

func TestRemoteWriteRequestMetadata(t *testing.T) {
	withCityHashFingerprints(t)
	node := newNode(t)
	req := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{{Labels: lbls("__name__", "up"), Samples: samples(1000, 1)}},
		Metadata: []*prompb.MetricMetadata{
			{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: "up", Help: "Target is up.", Unit: ""},
			{Type: prompb.MetricMetadata_HISTOGRAM, MetricFamilyName: "rpc_seconds", Help: "RPC latency.", Unit: "seconds"},
		},
	}
	rows := pushRemoteWrite(t, node, req)
	want := []metadataRow{
		{"up", "gauge", "Target is up.", ""},
		{"rpc_seconds", "histogram", "RPC latency.", "seconds"},
	}
	if len(rows.metadata) != len(want) || rows.metadata[0] != want[0] || rows.metadata[1] != want[1] {
		t.Fatalf("metadata rows: got %+v, want %+v", rows.metadata, want)
	}
	if again := pushRemoteWrite(t, node, req); len(again.metadata) != 0 || len(again.series) != 0 {
		t.Fatalf("unchanged metadata and known series must emit no rows, got %+v %+v", again.metadata, again.series)
	}
	req.Metadata[0].Help = "Scrape target is up."
	if changed := pushRemoteWrite(t, node, req); len(changed.metadata) != 1 ||
		changed.metadata[0] != (metadataRow{"up", "gauge", "Scrape target is up.", ""}) {
		t.Fatalf("changed metadata: got %+v", changed.metadata)
	}
	node.Series.Reset()
	if reset := pushRemoteWrite(t, node, req); len(reset.metadata) != 2 || len(reset.series) != 1 {
		t.Fatalf("after the reset every family and series must be emitted again, got %+v %+v", reset.metadata, reset.series)
	}
}

func TestRemoteWriteInBatchDuplicatesLastWins(t *testing.T) {
	withCityHashFingerprints(t)
	rows := pushRemoteWrite(t, newNode(t), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{Labels: lbls("__name__", "up"), Samples: samples(1000, 1, 2000, 2, 1000, 3)},
		{Labels: lbls("__name__", "up"), Samples: samples(2000, 4)},
	}})
	want := []stagingRow{
		{fpUpUnknown, 1000, 3, 0, 0, 1},
		{fpUpUnknown, 2000, 4, 1000, 3, 1},
	}
	if len(rows.staging) != len(want) || rows.staging[0] != want[0] || rows.staging[1] != want[1] {
		t.Fatalf("staging rows: got %+v, want %+v", rows.staging, want)
	}
}

func TestRemoteWritePredecessorsAcrossRequests(t *testing.T) {
	withCityHashFingerprints(t)
	node := newNode(t)
	staleNaN := math.Float64frombits(metriccache.StaleMarkerBits)
	pushRemoteWrite(t, node, &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{Labels: lbls("__name__", "up"), Samples: samples(1000, 1, 2000, 2)},
	}})
	rows := pushRemoteWrite(t, node, &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{Labels: lbls("__name__", "up"), Samples: []*prompb.Sample{
			{Timestamp: 1500, Value: 9},
			{Timestamp: 2000, Value: 5},
			{Timestamp: 3000, Value: staleNaN},
			{Timestamp: 4000, Value: 4},
		}},
	}})
	if len(rows.staging) != 4 {
		t.Fatalf("staging rows: got %+v", rows.staging)
	}
	want := []stagingRow{
		{fpUpUnknown, 1500, 9, 2000, 2, 0},
		{fpUpUnknown, 2000, 5, 2000, 2, 0},
		{},
		{fpUpUnknown, 4000, 4, 2000, 2, 1},
	}
	for _, i := range []int{0, 1, 3} {
		if rows.staging[i] != want[i] {
			t.Fatalf("staging row %d: got %+v, want %+v", i, rows.staging[i], want[i])
		}
	}
	marker := rows.staging[2]
	if math.Float64bits(marker.value) != metriccache.StaleMarkerBits {
		t.Fatalf("stale marker must reach the staging row bit-exact, got %#x", math.Float64bits(marker.value))
	}
	if marker.tsMs != 3000 || marker.prevTsMs != 2000 || marker.prevValue != 2 || marker.aggregate != 1 {
		t.Fatalf("stale marker row: got %+v", marker)
	}
	if len(rows.series) != 0 {
		t.Fatalf("a known series must not emit its row again, got %+v", rows.series)
	}
}

func TestRemoteWriteNativeHistogramsRejectedFloatsKept(t *testing.T) {
	withCityHashFingerprints(t)
	counter := metric.IngestRejected.WithLabelValues("remote_write_native_histogram")
	before := testutil.ToFloat64(counter)
	rows := pushRemoteWrite(t, newNode(t), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{
			Labels: lbls("__name__", "rpc_seconds"),
			Histograms: []*prompb.Histogram{
				{Count: &prompb.Histogram_CountInt{CountInt: 3}, Sum: 1.5, Timestamp: 1000},
				{Count: &prompb.Histogram_CountInt{CountInt: 4}, Sum: 2, Timestamp: 2000},
			},
		},
		{Labels: lbls("__name__", "up"), Samples: samples(1000, 1)},
	}})
	if got := testutil.ToFloat64(counter) - before; got != 2 {
		t.Fatalf("rejected native histograms: got %v, want 2", got)
	}
	if len(rows.staging) != 1 || rows.staging[0].fp != fpUpUnknown || rows.staging[0].value != 1 {
		t.Fatalf("the request's floats must land, got %+v", rows.staging)
	}
	if len(rows.series) != 1 || rows.series[0].name != "up" {
		t.Fatalf("a histogram-only series must emit no series row, got %+v", rows.series)
	}
}

func TestRemoteWriteExemplars(t *testing.T) {
	withCityHashFingerprints(t)
	rows := pushRemoteWrite(t, newNode(t), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{
			Labels:  lbls("__name__", "up"),
			Samples: samples(1000, 1),
			Exemplars: []*prompb.Exemplar{
				{Labels: lbls("trace_id", "abc123", "span_id", "def"), Value: 0.5, Timestamp: 990},
			},
		},
	}})
	want := exemplarRow{fpUpUnknown, 990, 0.5, "abc123", `{"trace_id":"abc123","span_id":"def"}`}
	if len(rows.exemplars) != 1 || rows.exemplars[0] != want {
		t.Fatalf("exemplar rows: got %+v, want %+v", rows.exemplars, want)
	}
}

// Prometheus sends each exemplar in a TimeSeries of its own, with no samples.
func TestRemoteWriteExemplarInItsOwnEntry(t *testing.T) {
	withCityHashFingerprints(t)
	rows := pushRemoteWrite(t, newNode(t), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{Labels: lbls("__name__", "up"), Samples: samples(1000, 1)},
		{
			Labels:    lbls("__name__", "up"),
			Exemplars: []*prompb.Exemplar{{Labels: lbls("trace_id", "abc"), Value: 0.5, Timestamp: 990}},
		},
	}})
	want := exemplarRow{fpUpUnknown, 990, 0.5, "abc", `{"trace_id":"abc"}`}
	if len(rows.exemplars) != 1 || rows.exemplars[0] != want {
		t.Fatalf("exemplar rows: got %+v, want %+v", rows.exemplars, want)
	}
	if len(rows.staging) != 1 || len(rows.series) != 1 {
		t.Fatalf("the exemplar entry must add no sample or series row, got %+v %+v", rows.staging, rows.series)
	}
}

func TestRemoteWriteExemplarOnHistogramOnlyEntry(t *testing.T) {
	withCityHashFingerprints(t)
	rows := pushRemoteWrite(t, newNode(t), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{
			Labels:     lbls("__name__", "up"),
			Histograms: []*prompb.Histogram{{Count: &prompb.Histogram_CountInt{CountInt: 1}, Timestamp: 1000}},
			Exemplars:  []*prompb.Exemplar{{Labels: lbls("trace_id", "abc"), Value: 0.5, Timestamp: 990}},
		},
	}})
	want := exemplarRow{fpUpUnknown, 990, 0.5, "abc", `{"trace_id":"abc"}`}
	if len(rows.exemplars) != 1 || rows.exemplars[0] != want {
		t.Fatalf("exemplar rows: got %+v, want %+v", rows.exemplars, want)
	}
	if len(rows.staging) != 0 || len(rows.series) != 0 {
		t.Fatalf("a histogram-only entry must add no sample or series row, got %+v %+v", rows.staging, rows.series)
	}
}

func TestRemoteWriteDuplicatesCollapseAcrossALargeRequest(t *testing.T) {
	withCityHashFingerprints(t)
	const n = 40000
	big := make([]*prompb.Sample, n)
	for i := range big {
		big[i] = &prompb.Sample{Timestamp: int64(i+1) * 1000, Value: 1}
	}
	rows := pushRemoteWrite(t, newNode(t), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{
		{Labels: lbls("__name__", "up"), Samples: big},
		{Labels: lbls("__name__", "up"), Samples: samples(1000, 7)},
	}})
	if len(rows.staging) != n {
		t.Fatalf("staging rows: got %d, want %d", len(rows.staging), n)
	}
	if got := rows.staging[0]; got.tsMs != 1000 || got.value != 7 {
		t.Fatalf("the later duplicate must replace the earlier one, got %+v", got)
	}
}
