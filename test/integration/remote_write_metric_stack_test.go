//go:build integration

// A Prometheus remote-write request lands in the metric stack and nowhere else:
// raw samples, both aggregate tiers, the series index, metadata and exemplars,
// with nothing in time_series or samples_v3. The request is shaped as
// Prometheus sends it, one sample, exemplar or histogram per TimeSeries.

package integration

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"google.golang.org/protobuf/proto"
)

// eventually polls sql until it returns want or the insert services' flush
// interval has long passed.
func eventually(t *testing.T, sql, want string) {
	t.Helper()
	var got string
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		if got = clickhouseQuery(t, sql); got == want {
			return
		}
	}
	t.Fatalf("%s\ngot  %q\nwant %q", sql, got, want)
}

func TestRemoteWriteLandsInTheMetricStack(t *testing.T) {
	waitReady(t)
	name := fmt.Sprintf("it_remote_write_%d_total", time.Now().UnixNano())
	// Five samples in one 5m bucket, ending with a stale marker.
	base := (time.Now().Add(-time.Hour).UnixMilli()/300000)*300000 + 1000
	stale := math.Float64frombits(0x7ff0000000000002)
	labels := []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "it"}}
	req := &prompb.WriteRequest{Metadata: []*prompb.MetricMetadata{
		{Type: prompb.MetricMetadata_COUNTER, MetricFamilyName: name, Help: "Integration requests."},
	}}
	for i, v := range []float64{1, 2, 3, 1, stale} {
		req.Timeseries = append(req.Timeseries, &prompb.TimeSeries{
			Labels: labels, Samples: []*prompb.Sample{{Timestamp: base + int64(i)*15000, Value: v}},
		})
	}
	req.Timeseries = append(req.Timeseries,
		&prompb.TimeSeries{Labels: labels, Exemplars: []*prompb.Exemplar{{
			Labels: []*prompb.Label{{Name: "trace_id", Value: "abc123"}}, Value: 2, Timestamp: base + 15000,
		}}},
		&prompb.TimeSeries{Labels: labels, Histograms: []*prompb.Histogram{{
			Count: &prompb.Histogram_CountInt{CountInt: 1}, Timestamp: base,
		}}},
	)
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL()+"/api/v1/prom/remote/write", "application/x-protobuf",
		bytes.NewReader(snappy.Encode(nil, body)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("remote write status = %d", resp.StatusCode)
	}

	fp := fmt.Sprintf("(SELECT fingerprint FROM metric_series WHERE name = '%s')", name)
	eventually(t, fmt.Sprintf("SELECT labels['job'], labels['service_name'] FROM metric_series WHERE name = '%s'", name),
		"it\tit")
	eventually(t, fmt.Sprintf("SELECT count(), countIf(reinterpretAsUInt64(value) = 0x7ff0000000000002) "+
		"FROM metric_samples FINAL WHERE fingerprint IN %s", fp), "5\t1")
	eventually(t, fmt.Sprintf("SELECT sum(count), sum(resets), sum(reset_drop), sum(changes) "+
		"FROM metrics_5m WHERE fingerprint IN %s", fp), "4\t1\t3\t3")
	eventually(t, fmt.Sprintf("SELECT sum(count), sum(resets) FROM metrics_1h WHERE fingerprint IN %s", fp), "4\t1")
	eventually(t, fmt.Sprintf("SELECT type, help FROM metric_metadata FINAL WHERE name = '%s'", name),
		"counter\tIntegration requests.")
	eventually(t, fmt.Sprintf("SELECT trace_id, value FROM metric_exemplars FINAL WHERE fingerprint IN %s", fp),
		"abc123\t2")
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM time_series WHERE labels LIKE '%%%s%%'", name)); got != "0" {
		t.Fatalf("time_series rows for the remote-written series: %s", got)
	}
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM samples_v3 WHERE fingerprint IN %s", fp)); got != "0" {
		t.Fatalf("samples_v3 rows for the remote-written series: %s", got)
	}
}
