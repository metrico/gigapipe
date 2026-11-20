//go:build integration

// Ingestion conformance for Prometheus remote-write (M8, #924). What a
// Prometheus server sends is checked where gigapipe stores it, in ClickHouse:
// which tables get rows, and the fingerprint, labels, timestamps, values and
// sample type in them. The M2 metrics round trip checks the same data through
// the query API.
//
// The expected values are what gigapipe stores today, measured against a real
// stack. If a case fails after an intended change (for example the samples
// table split in #972), review the difference and edit the case by hand. The
// table names and ClickHouse helpers are in ingest_loki_push_test.go.

package integration

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
)

// metricsRollupTable is the 15-second rollup of metric samples, filled by a
// materialized view. Today it is the same table as logsRollupTable; the
// samples table split (#972) gives metrics their own.
const metricsRollupTable = "metrics_15s"

// sampleTypeMetric is the type column value of a metric sample (writer/model).
const sampleTypeMetric = 2

// pinnedMetricFingerprint is the fingerprint gigapipe gives the series
// m8_pinned{app="checkout", case="pinned-fingerprint", pod="p1"}, measured
// with the default fingerprint function (CityHash) on 2026-11-20.
const pinnedMetricFingerprint = "16029035836432057331"

// remoteWritePaths are the URLs gigapipe accepts remote-write on
// (writer/router/prom.go).
var remoteWritePaths = []string{"/api/v1/prom/remote/write", "/v1/prom/remote/write", "/prom/remote/write", "/api/prom/remote/write"}

func TestPromRemoteWriteConformance(t *testing.T) {
	run := fmt.Sprintf("rw-%d", time.Now().UnixNano())
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)

	// The M2 seed: 8 series (a counter and a gauge per pod), 40 samples each.
	t.Run("seed metrics", func(t *testing.T) {
		series := seed.Default(run, start).Metrics()
		remoteWrite(t, remoteWritePaths[0], seed.PromRemoteWrite(series))
		assertSeedMetricsStored(t, run, series)
	})

	t.Run("every remote-write URL stores the same way", func(t *testing.T) {
		for i, path := range remoteWritePaths {
			remoteWrite(t, path, seed.PromRemoteWrite([]seed.MetricSeries{{
				Labels:  map[string]string{"__name__": "m8_path_probe", seed.RunLabel: run, "case": fmt.Sprintf("path-%d", i)},
				Samples: []seed.MetricSample{{Time: start, Value: float64(i)}},
			}}))
		}
		for i, path := range remoteWritePaths {
			name := fmt.Sprintf("path-%d", i)
			series := waitForCase(t, run, name, 1, 1)
			want := map[string]string{"__name__": "m8_path_probe", seed.RunLabel: run, "case": name, "service_name": "unknown"}
			if got := series[0].labelMap(t); !maps.Equal(got, want) || series[0].Type != sampleTypeMetric {
				t.Errorf("%s: labels %v type %d, want %v type %d", path, got, series[0].Type, want, sampleTypeMetric)
			}
			got := samplesWhere(t, "fingerprint = "+series[0].Fingerprint)
			wantSamples := []sampleRow{{series[0].Fingerprint, start.UnixNano(), float64(i), "", sampleTypeMetric}}
			if !slices.Equal(got, wantSamples) {
				t.Errorf("%s: samples %v, want %v", path, got, wantSamples)
			}
		}
	})

	t.Run("fingerprint is pinned", func(t *testing.T) {
		// A millisecond time only this run uses, so earlier runs' samples of
		// this fixed series don't count.
		at := time.Now().Add(-5 * time.Minute).Truncate(time.Millisecond)
		remoteWrite(t, remoteWritePaths[0], seed.PromRemoteWrite([]seed.MetricSeries{{
			Labels:  map[string]string{"__name__": "m8_pinned", "app": "checkout", "case": "pinned-fingerprint", "pod": "p1"},
			Samples: []seed.MetricSample{{Time: at, Value: 42.5}},
		}}))
		got := waitForSamples(t, fmt.Sprintf("fingerprint = %s AND timestamp_ns = %d", pinnedMetricFingerprint, at.UnixNano()), 1)
		want := []sampleRow{{pinnedMetricFingerprint, at.UnixNano(), 42.5, "", sampleTypeMetric}}
		if !slices.Equal(got, want) {
			t.Errorf("samples %v, want %v", got, want)
		}
		// Only the metric-type row: this fixed series' rows from earlier runs
		// stay in the table, and the sample above already checks this run's type.
		wantLabels := map[string]string{"__name__": "m8_pinned", "app": "checkout", "case": "pinned-fingerprint", "pod": "p1", "service_name": "checkout"}
		for _, s := range waitForSeries(t, fmt.Sprintf("fingerprint = %s AND type = %d", pinnedMetricFingerprint, sampleTypeMetric), 1) {
			if !maps.Equal(s.labelMap(t), wantLabels) {
				t.Errorf("series row %+v, want labels %v", s, wantLabels)
			}
		}
	})

	// Prometheus sends a series' samples over many requests.
	t.Run("the same series in two requests", func(t *testing.T) {
		labels := map[string]string{"__name__": "m8_repeat", seed.RunLabel: run, "case": "repeat"}
		remoteWrite(t, remoteWritePaths[0], seed.PromRemoteWrite([]seed.MetricSeries{{Labels: labels, Samples: []seed.MetricSample{{Time: start, Value: 1}}}}))
		remoteWrite(t, remoteWritePaths[0], seed.PromRemoteWrite([]seed.MetricSeries{{Labels: labels, Samples: []seed.MetricSample{{Time: start.Add(seed.ScrapeInterval), Value: 2}}}}))
		series := waitForCase(t, run, "repeat", 2, 1)
		fps := map[string]bool{}
		for _, s := range series {
			fps[s.Fingerprint] = true
		}
		if len(fps) != 1 {
			t.Fatalf("%d fingerprints for one series, want 1: %+v", len(fps), series)
		}
		got := samplesWhere(t, "fingerprint = "+series[0].Fingerprint)
		want := []sampleRow{
			{series[0].Fingerprint, start.UnixNano(), 1, "", sampleTypeMetric},
			{series[0].Fingerprint, start.Add(seed.ScrapeInterval).UnixNano(), 2, "", sampleTypeMetric},
		}
		if !slices.Equal(got, want) {
			t.Errorf("samples %v, want %v", got, want)
		}
	})

	// When a target disappears, Prometheus sends a staleness marker: a NaN with
	// the bit pattern 0x7ff0000000000002.
	t.Run("staleness marker is stored with its exact bits", func(t *testing.T) {
		stale := math.Float64frombits(0x7ff0000000000002)
		remoteWrite(t, remoteWritePaths[0], seed.PromRemoteWrite([]seed.MetricSeries{{
			Labels:  map[string]string{"__name__": "up", seed.RunLabel: run, "case": "stale", "instance": "10.0.0.1:9100", "job": "node"},
			Samples: []seed.MetricSample{{Time: start, Value: 1}, {Time: start.Add(seed.ScrapeInterval), Value: stale}},
		}}))
		series := waitForCase(t, run, "stale", 2, 1)
		var bits []string
		for _, row := range query(t, fmt.Sprintf("SELECT hex(reinterpretAsUInt64(value)) AS bits FROM %s WHERE fingerprint = %s ORDER BY timestamp_ns",
			table(samplesTable), series[0].Fingerprint)) {
			bits = append(bits, fmt.Sprint(row["bits"]))
		}
		if want := []string{"3FF0000000000000", "7FF0000000000002"}; !slices.Equal(bits, want) {
			t.Errorf("value bits %v, want %v (1, then the staleness marker)", bits, want)
		}
	})

	// Prometheus 3 accepts UTF-8 metric and label names, as OpenTelemetry
	// names them.
	t.Run("UTF-8 names: label names cleaned up, the metric name kept", func(t *testing.T) {
		remoteWrite(t, remoteWritePaths[0], seed.PromRemoteWrite([]seed.MetricSeries{{
			Labels:  map[string]string{"__name__": "http.server.request.duration", seed.RunLabel: run, "case": "utf8", "http.request.method": "GET"},
			Samples: []seed.MetricSample{{Time: start, Value: 0.25}},
		}}))
		series := waitForCase(t, run, "utf8", 1, 1)
		want := map[string]string{"__name__": "http.server.request.duration", seed.RunLabel: run, "case": "utf8", "http_request_method": "GET", "service_name": "unknown"}
		if got := series[0].labelMap(t); !maps.Equal(got, want) {
			t.Errorf("labels %v, want %v", got, want)
		}
	})
}

// assertSeedMetricsStored checks every row the seed series of one run should
// give: a series row per series and day, a sample per scrape, the label index,
// and the 15-second rollup.
func assertSeedMetricsStored(t *testing.T, run string, series []seed.MetricSeries) {
	t.Helper()
	total, seriesRows := 0, 0
	for _, s := range series {
		total += len(s.Samples)
		seriesRows += len(sampleDays(s.Samples))
	}
	runFilter := fmt.Sprintf("JSONExtractString(labels, %s) = %s", sqlString(seed.RunLabel), sqlString(run))
	waitForSamples(t, fmt.Sprintf("fingerprint IN (SELECT fingerprint FROM %s WHERE %s)", table(seriesTable), runFilter), total)

	// Labels as stored: in the order sent (names sorted, as Prometheus sends
	// them), with service_name, copied from app, last.
	byLabels := map[string][]seriesRow{}
	for _, s := range waitForSeries(t, runFilter, seriesRows) {
		byLabels[s.Labels] = append(byLabels[s.Labels], s)
	}
	if len(byLabels) != len(series) {
		t.Errorf("%d distinct label sets in %s, want %d", len(byLabels), seriesTable, len(series))
	}
	fingerprints := map[string]bool{}
	for _, s := range series {
		labels := fmt.Sprintf(`{"__name__":%q,"app":%q,%q:%q,"pod":%q,"service_name":%q}`,
			s.Labels["__name__"], s.Labels["app"], seed.RunLabel, s.Labels[seed.RunLabel], s.Labels["pod"], s.Labels["app"])
		rows := byLabels[labels]
		if len(rows) == 0 {
			t.Errorf("no %s row with labels %s", seriesTable, labels)
			continue
		}
		fp := rows[0].Fingerprint
		var gotDates []string
		for _, r := range rows {
			gotDates = append(gotDates, r.Date)
			if r.Fingerprint != fp || r.Type != sampleTypeMetric || r.Name != "" || r.Metadata != "" {
				t.Errorf("%s row %+v: want fingerprint %s, type %d, empty name and metadata", seriesTable, r, fp, sampleTypeMetric)
			}
		}
		if dates := sampleDays(s.Samples); !slices.Equal(gotDates, dates) {
			t.Errorf("%s: dates %v, want %v", labels, gotDates, dates)
		}
		if fingerprints[fp] {
			t.Errorf("fingerprint %s is shared by two series", fp)
		}
		fingerprints[fp] = true

		// Remote-write timestamps are milliseconds; gigapipe stores nanoseconds.
		var want []sampleRow
		sum, minV, maxV := 0.0, math.Inf(1), math.Inf(-1)
		for _, sample := range s.Samples {
			want = append(want, sampleRow{fp, sample.Time.UnixMilli() * int64(time.Millisecond), sample.Value, "", sampleTypeMetric})
			sum += sample.Value
			minV, maxV = min(minV, sample.Value), max(maxV, sample.Value)
		}
		if got := samplesWhere(t, "fingerprint = "+fp); !slices.Equal(got, want) {
			t.Errorf("%s: %s rows\n got %v\nwant %v", labels, samplesTable, got, want)
		}

		var index []string
		for _, row := range query(t, fmt.Sprintf("SELECT DISTINCT key, val, type FROM %s WHERE fingerprint = %s ORDER BY key",
			table(labelIndexTable), fp)) {
			index = append(index, fmt.Sprintf("%v=%v type %v", row["key"], row["val"], row["type"]))
		}
		var wantIndex []string
		for _, k := range []string{"__name__", "app", seed.RunLabel, "pod", "service_name"} {
			v := s.Labels[k]
			if k == "service_name" {
				v = s.Labels["app"]
			}
			wantIndex = append(wantIndex, fmt.Sprintf("%s=%s type %d", k, v, sampleTypeMetric))
		}
		if !slices.Equal(index, wantIndex) {
			t.Errorf("%s: %s rows %v, want %v", labels, labelIndexTable, index, wantIndex)
		}

		rollup := query(t, fmt.Sprintf("SELECT toString(countMerge(count)) AS r_count, toString(sum(sum)) AS r_sum, toString(min(min)) AS r_min, "+
			"toString(max(max)) AS r_max, toString(argMaxMerge(last)) AS r_last, toString(sum(bytes)) AS r_bytes, groupUniqArray(type) AS r_types FROM %s WHERE fingerprint = %s",
			table(metricsRollupTable), fp))
		wantRollup := fmt.Sprintf("count %d, sum %s, min %s, max %s, last %s, bytes 0, types [%d]", len(s.Samples),
			formatFloat(sum), formatFloat(minV), formatFloat(maxV), formatFloat(s.Samples[len(s.Samples)-1].Value), sampleTypeMetric)
		r := rollup[0]
		if got := fmt.Sprintf("count %v, sum %v, min %v, max %v, last %v, bytes %v, types %v",
			r["r_count"], r["r_sum"], r["r_min"], r["r_max"], r["r_last"], r["r_bytes"], r["r_types"]); got != wantRollup {
			t.Errorf("%s: %s %s, want %s", labels, metricsRollupTable, got, wantRollup)
		}
	}
}

// sampleDays lists the UTC dates of samples, in order, each once.
func sampleDays(samples []seed.MetricSample) []string {
	var out []string
	for _, s := range samples {
		if d := day(s.Time); !slices.Contains(out, d) {
			out = append(out, d)
		}
	}
	return out
}

// formatFloat formats v the way ClickHouse's toString does for the whole
// numbers the seed uses.
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// remoteWrite sends body to path with the headers Prometheus sends. It is
// postRemoteWrite (metrics_roundtrip_test.go) with the path as a parameter.
func remoteWrite(t *testing.T, path string, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL()+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("remote write to %s: status %d, body %q", path, resp.StatusCode, raw)
	}
}
