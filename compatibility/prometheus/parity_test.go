// Package prometheus runs the PromQL compliance suite
// (github.com/prometheus/compliance/promql) with a real Prometheus as the
// reference and gigapipe as the target, so every query answered differently
// from Prometheus shows up as a failing subtest.
//
// The suite replays a recorded 70-minute dataset through a scrape endpoint.
// Prometheus scrapes it directly. Gigapipe doesn't scrape, so the gigapipe
// target scrapes the replayer itself, sends the samples to gigapipe's
// remote-write endpoint, and forwards the suite's queries to gigapipe.
//
// Run it with run.sh, which builds gigapipe from this tree and downloads a
// pinned Prometheus. Against a running stack:
//
//	GIGAPIPE_URL=http://localhost:3100 PROMETHEUS_BINARY=/path/to/prometheus go test -v ./...
package prometheus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/compliance"
	"github.com/prometheus/compliance/promql"
	"github.com/prometheus/compliance/promql/config"
	"github.com/prometheus/prometheus/prompb"
)

// reference runs a Prometheus binary that scrapes the suite's replayer.
type reference struct {
	binary string
}

func (reference) Name() string { return "prometheus" }

func (r reference) Run(ctx context.Context, o promql.Options) error {
	dir, err := os.MkdirTemp("", "parity-prometheus-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	cfg := fmt.Sprintf(`global:
  scrape_interval: %[1]s
scrape_configs:
  - job_name: %[2]q
    scrape_interval: %[1]s
    honor_labels: true
    honor_timestamps: true
    scrape_native_histograms: true
    scrape_protocols: [PrometheusProto, OpenMetricsText1.0.0, PrometheusText0.0.4]
    static_configs:
      - targets: [%[3]q]
`, o.ScrapeInterval, o.ScrapeTargetJobName, o.ScrapeTargetHostPort)
	cfgFile := filepath.Join(dir, "prometheus.yml")
	if err := os.WriteFile(cfgFile, []byte(cfg), 0o600); err != nil {
		return err
	}
	return promql.RunCommand(ctx, dir, nil, r.binary,
		"--web.listen-address="+o.QueryHostPort,
		"--storage.tsdb.path="+dir,
		"--config.file="+cfgFile)
}

// gigapipe feeds the replayer's samples to a running gigapipe through remote
// write and serves the suite's queries by proxying them to it.
type gigapipe struct {
	base *url.URL
	// nativeHistograms counts native histogram samples that were not sent:
	// remote write here carries classic float samples only.
	nativeHistograms atomic.Int64
}

func (*gigapipe) Name() string { return "gigapipe" }

func (g *gigapipe) Run(ctx context.Context, o promql.Options) error {
	srv := &http.Server{Addr: o.QueryHostPort, Handler: httputil.NewSingleHostReverseProxy(g.base)}
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()
	defer srv.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-srvErr:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return fmt.Errorf("query proxy: %w", err)
		default:
		}
		n, err := g.forwardScrape(ctx, o)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// forwardScrape scrapes the replayer once and remote-writes what it returned.
// It returns the number of series sent.
func (g *gigapipe) forwardScrape(ctx context.Context, o promql.Options) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+o.ScrapeTargetHostPort+"/metrics", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", compliance.ProtoMediaType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var series []prompb.TimeSeries
	// add appends one sample. Labels from the scrape win over job, as with
	// honor_labels in the reference scrape config.
	add := func(name string, m *dto.Metric, extra []string, v float64) {
		lbls := map[string]string{model.MetricNameLabel: name, model.JobLabel: o.ScrapeTargetJobName}
		for _, p := range m.GetLabel() {
			lbls[p.GetName()] = p.GetValue()
		}
		for i := 0; i+1 < len(extra); i += 2 {
			lbls[extra[i]] = extra[i+1]
		}
		ls := make([]prompb.Label, 0, len(lbls))
		for k, v := range lbls {
			ls = append(ls, prompb.Label{Name: k, Value: v})
		}
		sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
		series = append(series, prompb.TimeSeries{
			Labels:  ls,
			Samples: []prompb.Sample{{Value: v, Timestamp: m.GetTimestampMs()}},
		})
	}
	formatFloat := func(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

	dec := expfmt.NewDecoder(resp.Body, expfmt.NewFormat(expfmt.TypeProtoDelim))
	for {
		var mf dto.MetricFamily
		if err := dec.Decode(&mf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("decode scrape: %w", err)
		}
		name := mf.GetName()
		for _, m := range mf.GetMetric() {
			switch {
			case m.Gauge != nil:
				add(name, m, nil, m.GetGauge().GetValue())
			case m.Counter != nil:
				add(name, m, nil, m.GetCounter().GetValue())
			case m.Untyped != nil:
				add(name, m, nil, m.GetUntyped().GetValue())
			case m.Summary != nil:
				s := m.GetSummary()
				for _, q := range s.GetQuantile() {
					add(name, m, []string{model.QuantileLabel, formatFloat(q.GetQuantile())}, q.GetValue())
				}
				add(name+"_sum", m, nil, s.GetSampleSum())
				add(name+"_count", m, nil, float64(s.GetSampleCount()))
			case m.Histogram != nil:
				h := m.GetHistogram()
				if len(h.GetBucket()) == 0 {
					g.nativeHistograms.Add(1)
					continue
				}
				for _, b := range h.GetBucket() {
					add(name+"_bucket", m, []string{model.BucketLabel, formatFloat(b.GetUpperBound())}, float64(b.GetCumulativeCount()))
				}
				add(name+"_bucket", m, []string{model.BucketLabel, "+Inf"}, float64(h.GetSampleCount()))
				add(name+"_sum", m, nil, h.GetSampleSum())
				add(name+"_count", m, nil, float64(h.GetSampleCount()))
			}
		}
	}
	if len(series) == 0 {
		return 0, nil
	}

	body, err := (&prompb.WriteRequest{Timeseries: series}).Marshal()
	if err != nil {
		return 0, err
	}
	wreq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		g.base.JoinPath("/api/v1/prom/remote/write").String(), bytes.NewReader(snappy.Encode(nil, body)))
	if err != nil {
		return 0, err
	}
	wreq.Header.Set("Content-Type", "application/x-protobuf")
	wreq.Header.Set("Content-Encoding", "snappy")
	wresp, err := http.DefaultClient.Do(wreq)
	if err != nil {
		return 0, err
	}
	defer wresp.Body.Close()
	if wresp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(wresp.Body)
		return 0, fmt.Errorf("remote write: status %d: %s", wresp.StatusCode, msg)
	}
	return len(series), nil
}

// TestPromQLParity runs every compliance query against Prometheus and
// gigapipe. Each query is a subtest named TestPromQLParity/gigapipe/<query>.
func TestPromQLParity(t *testing.T) {
	gigapipeURL, promBinary := os.Getenv("GIGAPIPE_URL"), os.Getenv("PROMETHEUS_BINARY")
	if gigapipeURL == "" || promBinary == "" {
		t.Skip("GIGAPIPE_URL and PROMETHEUS_BINARY must be set; run.sh sets both")
	}
	base, err := url.Parse(gigapipeURL)
	if err != nil {
		t.Fatalf("GIGAPIPE_URL: %v", err)
	}

	var opts []promql.RunOption
	// Gigapipe adds a service_name label to every series, which Prometheus
	// doesn't have, so it is left out of the comparison by default.
	// PARITY_KEEP_SERVICE_NAME=1 compares it too.
	if os.Getenv("PARITY_KEEP_SERVICE_NAME") == "" {
		opts = append(opts, promql.WithQueryTweaks(&config.QueryTweak{
			Note:             "gigapipe adds a service_name label to every series",
			DropResultLabels: []model.LabelName{"service_name"},
		}))
	}

	target := &gigapipe{base: base}
	t.Cleanup(func() {
		if n := target.nativeHistograms.Load(); n > 0 {
			t.Logf("native histogram samples not sent to gigapipe: %d", n)
		}
	})
	promql.RunTests(t, reference{binary: promBinary}, target, promql.ComplianceTests(), opts...)
}
