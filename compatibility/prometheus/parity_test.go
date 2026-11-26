//go:build parity

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
// Run it with run.sh, which builds gigapipe from this tree and installs the
// Prometheus version gigapipe's go.mod uses. Against a running stack, from the
// repository root:
//
//	GIGAPIPE_URL=http://localhost:3100 PROMETHEUS_BINARY=/path/to/prometheus \
//		go test -tags parity -v -run '^TestPromQLParity$' ./compatibility/prometheus/
//
// PARITY_STEP (for example 60s) replaces the suite's 10s range query step on
// both sides.
package prometheus

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
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
	// step, when set, replaces the step of every range query.
	step string
	// queryAddr is where Run serves queries, once it has started.
	queryAddr atomic.Pointer[string]
}

func (*reference) Name() string { return "prometheus" }

func (r *reference) Run(ctx context.Context, o promql.Options) error {
	r.queryAddr.Store(&o.QueryHostPort)
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
# The replay's samples are up to 70 minutes old, but its up sample is stamped
# now, and without an out-of-order window samples more than an hour older
# than the newest one are rejected.
storage:
  tsdb:
    out_of_order_time_window: 2h
`, o.ScrapeInterval, o.ScrapeTargetJobName, o.ScrapeTargetHostPort)
	cfgFile := filepath.Join(dir, "prometheus.yml")
	if err := os.WriteFile(cfgFile, []byte(cfg), 0o600); err != nil {
		return err
	}
	listen := o.QueryHostPort
	if r.step != "" {
		// Prometheus listens elsewhere; the query address is a proxy that
		// rewrites the step.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		listen = l.Addr().String()
		l.Close()
		backend := &url.URL{Scheme: "http", Host: listen}
		stop, err := serve(o.QueryHostPort, stepProxy(backend, r.step))
		if err != nil {
			return err
		}
		defer stop()
	}
	return promql.RunCommand(ctx, dir, nil, r.binary,
		"--web.listen-address="+listen,
		"--storage.tsdb.path="+dir,
		"--config.file="+cfgFile)
}

// serve listens on addr in the background and returns a function that stops
// the server.
func serve(addr string, h http.Handler) (func(), error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(l) }()
	return func() { _ = srv.Close() }, nil
}

// stepProxy forwards requests to target. When step is set, it replaces the
// step of every range query, sent as a URL parameter or as a form field.
func stepProxy(target *url.URL, step string) http.Handler {
	return &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
		pr.SetURL(target)
		r := pr.Out
		if step == "" || !strings.HasSuffix(r.URL.Path, "/api/v1/query_range") {
			return
		}
		if q := r.URL.Query(); q.Has("step") {
			q.Set("step", step)
			r.URL.RawQuery = q.Encode()
		}
		if r.Method != http.MethodPost || r.Body == nil ||
			!strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
			return
		}
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if form, perr := url.ParseQuery(string(body)); err == nil && perr == nil && form.Has("step") {
			form.Set("step", step)
			body = []byte(form.Encode())
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		r.Header.Set("Content-Length", strconv.Itoa(len(body)))
	}}
}

// gigapipe feeds the replayer's samples to a running gigapipe through remote
// write and serves the suite's queries by proxying them to it.
type gigapipe struct {
	base *url.URL
	// step, when set, replaces the step of every range query.
	step string
	// ref is the reference, whose data Run checks against what was sent.
	ref *reference
	// nativeHistograms counts native histogram samples that were not sent:
	// remote write here carries classic float samples only.
	nativeHistograms atomic.Int64
	// checkSent, checkFirstMs and checkLastMs describe the checkMetric
	// samples sent. Only Run's goroutine uses them.
	checkSent                 int
	checkFirstMs, checkLastMs int64
}

// checkMetric is the gauge counted on both sides after the replay, the one
// the suite's own readiness check uses.
const checkMetric = "demo_num_cpus"

func (*gigapipe) Name() string { return "gigapipe" }

// Run forwards every snapshot before it serves queries. The suite starts
// querying once a target answers for the replay's end time, which the
// second-to-last snapshot already does, so serving earlier would let the
// first queries run before the last snapshot is stored. The replayer serves
// an empty scrape only after its last snapshot, so the first empty scrape
// means everything was sent.
//
// It then waits until the reference holds the same data, and fails the setup
// if it doesn't: a scrape that misses the reference's timeout loses that
// snapshot on the reference side only, and every query covering it would
// differ.
func (g *gigapipe) Run(ctx context.Context, o promql.Options) error {
	snapshots := 0
	for {
		n, err := g.forwardScrape(ctx, o)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n == 0 {
			break
		}
		snapshots++
	}
	// The reference scrapes one snapshot per interval; allow it that long,
	// plus a margin for its start-up.
	deadline := time.Duration(snapshots)*o.ScrapeInterval + 30*time.Second
	if err := g.waitReference(ctx, deadline); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	stop, err := serve(o.QueryHostPort, stepProxy(g.base, g.step))
	if err != nil {
		return fmt.Errorf("query proxy: %w", err)
	}
	defer stop()
	<-ctx.Done()
	return nil
}

// waitReference waits until the reference holds as many checkMetric samples
// as were sent to gigapipe, counted by the reference itself over the whole
// replay. It fails if the reference has a different count when the deadline
// passes.
func (g *gigapipe) waitReference(ctx context.Context, deadline time.Duration) error {
	if g.checkSent == 0 {
		return fmt.Errorf("no %s samples in the replay", checkMetric)
	}
	end := time.UnixMilli(g.checkLastMs)
	window := time.Duration(g.checkLastMs-g.checkFirstMs)*time.Millisecond + time.Minute
	query := fmt.Sprintf("sum(count_over_time(%s[%ds]))", checkMetric, int64(window.Seconds()))

	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	// held is the last count the reference answered, -1 before it has
	// answered; lastErr is the last query error.
	held, lastErr := -1, error(nil)
	for {
		if addr := g.ref.queryAddr.Load(); addr != nil {
			n, err := queryCount(ctx, *addr, query, end)
			if err == nil && n == g.checkSent {
				return nil
			}
			if err == nil {
				held = n
			} else {
				lastErr = err
			}
		}
		select {
		case <-ctx.Done():
			if held < 0 {
				return fmt.Errorf("reference data check: %w", cmp.Or(lastErr, ctx.Err()))
			}
			return fmt.Errorf("the reference holds %d %s samples, gigapipe was sent %d: "+
				"the reference lost scrapes during the replay, so the two sides would be "+
				"compared on different data", held, checkMetric, g.checkSent)
		case <-tick.C:
		}
	}
}

// queryCount runs an instant query that returns one number on the Prometheus
// API at addr. An empty result is 0.
func queryCount(ctx context.Context, addr, query string, at time.Time) (int, error) {
	client, err := api.NewClient(api.Config{Address: "http://" + addr})
	if err != nil {
		return 0, err
	}
	val, _, _, err := v1.NewAPI(client).Query(ctx, query, at)
	if err != nil {
		return 0, err
	}
	vec, ok := val.(model.Vector)
	if !ok {
		return 0, fmt.Errorf("%s: got %s, want a vector", query, val.Type())
	}
	if len(vec) == 0 {
		return 0, nil
	}
	return int(vec[0].Value), nil
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
		if name == checkMetric {
			ts := m.GetTimestampMs()
			if g.checkSent == 0 || ts < g.checkFirstMs {
				g.checkFirstMs = ts
			}
			g.checkLastMs = max(g.checkLastMs, ts)
			g.checkSent++
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

// scrapeInterval is the replay's scrape interval on both sides, and so the
// reference's scrape timeout.
const scrapeInterval = 250 * time.Millisecond

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

	// The suite scrapes every 15ms, and Prometheus caps a scrape's timeout at
	// the interval, so any scrape slower than 15ms lost its snapshot on the
	// reference side. The suite allows 2 minutes for the replay of 174
	// snapshots, so the interval can't go much higher than this.
	opts := []promql.RunOption{promql.WithScrapeInterval(scrapeInterval)}
	// Gigapipe adds a service_name label to every series, which Prometheus
	// doesn't have, so it is left out of the comparison by default.
	// PARITY_KEEP_SERVICE_NAME=1 compares it too.
	if os.Getenv("PARITY_KEEP_SERVICE_NAME") == "" {
		opts = append(opts, promql.WithQueryTweaks(&config.QueryTweak{
			Note:             "gigapipe adds a service_name label to every series",
			DropResultLabels: []model.LabelName{"service_name"},
		}))
	}

	// PARITY_STEP replaces the suite's 10s range query step on both sides,
	// as seconds, which both APIs accept.
	var step string
	if v := os.Getenv("PARITY_STEP"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			t.Fatalf("PARITY_STEP=%q: want a positive duration such as 60s", v)
		}
		step = strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
		t.Logf("range queries use a %s step instead of the suite's 10s", d)
	}

	ref := &reference{binary: promBinary, step: step}
	target := &gigapipe{base: base, step: step, ref: ref}
	t.Cleanup(func() {
		if n := target.nativeHistograms.Load(); n > 0 {
			t.Logf("native histogram samples not sent to gigapipe: %d", n)
		}
	})
	promql.RunTests(t, ref, target, promql.ComplianceTests(), opts...)
}
