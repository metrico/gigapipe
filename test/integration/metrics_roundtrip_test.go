//go:build integration

// Write-read round trip for metrics (M2, #920): the seed fixture's metrics are
// sent as a Prometheus remote-write request and read back through the
// Prometheus query API, each answer checked against the fixture. See
// logs_roundtrip_test.go for the pinned responses.

package integration

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
)

func TestMetricsRoundTrip(t *testing.T) {
	run := fmt.Sprintf("metrics-%d", time.Now().UnixNano())
	start := time.Now().Add(-15 * time.Minute).Truncate(time.Minute)
	fixture := seed.Default(run, start)
	series := fixture.Metrics()
	// The last scrape: instant queries are evaluated there, like a dashboard
	// refreshed right after it.
	end := start.Add((seed.Scrapes - 1) * seed.ScrapeInterval)
	runMatcher := fmt.Sprintf("%s=%q", seed.RunLabel, run)
	norm := normalizer{run: run, start: start}

	postRemoteWrite(t, seed.PromRemoteWrite(series))
	waitForLastSamples(t, fmt.Sprintf("http_requests_total{%s}", runMatcher), end, len(series)/2)

	t.Run("counter value per pod at the last scrape", func(t *testing.T) {
		raw := apiGet(t, "/api/v1/query", url.Values{
			"query": {fmt.Sprintf("http_requests_total{%s}", runMatcher)},
			"time":  {unixSeconds(end)},
		})
		got := vectorByLabels(t, raw)
		want := map[string]float64{}
		for _, s := range series {
			if s.Labels["__name__"] == "http_requests_total" {
				want[labelKey(withServiceName(s.Labels))] = s.Samples[len(s.Samples)-1].Value
			}
		}
		assertFloatMap(t, got, want, 0)
		assertPinned(t, "metrics/query_counter.json", norm.vector(t, raw, end))
	})

	ratePerApp := func() map[string]float64 {
		want := map[string]float64{}
		for _, svc := range fixture.Services {
			for _, pod := range svc.Pods {
				want[labelKey(map[string]string{"app": svc.Name})] += float64(pod.RequestsPerScrape) / seed.ScrapeInterval.Seconds()
			}
		}
		return want
	}
	rateQuery := fmt.Sprintf("sum by (app) (rate(http_requests_total{%s}[5m]))", runMatcher)

	t.Run("requests per second per app", func(t *testing.T) {
		raw := apiGet(t, "/api/v1/query", url.Values{"query": {rateQuery}, "time": {unixSeconds(end)}})
		assertFloatMap(t, vectorByLabels(t, raw), ratePerApp(), 1e-9)
	})

	t.Run("requests per second over time", func(t *testing.T) {
		from := start.Add(5 * time.Minute)
		raw := apiGet(t, "/api/v1/query_range", url.Values{
			"query": {rateQuery},
			"start": {unixSeconds(from)},
			"end":   {unixSeconds(end)},
			"step":  {"60"},
		})
		want := ratePerApp()
		for key, points := range matrixByLabels(t, raw) {
			wantRate, ok := want[key]
			if !ok {
				t.Errorf("unexpected series %s", key)
				continue
			}
			// One point per step from the start of the range up to its end.
			if wantPoints := int(end.Sub(from)/time.Minute) + 1; len(points) != wantPoints {
				t.Errorf("%s: %d points, want %d", key, len(points), wantPoints)
			}
			for _, p := range points {
				if math.Abs(p.value-wantRate) > 1e-9 {
					t.Errorf("%s at %v: %v requests/s, want %v", key, p.time, p.value, wantRate)
				}
			}
			delete(want, key)
		}
		for key := range want {
			t.Errorf("missing series %s", key)
		}
	})

	t.Run("total requests per app over time", func(t *testing.T) {
		from := start.Add(5 * time.Minute)
		raw := apiGet(t, "/api/v1/query_range", url.Values{
			"query": {fmt.Sprintf("sum by (app) (http_requests_total{%s})", runMatcher)},
			"start": {unixSeconds(from)},
			"end":   {unixSeconds(end)},
			"step":  {"60"},
		})
		// Each step falls on a scrape, so a pod contributes the sample taken at
		// that step.
		want := map[string][]matrixPoint{}
		for at := from; !at.After(end); at = at.Add(time.Minute) {
			scrape := int(at.Sub(start) / seed.ScrapeInterval)
			perApp := map[string]float64{}
			for _, s := range series {
				if s.Labels["__name__"] == "http_requests_total" {
					perApp[s.Labels["app"]] += s.Samples[scrape].Value
				}
			}
			for app, v := range perApp {
				key := labelKey(map[string]string{"app": app})
				want[key] = append(want[key], matrixPoint{float64(at.Unix()), v})
			}
		}
		got := matrixByLabels(t, raw)
		for key, points := range want {
			if !slices.Equal(got[key], points) {
				t.Errorf("%s:\n got %v\nwant %v", key, got[key], points)
			}
		}
		for key := range got {
			if _, ok := want[key]; !ok {
				t.Errorf("unexpected series %s", key)
			}
		}
		assertPinned(t, "metrics/query_range_total.json", norm.matrix(t, raw))
	})

	t.Run("peak memory per pod over ten minutes", func(t *testing.T) {
		raw := apiGet(t, "/api/v1/query", url.Values{
			"query": {fmt.Sprintf("max_over_time(process_resident_memory_bytes{%s}[10m])", runMatcher)},
			"time":  {unixSeconds(end)},
		})
		want := map[string]float64{}
		for _, s := range series {
			if s.Labels["__name__"] != "process_resident_memory_bytes" {
				continue
			}
			peak := 0.0
			for _, sample := range s.Samples {
				peak = max(peak, sample.Value)
			}
			// *_over_time functions drop the metric name.
			labels := withServiceName(s.Labels)
			delete(labels, "__name__")
			want[labelKey(labels)] = peak
		}
		assertFloatMap(t, vectorByLabels(t, raw), want, 0)
	})

	matchParams := url.Values{
		"match[]": {fmt.Sprintf("{%s}", runMatcher)},
		"start":   {unixSeconds(start)},
		"end":     {unixSeconds(end)},
	}

	t.Run("series lists every series", func(t *testing.T) {
		raw := apiGet(t, "/api/v1/series", matchParams)
		var resp struct {
			Data []map[string]string `json:"data"`
		}
		mustUnmarshal(t, raw, &resp)
		var got, want []string
		for _, s := range resp.Data {
			got = append(got, labelKey(s))
		}
		for _, s := range series {
			want = append(want, labelKey(withServiceName(s.Labels)))
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("series:\n got %q\nwant %q", got, want)
		}
		assertPinned(t, "metrics/series.json", norm.series(t, raw))
	})

	t.Run("label names", func(t *testing.T) {
		raw := apiGet(t, "/api/v1/labels", matchParams)
		var resp struct {
			Data []string `json:"data"`
		}
		mustUnmarshal(t, raw, &resp)
		got := slices.Clone(resp.Data)
		slices.Sort(got)
		want := []string{"__name__", "app", seed.RunLabel, "pod", "service_name"}
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("labels = %v, want %v", got, want)
		}
	})

	t.Run("label values of pod", func(t *testing.T) {
		raw := apiGet(t, "/api/v1/label/pod/values", matchParams)
		var resp struct {
			Data []string `json:"data"`
		}
		mustUnmarshal(t, raw, &resp)
		var want []string
		for _, s := range series {
			if !slices.Contains(want, s.Labels["pod"]) {
				want = append(want, s.Labels["pod"])
			}
		}
		got := slices.Clone(resp.Data)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("pod values = %v, want %v", got, want)
		}
	})
}

func unixSeconds(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

func postRemoteWrite(t *testing.T, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL()+"/api/v1/prom/remote/write", bytes.NewReader(body))
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
		t.Fatalf("remote write: status %d, body %q", resp.StatusCode, raw)
	}
}

// waitForLastSamples polls until query, evaluated at the last scrape, returns
// want series, since ingestion is flushed asynchronously. On timeout it only
// logs, so the named checks that follow still run and report what is wrong.
func waitForLastSamples(t *testing.T, query string, at time.Time, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := len(vectorByLabels(t, apiGet(t, "/api/v1/query", url.Values{"query": {query}, "time": {unixSeconds(at)}})))
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("after 30s %s returns %d series, want %d; checking anyway", query, got, want)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// vectorByLabels decodes an instant query vector response into values keyed by
// label set.
func vectorByLabels(t *testing.T, raw []byte) map[string]float64 {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	mustUnmarshal(t, raw, &resp)
	if resp.Data.ResultType != "vector" {
		t.Fatalf("resultType = %q, want vector", resp.Data.ResultType)
	}
	out := map[string]float64{}
	for _, r := range resp.Data.Result {
		out[labelKey(r.Metric)] = parseSampleValue(t, r.Value[1])
	}
	return out
}

type matrixPoint struct {
	time  float64
	value float64
}

// matrixByLabels decodes a range query matrix response into points keyed by
// label set.
func matrixByLabels(t *testing.T, raw []byte) map[string][]matrixPoint {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	mustUnmarshal(t, raw, &resp)
	if resp.Data.ResultType != "matrix" {
		t.Fatalf("resultType = %q, want matrix", resp.Data.ResultType)
	}
	out := map[string][]matrixPoint{}
	for _, r := range resp.Data.Result {
		for _, v := range r.Values {
			ts, ok := v[0].(float64)
			if !ok {
				t.Fatalf("point time %v is not a number", v[0])
			}
			out[labelKey(r.Metric)] = append(out[labelKey(r.Metric)], matrixPoint{ts, parseSampleValue(t, v[1])})
		}
	}
	return out
}

// parseSampleValue parses a sample value, which the Prometheus API encodes as
// a string.
func parseSampleValue(t *testing.T, v any) float64 {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("sample value %v is not a string", v)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("sample value %q: %v", s, err)
	}
	return f
}

func assertFloatMap(t *testing.T, got, want map[string]float64, tolerance float64) {
	t.Helper()
	for key, w := range want {
		g, ok := got[key]
		if !ok {
			t.Errorf("missing %s (want %v)", key, w)
			continue
		}
		if math.Abs(g-w) > tolerance {
			t.Errorf("%s = %v, want %v", key, g, w)
		}
	}
	for key, g := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected %s = %v", key, g)
		}
	}
}

// matrix normalizes a range query matrix response.
func (n normalizer) matrix(t *testing.T, raw []byte) any {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	mustUnmarshal(t, raw, &resp)
	for i, r := range resp.Data.Result {
		r.Metric = n.labels(r.Metric)
		for j, v := range r.Values {
			ts, ok := v[0].(float64)
			if !ok {
				t.Fatalf("point time %v is not a number", v[0])
			}
			r.Values[j][0] = n.offset(time.Unix(int64(ts), 0))
		}
		resp.Data.Result[i] = r
	}
	sort.Slice(resp.Data.Result, func(i, j int) bool {
		return labelKey(resp.Data.Result[i].Metric) < labelKey(resp.Data.Result[j].Metric)
	})
	return resp
}
