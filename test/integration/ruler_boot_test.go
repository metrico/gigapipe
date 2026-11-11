//go:build integration

// Ruler boot-order lane (M2, #920): with the ruler enabled, gigapipe must
// start it, which only works when it boots after the writer and the reader
// (see bootSequence in cmd/gigapipe). A ruler started too early logs an error
// and registers no routes, so the server is up and nothing else looks wrong.
// This checks it from outside: the rules API answers, and a recording rule
// uploaded through it produces samples.

package integration

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
)

func TestRulerBootOrder(t *testing.T) {
	base := startGigapipe(t, processEnv(freshDatabase(t, "m2_ruler"),
		"MODE=all", "GIGAPIPE_RULER_ENABLED=true", "GIGAPIPE_RULER_POLL_INTERVAL=2s"))

	t.Run("rules API answers", func(t *testing.T) {
		resp, err := http.Get(base + "/api/v1/rules")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("GET /api/v1/rules: status %d (404 means the ruler did not start), body %q", resp.StatusCode, body)
		}
	})

	t.Run("recording rule produces samples", func(t *testing.T) {
		run := fmt.Sprintf("ruler-%d", time.Now().UnixNano())
		// The last scrape is within a minute of now, inside the lookback of the
		// rule's evaluation.
		fixture := seed.Default(run, time.Now().Truncate(time.Minute).Add(-(seed.Scrapes-1)*seed.ScrapeInterval))
		series := fixture.Metrics()
		postTo(t, base+"/api/v1/prom/remote/write", "application/x-protobuf",
			map[string]string{"Content-Encoding": "snappy", "X-Prometheus-Remote-Write-Version": "0.1.0"},
			seed.PromRemoteWrite(series))

		const record = "app:http_requests:sum"
		rules := fmt.Sprintf("name: m2\ninterval: 5s\nrules:\n  - record: %s\n    expr: sum by (app) (http_requests_total{%s=%q})\n",
			record, seed.RunLabel, run)
		postTo(t, base+"/api/v1/rules/m2", "application/yaml", nil, []byte(rules))

		want := map[string]float64{}
		for _, s := range series {
			if s.Labels["__name__"] == "http_requests_total" {
				// gigapipe adds service_name to every series it writes, including
				// the ones the ruler writes back.
				want[labelKey(withServiceName(map[string]string{"__name__": record, "app": s.Labels["app"]}))] += s.Samples[len(s.Samples)-1].Value
			}
		}
		deadline := time.Now().Add(60 * time.Second)
		for {
			resp, err := http.Get(base + "/api/v1/query?" + url.Values{"query": {record}}.Encode())
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("query %s: status %d, body %q", record, resp.StatusCode, raw)
			}
			got := vectorByLabels(t, raw)
			if len(got) == len(want) {
				assertFloatMap(t, got, want, 0)
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("after 60s %s returns %v, want %v", record, got, want)
			}
			time.Sleep(time.Second)
		}
	})
}

func postTo(t *testing.T, target, contentType string, headers map[string]string, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s: status %d, body %q", target, resp.StatusCode, raw)
	}
}
