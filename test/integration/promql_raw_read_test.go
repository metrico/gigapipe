//go:build integration

// PromQL over remote-written samples returns Prometheus's values. The dataset and the
// expected figures are the read-path probe's: one counter at 15s with two resets and a
// stale marker, rate[5m] 0.0666667 at 05:00 and 0.0456140 at 10:00.

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"google.golang.org/protobuf/proto"
)

// probeSamples is the probe dataset relative to t0 (ms): 1 2 3 | 1..17 to 05:00 | 18 19 |
// 2 3 | stale marker at 06:15 | 4..12 from 08:00 to 10:00.
func probeSamples(t0 int64) []*prompb.Sample {
	var res []*prompb.Sample
	add := func(from, to int, delta float64) {
		for n := from; n <= to; n++ {
			res = append(res, &prompb.Sample{Timestamp: t0 + int64(n)*15000, Value: float64(n) - delta})
		}
	}
	add(1, 3, 0)
	add(4, 20, 3)
	add(21, 22, 3)
	add(23, 24, 21)
	res = append(res, &prompb.Sample{Timestamp: t0 + 375000, Value: math.Float64frombits(0x7ff0000000000002)})
	add(32, 40, 28)
	return res
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
			Values [][2]any          `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func promGet(t *testing.T, path string, params url.Values) promResponse {
	t.Helper()
	resp, err := http.Get(baseURL() + path + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %v: status %d, body %s", path, params, resp.StatusCode, body)
	}
	var res promResponse
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("%s: %v in %s", path, err, body)
	}
	return res
}

// digits formats a JSON sample value as the probe prints its oracle figures.
func digits(t *testing.T, v any) string {
	t.Helper()
	var f float64
	if _, err := fmt.Sscan(v.(string), &f); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%.7f", f)
}

func instantValue(t *testing.T, query string, at int64) (string, bool) {
	t.Helper()
	res := promGet(t, "/api/v1/query", url.Values{"query": {query}, "time": {fmt.Sprint(at / 1000)}})
	if len(res.Data.Result) == 0 {
		return "", false
	}
	return res.Data.Result[0].Value[1].(string), true
}

func TestPromQLOverRemoteWrittenSamplesMatchesPrometheus(t *testing.T) {
	waitReady(t)
	name := fmt.Sprintf("it_probe_%d_total", time.Now().UnixNano())
	t0 := time.Now().Add(-2 * time.Hour).Truncate(time.Minute).UnixMilli()
	body, err := proto.Marshal(&prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{{
		Labels:  []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "probe"}},
		Samples: probeSamples(t0),
	}}})
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
	end := t0 + 600000
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		if v, ok := instantValue(t, name, end); ok && v == "12" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached 12 at 10:00", name)
		}
	}

	rate := fmt.Sprintf("rate(%s[5m])", name)
	rng := promGet(t, "/api/v1/query_range", url.Values{"query": {rate},
		"start": {fmt.Sprint(t0 / 1000)}, "end": {fmt.Sprint(end / 1000)}, "step": {"60"}})
	if len(rng.Data.Result) != 1 {
		t.Fatalf("query_range %s: %d series", rate, len(rng.Data.Result))
	}
	byTs := map[int64]string{}
	for _, p := range rng.Data.Result[0].Values {
		byTs[int64(p[0].(float64)*1000)] = digits(t, p[1])
	}
	for ts, want := range map[int64]string{t0 + 300000: "0.0666667", end: "0.0456140"} {
		if got := byTs[ts]; got != want {
			t.Errorf("query_range %s at +%ds = %q, want %s", rate, (ts-t0)/1000, got, want)
		}
	}
	if v, ok := instantValue(t, rate, end); !ok || digits(t, v) != "0.0456140" {
		t.Errorf("query %s at 10:00 = %q, want 0.0456140", rate, v)
	}

	for _, tc := range []struct {
		at   int64
		want string
	}{{t0 + 300000, "17"}, {t0 + 420000, ""}, {end, "12"}} {
		if v, _ := instantValue(t, name, tc.at); v != tc.want {
			t.Errorf("query %s at +%ds = %q, want %q", name, (tc.at-t0)/1000, v, tc.want)
		}
	}
}
