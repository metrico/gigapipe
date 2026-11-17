//go:build integration

// Write-read round trip for logs (M2, #920). The seed fixture is pushed through
// the Loki push API and read back through the Loki read APIs, and every check
// compares the answer with the fixture. A change to how logs are stored (for
// example the samples table split in #972) that changes what a query returns
// fails a named case.
//
// Some checks also compare the response with a pinned copy under
// testdata/logs/: that JSON is what Grafana reads, so it is a wire contract.
// The pins are maintained by hand. If one fails after an intended change,
// review the difference and edit the file; there is no regeneration mode.

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
)

func TestLogsRoundTrip(t *testing.T) {
	run := fmt.Sprintf("logs-%d", time.Now().UnixNano())
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	streams := seed.Default(run, start).Logs()
	from, to := start.Add(-time.Minute), start.Add(5*time.Minute)
	selector := fmt.Sprintf(`{%s=%q}`, seed.RunLabel, run)

	postJSON(t, "/loki/api/v1/push", seed.LokiPushJSON(streams))
	total := 0
	for _, s := range streams {
		total += len(s.Lines)
	}
	waitForLogLines(t, selector, from, to, total)

	norm := normalizer{run: run, start: start}

	t.Run("all lines come back per stream", func(t *testing.T) {
		raw := apiGet(t, "/loki/api/v1/query_range", rangeParams(selector, from, to))
		assertStreams(t, raw, expectedStreams(streams, func(seed.LogLine) bool { return true }))
		assertPinned(t, "logs/query_range_streams.json", norm.streams(t, raw))
	})

	t.Run("line filter keeps only error lines", func(t *testing.T) {
		raw := apiGet(t, "/loki/api/v1/query_range", rangeParams(selector+` |= "\"level\":\"error\""`, from, to))
		assertStreams(t, raw, expectedStreams(streams, func(l seed.LogLine) bool { return l.Level == "error" }))
	})

	t.Run("json parser and label filter keep only error lines", func(t *testing.T) {
		raw := apiGet(t, "/loki/api/v1/query_range", rangeParams(selector+` | json | level="error"`, from, to))
		var got []string
		for _, r := range decodeStreams(t, raw) {
			if r.Stream["level"] != "error" {
				t.Errorf("stream %v: level label = %q, want \"error\"", r.Stream, r.Stream["level"])
			}
			for _, v := range r.Values {
				got = append(got, r.Stream["app"]+"/"+r.Stream["pod"]+" "+v[1])
			}
		}
		var want []string
		for _, s := range streams {
			for _, l := range s.Lines {
				if l.Level == "error" {
					want = append(want, s.Labels["app"]+"/"+s.Labels["pod"]+" "+l.Text)
				}
			}
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("lines:\n got %q\nwant %q", got, want)
		}
		assertPinned(t, "logs/query_range_json_filter.json", norm.streams(t, raw))
	})

	t.Run("count_over_time per app and pod", func(t *testing.T) {
		q := fmt.Sprintf("sum by (app, pod) (count_over_time(%s[1h]))", selector)
		raw := apiGet(t, "/loki/api/v1/query", url.Values{"query": {q}, "time": {nanos(to)}})
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
		got := map[string]string{}
		for _, r := range resp.Data.Result {
			got[r.Metric["app"]+"/"+r.Metric["pod"]] = fmt.Sprint(r.Value[1])
		}
		want := map[string]string{}
		for _, s := range streams {
			want[s.Labels["app"]+"/"+s.Labels["pod"]] = strconv.Itoa(len(s.Lines))
		}
		if !mapsEqual(got, want) {
			t.Errorf("counts = %v, want %v", got, want)
		}
		assertPinned(t, "logs/query_count_by_pod.json", norm.vector(t, raw, to))
	})

	// gigapipe's labels endpoint ignores the query parameter that Loki uses to
	// narrow it to a selector, so this cannot be limited to this run's data:
	// it shows the names are listed, not that this run wrote them. The series
	// case below is limited to this run and covers the label sets.
	t.Run("labels lists every label written", func(t *testing.T) {
		raw := apiGet(t, "/loki/api/v1/labels", url.Values{"start": {nanos(from)}, "end": {nanos(to)}})
		var resp struct {
			Data []string `json:"data"`
		}
		mustUnmarshal(t, raw, &resp)
		for _, name := range []string{seed.RunLabel, "app", "pod", "service_name"} {
			if !slices.Contains(resp.Data, name) {
				t.Errorf("labels %v do not include %q", resp.Data, name)
			}
		}
	})

	t.Run("label values of pod", func(t *testing.T) {
		raw := apiGet(t, "/loki/api/v1/label/pod/values",
			url.Values{"query": {selector}, "start": {nanos(from)}, "end": {nanos(to)}})
		var resp struct {
			Data []string `json:"data"`
		}
		mustUnmarshal(t, raw, &resp)
		var want []string
		for _, s := range streams {
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
		assertPinned(t, "logs/label_pod_values.json", norm.generic(t, raw))
	})

	t.Run("series lists every stream", func(t *testing.T) {
		raw := apiGet(t, "/loki/api/v1/series",
			url.Values{"match[]": {selector}, "start": {nanos(from)}, "end": {nanos(to)}})
		var resp struct {
			Data []map[string]string `json:"data"`
		}
		mustUnmarshal(t, raw, &resp)
		var got, want []string
		for _, s := range resp.Data {
			got = append(got, labelKey(s))
		}
		for _, s := range streams {
			want = append(want, labelKey(withServiceName(s.Labels)))
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("series:\n got %q\nwant %q", got, want)
		}
		assertPinned(t, "logs/series.json", norm.series(t, raw))
	})
}

// withServiceName returns the labels gigapipe stores for a stream it was sent
// with these labels: it adds service_name, copied from app.
func withServiceName(labels map[string]string) map[string]string {
	out := map[string]string{"service_name": labels["app"]}
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// expectedStreams returns, keyed by label set, the [timestamp, line] values a
// query over the fixture should return when it keeps the lines keep accepts.
// Streams with no kept line are absent, as in a Loki response.
func expectedStreams(streams []seed.LogStream, keep func(seed.LogLine) bool) map[string][][2]string {
	out := map[string][][2]string{}
	for _, s := range streams {
		var values [][2]string
		for _, l := range s.Lines {
			if keep(l) {
				values = append(values, [2]string{nanos(l.Time), l.Text})
			}
		}
		if len(values) > 0 {
			out[labelKey(withServiceName(s.Labels))] = values
		}
	}
	return out
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

func decodeStreams(t *testing.T, raw []byte) []lokiStream {
	t.Helper()
	var resp struct {
		Data struct {
			ResultType string       `json:"resultType"`
			Result     []lokiStream `json:"result"`
		} `json:"data"`
	}
	mustUnmarshal(t, raw, &resp)
	if resp.Data.ResultType != "streams" {
		t.Fatalf("resultType = %q, want streams", resp.Data.ResultType)
	}
	return resp.Data.Result
}

// assertStreams compares a streams response with want, by label set, with the
// lines of each stream in time order (the request uses direction=forward).
func assertStreams(t *testing.T, raw []byte, want map[string][][2]string) {
	t.Helper()
	got := map[string][][2]string{}
	for _, r := range decodeStreams(t, raw) {
		key := labelKey(r.Stream)
		if _, dup := got[key]; dup {
			t.Errorf("stream %s returned twice", key)
		}
		got[key] = r.Values
	}
	for key, values := range want {
		if !slices.Equal(got[key], values) {
			t.Errorf("stream %s:\n got %q\nwant %q", key, got[key], values)
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected stream %s: %q", key, got[key])
		}
	}
}

// waitForLogLines polls until the query returns want lines in total, since
// ingestion is flushed asynchronously. On timeout it only logs, so the named
// checks that follow still run and report what is wrong.
func waitForLogLines(t *testing.T, selector string, from, to time.Time, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := 0
		for _, r := range decodeStreams(t, apiGet(t, "/loki/api/v1/query_range", rangeParams(selector, from, to))) {
			got += len(r.Values)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("after 30s the query returns %d lines, want %d; checking anyway", got, want)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func rangeParams(query string, from, to time.Time) url.Values {
	return url.Values{
		"query":     {query},
		"start":     {nanos(from)},
		"end":       {nanos(to)},
		"direction": {"forward"},
		"limit":     {"1000"},
	}
}

func nanos(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

// labelKey renders a label set in a fixed order, for comparisons.
func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%q", k, labels[k])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func apiGet(t *testing.T, path string, params url.Values) []byte {
	t.Helper()
	resp, err := http.Get(baseURL() + path + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, body %q", path, resp.StatusCode, raw)
	}
	return raw
}

func postJSON(t *testing.T, path string, body []byte) {
	t.Helper()
	resp, err := http.Post(baseURL()+path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s: status %d, body %q", path, resp.StatusCode, raw)
	}
}

func mustUnmarshal(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
}

// normalizer replaces the values that differ between runs with fixed ones, so
// a response can be compared with its pinned copy: the run label value becomes
// "RUN" and timestamps become offsets from the fixture start, such as "+10s".
// Result order, which the APIs do not define, is sorted.
type normalizer struct {
	run   string
	start time.Time
}

func (n normalizer) offset(ts time.Time) string {
	return fmt.Sprintf("%+ds", int64(ts.Sub(n.start)/time.Second))
}

func (n normalizer) labels(labels map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range labels {
		if v == n.run {
			v = "RUN"
		}
		out[k] = v
	}
	return out
}

// streams normalizes a query_range streams response.
func (n normalizer) streams(t *testing.T, raw []byte) any {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string       `json:"resultType"`
			Result     []lokiStream `json:"result"`
		} `json:"data"`
	}
	mustUnmarshal(t, raw, &resp)
	for i, r := range resp.Data.Result {
		r.Stream = n.labels(r.Stream)
		for j, v := range r.Values {
			ns, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				t.Fatalf("timestamp %q: %v", v[0], err)
			}
			r.Values[j][0] = n.offset(time.Unix(0, ns))
		}
		resp.Data.Result[i] = r
	}
	sort.Slice(resp.Data.Result, func(i, j int) bool {
		return labelKey(resp.Data.Result[i].Stream) < labelKey(resp.Data.Result[j].Stream)
	})
	return resp
}

// vector normalizes an instant query vector response evaluated at evalTime.
func (n normalizer) vector(t *testing.T, raw []byte, evalTime time.Time) any {
	t.Helper()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	mustUnmarshal(t, raw, &resp)
	for i, r := range resp.Data.Result {
		ts, ok := r.Value[0].(float64)
		if !ok {
			t.Fatalf("sample time %v is not a number", r.Value[0])
		}
		if int64(ts) != evalTime.Unix() {
			t.Errorf("sample time %v, want the evaluation time %d", ts, evalTime.Unix())
		}
		r.Metric = n.labels(r.Metric)
		r.Value[0] = n.offset(time.Unix(int64(ts), 0))
		resp.Data.Result[i] = r
	}
	sort.Slice(resp.Data.Result, func(i, j int) bool {
		return labelKey(resp.Data.Result[i].Metric) < labelKey(resp.Data.Result[j].Metric)
	})
	return resp
}

// series normalizes a series response.
func (n normalizer) series(t *testing.T, raw []byte) any {
	t.Helper()
	var resp struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"`
	}
	mustUnmarshal(t, raw, &resp)
	for i, s := range resp.Data {
		resp.Data[i] = n.labels(s)
	}
	sort.Slice(resp.Data, func(i, j int) bool { return labelKey(resp.Data[i]) < labelKey(resp.Data[j]) })
	return resp
}

// generic decodes a response that has no run-specific values.
func (n normalizer) generic(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	mustUnmarshal(t, raw, &v)
	return v
}

// assertPinned compares got, once encoded as JSON, with the pinned file under
// testdata/. Both sides are decoded and re-encoded, so key order and
// formatting do not matter.
func assertPinned(t *testing.T, name string, got any) {
	t.Helper()
	gotJSON := canonicalJSON(t, got)
	path := filepath.Join("testdata", name)
	pinned, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pinned response %s: %v\nresponse:\n%s", path, err, gotJSON)
	}
	var pinnedValue any
	mustUnmarshal(t, pinned, &pinnedValue)
	if want := canonicalJSON(t, pinnedValue); want != gotJSON {
		t.Errorf("response differs from %s (edit the file by hand if the change is intended)\n got:\n%s\nwant:\n%s", path, gotJSON, want)
	}
}

func canonicalJSON(t *testing.T, v any) string {
	t.Helper()
	// Round-trip through any so struct field order and map key order are the
	// same on both sides.
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var generic any
	mustUnmarshal(t, raw, &generic)
	out, err := json.MarshalIndent(generic, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
