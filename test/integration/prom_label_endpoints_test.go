//go:build integration

// The Prometheus label, series, metadata and exemplar endpoints answer from the metric
// series index, checked through the HTTP API on a remote-written dataset: two families, a
// classic histogram with an exemplar, an old series and unmerged index and metadata rows.
// The LogQL label endpoints keep answering from the log tables.

package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

type labelEnvelope struct {
	Status   string          `json:"status"`
	Data     json.RawMessage `json:"data"`
	Warnings []string        `json:"warnings"`
}

func labelGet(t *testing.T, path string, params url.Values) (labelEnvelope, string) {
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
	var res labelEnvelope
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("%s: %v in %s", path, err, body)
	}
	return res, string(body)
}

func decode[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%v in %s", err, raw)
	}
	return v
}

func TestPromLabelEndpointsReadTheSeriesIndex(t *testing.T) {
	waitReady(t)
	sfx := time.Now().UnixNano()
	reqs := fmt.Sprintf("it_lbl_%d_requests_total", sfx)
	up := fmt.Sprintf("it_lbl_%d_up", sfx)
	lat := fmt.Sprintf("it_lbl_%d_latency_seconds", sfx)
	family := fmt.Sprintf(`{__name__=~"it_lbl_%d_.*"}`, sfx)
	t0 := time.Now().Add(-time.Hour).Truncate(time.Second).UnixMilli()

	lbls := func(name string, kv ...string) []*prompb.Label {
		res := []*prompb.Label{{Name: "__name__", Value: name}}
		for i := 0; i < len(kv); i += 2 {
			res = append(res, &prompb.Label{Name: kv[i], Value: kv[i+1]})
		}
		return res
	}
	sample := func(l []*prompb.Label, v float64) *prompb.TimeSeries {
		return &prompb.TimeSeries{Labels: l, Samples: []*prompb.Sample{{Timestamp: t0, Value: v}}}
	}
	bucket := lbls(lat+"_bucket", "job", "api", "le", "0.1")
	remoteWriteRequest(t, &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			sample(lbls(reqs, "job", "api", "status", "200"), 1),
			sample(lbls(reqs, "job", "api", "path", "/v1/x", "status", "500"), 1),
			sample(lbls(up, "job", "api"), 1),
			sample(bucket, 1),
			sample(lbls(lat+"_bucket", "job", "api", "le", "+Inf"), 2),
			sample(lbls(lat+"_sum", "job", "api"), 0.3),
			sample(lbls(lat+"_count", "job", "api"), 2),
			{Labels: bucket, Exemplars: []*prompb.Exemplar{{
				Labels: []*prompb.Label{{Name: "trace_id", Value: "abc"}}, Value: 0.05, Timestamp: t0 + 1500,
			}}},
		},
		Metadata: []*prompb.MetricMetadata{
			{Type: prompb.MetricMetadata_COUNTER, MetricFamilyName: reqs, Help: "Requests served."},
			{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: up, Help: "Target up."},
			{Type: prompb.MetricMetadata_HISTOGRAM, MetricFamilyName: lat, Help: "Latency."},
		},
	})
	eventually(t, fmt.Sprintf("SELECT uniqExact(fingerprint) FROM metric_series WHERE name LIKE 'it_lbl_%d_%%'", sfx), "7")
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_metadata FINAL WHERE name LIKE 'it_lbl_%d_%%'", sfx), "3")
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_exemplars WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s_bucket')", lat), "1")

	// An old series, a second unmerged index row of reqs{status="200"} and an earlier
	// metadata row of reqs, all kept unmerged while the endpoints read them.
	for _, table := range []string{"metric_series", "metric_metadata"} {
		clickhouseQuery(t, "SYSTEM STOP MERGES "+table)
		t.Cleanup(func() { clickhouseQuery(t, "SYSTEM START MERGES "+table) })
	}
	clickhouseQuery(t, fmt.Sprintf("INSERT INTO metric_series VALUES ('%[1]s', cityHash64('%[1]s-db'), "+
		"{'__name__':'%[1]s','job':'db'}, '2026-01-01 00:00:00.000', '2026-01-02 00:00:00.000')", up))
	clickhouseQuery(t, fmt.Sprintf("INSERT INTO metric_series SELECT name, fingerprint, labels, "+
		"first_seen - INTERVAL 1 DAY, last_seen FROM metric_series WHERE name = '%s' AND labels['status'] = '200'", reqs))
	clickhouseQuery(t, fmt.Sprintf("INSERT INTO metric_metadata VALUES ('%s', 'counter', 'Requests.', '', "+
		"now64(3) - INTERVAL 1 DAY)", reqs))
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT count() >= 2 FROM metric_series WHERE name = '%s' AND labels['status'] = '200'", reqs)); got != "1" {
		t.Fatalf("index rows of %s{status=\"200\"} = %s, want two unmerged rows", reqs, got)
	}
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM metric_metadata WHERE name = '%s'", reqs)); got != "2" {
		t.Fatalf("metadata rows of %s = %s, want two unmerged rows", reqs, got)
	}

	bounds := url.Values{
		"start": {strconv.FormatInt(t0/1000-600, 10)},
		"end":   {time.Now().UTC().Format(time.RFC3339)},
	}
	with := func(kv ...string) url.Values {
		v := url.Values{}
		for k, vs := range bounds {
			v[k] = vs
		}
		for i := 0; i < len(kv); i += 2 {
			v.Add(kv[i], kv[i+1])
		}
		return v
	}

	t.Run("names narrow under a selector", func(t *testing.T) {
		res, _ := labelGet(t, "/api/v1/labels", with("match[]", up))
		if got := decode[[]string](t, res.Data); !reflect.DeepEqual(got, []string{"__name__", "job", "service_name"}) {
			t.Fatalf("labels of %s = %q", up, got)
		}
		res, _ = labelGet(t, "/api/v1/labels", with("match[]", family))
		want := []string{"__name__", "job", "le", "path", "service_name", "status"}
		if got := decode[[]string](t, res.Data); !reflect.DeepEqual(got, want) {
			t.Fatalf("labels of the dataset = %q, want %q", got, want)
		}
	})

	t.Run("a missing label is excluded from values", func(t *testing.T) {
		res, _ := labelGet(t, "/api/v1/label/path/values", with("match[]", family))
		if got := decode[[]string](t, res.Data); !reflect.DeepEqual(got, []string{"/v1/x"}) {
			t.Fatalf("path values = %q", got)
		}
	})

	t.Run("__name__ values", func(t *testing.T) {
		res, _ := labelGet(t, "/api/v1/label/__name__/values", with("match[]", family))
		want := []string{lat + "_bucket", lat + "_count", lat + "_sum", reqs, up}
		if got := decode[[]string](t, res.Data); !reflect.DeepEqual(got, want) {
			t.Fatalf("__name__ values = %q, want %q", got, want)
		}
	})

	t.Run("ORed selectors with != and =~ and time bounds", func(t *testing.T) {
		sel1 := fmt.Sprintf(`%s{job="api",status!="500",path=~"/v1/.*"}`, reqs)
		res, _ := labelGet(t, "/api/v1/series", with("match[]", sel1, "match[]", up))
		want := []map[string]string{{"__name__": up, "job": "api", "service_name": "api"}}
		if got := decode[[]map[string]string](t, res.Data); !reflect.DeepEqual(got, want) {
			t.Fatalf("series = %v, want %v", got, want)
		}
		res, _ = labelGet(t, "/api/v1/series", url.Values{"match[]": {up}})
		if got := decode[[]map[string]string](t, res.Data); len(got) != 2 {
			t.Fatalf("unbounded series of %s = %v, want the live and the old one", up, got)
		}
	})

	t.Run("two unmerged rows give one label set", func(t *testing.T) {
		res, _ := labelGet(t, "/api/v1/series", url.Values{"match[]": {fmt.Sprintf(`%s{status="200"}`, reqs)}})
		want := []map[string]string{{"__name__": reqs, "job": "api", "service_name": "api", "status": "200"}}
		if got := decode[[]map[string]string](t, res.Data); !reflect.DeepEqual(got, want) {
			t.Fatalf("series = %v, want %v", got, want)
		}
	})

	t.Run("limit reads one more and warns", func(t *testing.T) {
		res, _ := labelGet(t, "/api/v1/series", url.Values{
			"match[]": {fmt.Sprintf(`%s{status!=""}`, reqs)}, "limit": {"1"},
		})
		if got := decode[[]map[string]string](t, res.Data); len(got) != 1 {
			t.Fatalf("series = %v, want one", got)
		}
		if !reflect.DeepEqual(res.Warnings, []string{"results truncated due to limit"}) {
			t.Fatalf("warnings = %q", res.Warnings)
		}
		res, _ = labelGet(t, "/api/v1/series", url.Values{
			"match[]": {fmt.Sprintf(`%s{status!=""}`, reqs)}, "limit": {"2"},
		})
		if got := decode[[]map[string]string](t, res.Data); len(got) != 2 || res.Warnings != nil {
			t.Fatalf("series = %v, warnings = %q", got, res.Warnings)
		}
	})

	t.Run("metadata per family from FINAL", func(t *testing.T) {
		type entry struct{ Type, Help, Unit string }
		res, _ := labelGet(t, "/api/v1/metadata", url.Values{"metric": {reqs}})
		want := map[string][]entry{reqs: {{Type: "counter", Help: "Requests served."}}}
		if got := decode[map[string][]entry](t, res.Data); !reflect.DeepEqual(got, want) {
			t.Fatalf("metadata = %v, want %v", got, want)
		}
		res, _ = labelGet(t, "/api/v1/metadata", url.Values{"metric": {lat}, "limit_per_metric": {"5"}})
		want = map[string][]entry{lat: {{Type: "histogram", Help: "Latency."}}}
		if got := decode[map[string][]entry](t, res.Data); !reflect.DeepEqual(got, want) {
			t.Fatalf("metadata = %v, want %v", got, want)
		}
		res, _ = labelGet(t, "/api/v1/metadata", url.Values{"limit": {"1"}})
		if got := decode[map[string][]entry](t, res.Data); len(got) != 1 {
			t.Fatalf("metadata with limit 1 = %v", got)
		}
	})

	t.Run("exemplars", func(t *testing.T) {
		query := fmt.Sprintf(`rate(%s_bucket{job="api"}[5m])`, lat)
		res, body := labelGet(t, "/api/v1/query_exemplars", with("query", query))
		type exemplar struct {
			Labels    map[string]string `json:"labels"`
			Value     string            `json:"value"`
			Timestamp float64           `json:"timestamp"`
		}
		type exemplarSeries struct {
			SeriesLabels map[string]string `json:"seriesLabels"`
			Exemplars    []exemplar        `json:"exemplars"`
		}
		want := []exemplarSeries{{
			SeriesLabels: map[string]string{"__name__": lat + "_bucket", "job": "api", "le": "0.1", "service_name": "api"},
			Exemplars: []exemplar{{
				Labels: map[string]string{"trace_id": "abc"}, Value: "0.05", Timestamp: float64(t0+1500) / 1000,
			}},
		}}
		if got := decode[[]exemplarSeries](t, res.Data); !reflect.DeepEqual(got, want) {
			t.Fatalf("exemplars = %+v, want %+v", got, want)
		}
		if ts := fmt.Sprintf(`"timestamp":%d.500`, (t0+1500)/1000); !strings.Contains(body, ts) {
			t.Fatalf("body %s lacks %s", body, ts)
		}
		res, _ = labelGet(t, "/api/v1/query_exemplars", url.Values{
			"query": {query}, "start": {strconv.FormatInt(t0/1000+2, 10)},
		})
		if string(res.Data) != "[]" {
			t.Fatalf("exemplars after the only one = %s", res.Data)
		}
	})
}

func TestLogQLLabelEndpointsReadTheLogTables(t *testing.T) {
	waitReady(t)
	sfx := strconv.FormatInt(time.Now().UnixNano(), 10)
	now := time.Now()
	pushStreams(t, []lokiPushStream{{
		Stream: map[string]string{"it_lbl_job": sfx, "level": "info"},
		Values: [][]string{{strconv.FormatInt(now.UnixNano(), 10), "hello"}},
	}})
	sel := fmt.Sprintf(`{it_lbl_job="%s"}`, sfx)
	params := url.Values{
		"start": {strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.Add(time.Minute).UnixNano(), 10)},
	}
	var series []map[string]string
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		p := url.Values{"match[]": {sel}}
		for k, v := range params {
			p[k] = v
		}
		res, _ := labelGet(t, "/loki/api/v1/series", p)
		if series = decode[[]map[string]string](t, res.Data); len(series) > 0 {
			break
		}
	}
	if want := []map[string]string{{"it_lbl_job": sfx, "level": "info", "service_name": "unknown"}}; !reflect.DeepEqual(series, want) {
		t.Fatalf("series = %v, want %v", series, want)
	}
	res, _ := labelGet(t, "/loki/api/v1/labels", params)
	if names := decode[[]string](t, res.Data); !contains(names, "it_lbl_job") {
		t.Fatalf("labels = %q lack it_lbl_job", names)
	}
	res, _ = labelGet(t, "/loki/api/v1/label/it_lbl_job/values", params)
	if values := decode[[]string](t, res.Data); !contains(values, sfx) {
		t.Fatalf("values = %q lack %s", values, sfx)
	}
}

func contains(vals []string, v string) bool {
	for _, x := range vals {
		if x == v {
			return true
		}
	}
	return false
}
