//go:build integration

// Pushed-down PromQL returns Prometheus's values. The probe's oracle figures come from the
// read-path probe; every other pushed-down function is checked against the engine over raw
// samples, which serves the same expression with `offset 1m` evaluated one minute later.

package integration

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"google.golang.org/protobuf/proto"
)

// gaugeSamples is a gauge every 20s with negative values, rises and falls, ending at 06:40.
// It is NaN at 00:20 and 03:00, +Inf at 04:00 and -Inf at 05:20.
func gaugeSamples(t0 int64) []*prompb.Sample {
	var res []*prompb.Sample
	for n := int64(1); n <= 20; n++ {
		v := float64(n*7%23) - 9.5
		switch n {
		case 1, 9:
			v = math.NaN()
		case 12:
			v = math.Inf(1)
		case 16:
			v = math.Inf(-1)
		}
		res = append(res, &prompb.Sample{Timestamp: t0 + n*20000, Value: v})
	}
	return res
}

// writeProbe remote-writes the probe counter {job="probe"} and a gauge {job="probe",
// instance="b"} under a fresh name, and waits until both are readable. Its t0 lies off the
// 5m grid, so no query on a one-minute grid is an aligned read.
func writeProbe(t *testing.T) (string, int64) {
	t.Helper()
	return writeProbeAt(t, offFiveMinuteGrid())
}

// offFiveMinuteGrid is a t0 about two hours back, one minute off the 5m grid.
func offFiveMinuteGrid() int64 {
	return time.Now().Add(-2 * time.Hour).Truncate(5 * time.Minute).Add(time.Minute).UnixMilli()
}

func writeProbeAt(t *testing.T, t0 int64) (string, int64) {
	t.Helper()
	waitReady(t)
	name := fmt.Sprintf("it_pushdown_%d", time.Now().UnixNano())
	remoteWrite(t,
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "probe"}},
			Samples: probeSamples(t0)},
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "instance", Value: "b"}, {Name: "job", Value: "probe"}},
			Samples: gaugeSamples(t0)})
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_samples WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), "54")
	return name, t0
}

func remoteWrite(t *testing.T, series ...*prompb.TimeSeries) {
	t.Helper()
	remoteWriteRequest(t, &prompb.WriteRequest{Timeseries: series})
}

func remoteWriteRequest(t *testing.T, req *prompb.WriteRequest) {
	t.Helper()
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
}

// promFailure returns the body of a query that fails, failing the test if it succeeds.
func promFailure(t *testing.T, path string, params url.Values) string {
	t.Helper()
	resp, err := http.Get(baseURL() + path + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		t.Errorf("%s %v succeeded: %s", path, params, body)
	}
	return string(body)
}

// series is a query result keyed by label set: timestamp (ms) to value.
type series map[string]map[int64]float64

func resultSeries(t *testing.T, res promResponse, shiftMs int64) series {
	t.Helper()
	out := series{}
	for _, r := range res.Data.Result {
		keys := make([]string, 0, len(r.Metric))
		for k, v := range r.Metric {
			keys = append(keys, k+"="+v)
		}
		sort.Strings(keys)
		pts := map[int64]float64{}
		points := r.Values
		if len(points) == 0 {
			points = [][2]any{r.Value}
		}
		for _, p := range points {
			f, err := strconv.ParseFloat(p[1].(string), 64)
			if err != nil {
				t.Fatal(err)
			}
			pts[int64(math.Round(p[0].(float64)*1000))-shiftMs] = f
		}
		out[strings.Join(keys, ",")] = pts
	}
	return out
}

func rangeQuery(t *testing.T, query string, start, end, shiftMs int64) series {
	t.Helper()
	return rangeFrom(t, baseURL(), query, start, end, 60000, shiftMs)
}

// rangeFrom runs a range query at the gigapipe at base, its grid shifted by shiftMs and its
// result shifted back.
func rangeFrom(t *testing.T, base, query string, start, end, stepMs, shiftMs int64) series {
	t.Helper()
	return resultSeries(t, promGetFrom(t, base, "/api/v1/query_range", url.Values{"query": {query},
		"start": {fmt.Sprint((start + shiftMs) / 1000)}, "end": {fmt.Sprint((end + shiftMs) / 1000)},
		"step": {fmt.Sprint(stepMs / 1000)}}), shiftMs)
}

func instantQuery(t *testing.T, query string, at, shiftMs int64) series {
	t.Helper()
	return instantFrom(t, baseURL(), query, at, shiftMs)
}

func instantFrom(t *testing.T, base, query string, at, shiftMs int64) series {
	t.Helper()
	return resultSeries(t, promGetFrom(t, base, "/api/v1/query", url.Values{"query": {query},
		"time": {fmt.Sprint((at + shiftMs) / 1000)}}), shiftMs)
}

// sameSeries reports where got and want differ: label sets, timestamps, or values beyond
// float64 rounding.
func sameSeries(got, want series) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d series, want %d", len(got), len(want))
	}
	for lbls, wp := range want {
		gp, ok := got[lbls]
		if !ok {
			return "no series " + lbls
		}
		if len(gp) != len(wp) {
			return fmt.Sprintf("%s: %d points %v, want %d %v", lbls, len(gp), gp, len(wp), wp)
		}
		for ts, w := range wp {
			g, ok := gp[ts]
			if !ok || math.IsNaN(g) != math.IsNaN(w) || math.IsInf(w, 0) && g != w ||
				!math.IsInf(w, 0) && math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				return fmt.Sprintf("%s at %d: %v, want %v", lbls, ts, g, w)
			}
		}
	}
	return ""
}

// pushedDown reports whether ClickHouse ran a pushdown carrying marker, and no raw read.
func pushedDown(t *testing.T, marker string) bool {
	t.Helper()
	clickhouseQuery(t, "SYSTEM FLUSH LOGS")
	count := func(cond string) string {
		return clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "+
			"AND query LIKE '%%%s%%' AND query NOT LIKE '%%system.query_log%%' AND %s", marker, cond))
	}
	return count("query LIKE '%ARRAY JOIN range(k_min%'") != "0" &&
		count("query LIKE '%FROM metric_samples FINAL WHERE%' AND query NOT LIKE '%ARRAY JOIN%'") == "0"
}

func TestPromQLPushdownReproducesTheProbe(t *testing.T) {
	name, t0 := writeProbe(t)
	end := t0 + 600000
	// Each query carries its own no-op matcher, so its SQL can be found in the query log.
	tag := func(tc string, ts int64) string { return fmt.Sprintf("tag_%s_%d", tc, ts) }
	sel := func(tag string) string { return fmt.Sprintf(`%s{instance="", job!="%s"}`, name, tag) }
	at := func(s series, ts int64) string {
		if len(s) != 1 {
			return fmt.Sprintf("%d series", len(s))
		}
		for _, pts := range s {
			if v, ok := pts[ts]; ok {
				return fmt.Sprintf("%.7g", v)
			}
		}
		return "absent"
	}
	for _, tc := range []struct {
		tag, query string
		instant    bool
		ts         int64
		want       string
	}{
		{"r5", "rate(%s[5m])", false, t0 + 300000, "0.06666667"},
		{"r5", "rate(%s[5m])", false, end, "0.04561404"},
		{"r10", "rate(%s[10m])", false, end, "0.05641026"},
		{"r10i", "rate(%s[10m])", true, end, "0.05641026"},
		{"inc10", "increase(%s[10m])", false, end, "33.84615"},
		{"inc10i", "increase(%s[10m])", true, end, "33.84615"},
		{"resets", "resets(%s[10m])", true, end, "2"},
		{"changes", "changes(%s[10m])", true, end, "32"},
		{"count", "count_over_time(%s[10m])", true, end, "33"},
		{"sum", "sum_over_time(%s[10m])", true, end, "273"},
		{"min", "min_over_time(%s[10m])", true, end, "1"},
		{"max", "max_over_time(%s[10m])", true, end, "19"},
		{"x5", "%s", false, t0 + 300000, "17"},
		{"x7", "%s", false, t0 + 420000, "absent"},
		{"x10", "%s", false, end, "12"},
		{"x5i", "%s", true, t0 + 300000, "17"},
		{"x7i", "%s", true, t0 + 420000, "0 series"},
	} {
		query := fmt.Sprintf(tc.query, sel(tag(tc.tag, tc.ts)))
		var got series
		if tc.instant {
			got = instantQuery(t, query, tc.ts, 0)
		} else {
			got = rangeQuery(t, query, t0, end, 0)
		}
		if v := at(got, tc.ts); v != tc.want {
			t.Errorf("%s at +%ds = %s, want %s", tc.query, (tc.ts-t0)/1000, v, tc.want)
		}
		if !pushedDown(t, tag(tc.tag, tc.ts)) {
			t.Errorf("%s at +%ds did not run as a pushdown", tc.query, (tc.ts-t0)/1000)
		}
	}
}

func TestPromQLPushdownEqualsTheEngineOverRawSamples(t *testing.T) {
	name, t0 := writeProbe(t)
	start, end := t0-120000, t0+900000
	var queries []string
	for _, fn := range []string{"rate", "increase", "delta", "irate", "idelta", "resets", "changes",
		"count_over_time", "sum_over_time", "min_over_time", "max_over_time", "avg_over_time",
		"stddev_over_time", "stdvar_over_time", "present_over_time", "last_over_time"} {
		for _, r := range []string{"1m", "5m", "10m"} {
			queries = append(queries, fmt.Sprintf("%s(%s[%s]%%s)", fn, name, r))
		}
	}
	queries = append(queries, name+"%s")
	for _, q := range append([]string(nil), queries...) {
		for _, agg := range []string{"sum by (job)", "max without (instance)", "min", "count", "avg by (instance)",
			"sum by (__name__, job)"} {
			queries = append(queries, agg+" ("+q+")")
		}
	}
	for _, q := range queries {
		pushed, raw := fmt.Sprintf(q, ""), fmt.Sprintf(q, " offset 1m")
		got := rangeQuery(t, pushed, start, end, 0)
		if len(got) == 0 {
			t.Errorf("query_range %s: no series", pushed)
		}
		if diff := sameSeries(got, rangeQuery(t, raw, start, end, 60000)); diff != "" {
			t.Errorf("query_range %s: %s", pushed, diff)
		}
		for _, at := range []int64{t0 + 300000, t0 + 420000, t0 + 600000} {
			if diff := sameSeries(instantQuery(t, pushed, at, 0), instantQuery(t, raw, at, 60000)); diff != "" {
				t.Errorf("query %s at +%ds: %s", pushed, (at-t0)/1000, diff)
			}
		}
	}
}

// counterSamples is a counter rising by one every 15s over (from, to], relative to t0 in ms.
func counterSamples(t0, from, to int64) []*prompb.Sample {
	var res []*prompb.Sample
	for ts := from + 15000; ts <= to; ts += 15000 {
		res = append(res, &prompb.Sample{Timestamp: t0 + ts, Value: float64(ts / 15000)})
	}
	return res
}

func TestPromQLPushdownOnSeriesSharingALabelSetOnceNamesAreDropped(t *testing.T) {
	waitReady(t)
	base := fmt.Sprintf("it_dup_%d", time.Now().UnixNano())
	t0 := offFiveMinuteGrid()
	series := func(name, job string, from, to int64) *prompb.TimeSeries {
		return &prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: base + name}, {Name: "job", Value: job}},
			Samples: counterSamples(t0, from, to)}
	}
	// a and b overlap; c ends at 02:00 and d starts at 06:00.
	remoteWrite(t, series("_a", "overlap", 0, 600000), series("_b", "overlap", 0, 600000),
		series("_c", "disjoint", 0, 120000), series("_d", "disjoint", 360000, 600000))
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_samples WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name LIKE '%s%%')", base), "104")
	start, end := t0, t0+600000

	// The engine rejects a range function's result holding one label set twice, at any timestamps.
	const sameLabelset = "vector cannot contain metrics with the same labelset"
	for _, pair := range []string{"_a|%s_b", "_c|%s_d"} {
		for _, q := range []string{`rate({__name__=~"%s` + pair + `"}[1m]%s)`, `sum(rate({__name__=~"%s` + pair + `"}[1m]%s))`} {
			for _, offset := range []string{"", " offset 1m"} {
				query := fmt.Sprintf(q, base, base, offset)
				if body := promFailure(t, "/api/v1/query_range", url.Values{"query": {query},
					"start": {fmt.Sprint(start / 1000)}, "end": {fmt.Sprint(end / 1000)}, "step": {"60"}}); !strings.Contains(body, sameLabelset) {
					t.Errorf("query_range %s: %s, want %q", query, body, sameLabelset)
				}
			}
		}
	}
	for _, q := range []string{`rate({__name__=~"%s_a|%s_b"}[1m]%s)`, `sum(rate({__name__=~"%s_a|%s_b"}[1m]%s))`} {
		for _, offset := range []string{"", " offset 1m"} {
			query := fmt.Sprintf(q, base, base, offset)
			if body := promFailure(t, "/api/v1/query", url.Values{"query": {query},
				"time": {fmt.Sprint((t0 + 300000) / 1000)}}); !strings.Contains(body, sameLabelset) {
				t.Errorf("query %s: %s, want %q", query, body, sameLabelset)
			}
		}
	}

	// At 07:00 only d has a rate.
	for _, q := range []string{`rate({__name__=~"%s_c|%s_d"}[1m]%s)`, `sum(rate({__name__=~"%s_c|%s_d"}[1m]%s))`} {
		pushed, raw := fmt.Sprintf(q, base, base, ""), fmt.Sprintf(q, base, base, " offset 1m")
		got := instantQuery(t, pushed, t0+420000, 0)
		if len(got) != 1 {
			t.Errorf("query %s: %d series, want 1", pushed, len(got))
		}
		if diff := sameSeries(got, instantQuery(t, raw, t0+420000, 60000)); diff != "" {
			t.Errorf("query %s: %s", pushed, diff)
		}
	}
}

// subBucketProbe is two hours of a counter and a gauge, 15s apart from t0. The counter resets
// at every 37th sample, falling at every offset to a 1m or 2m edge, and at 01:00:15, just past
// a 5m edge; both series carry a stale marker 5s after every 53rd sample, and end with one at
// 02:00:20, alone in its 1m sub-bucket.
func subBucketProbe(t0 int64) (counter, gauge []*prompb.Sample) {
	stale := math.Float64frombits(0x7ff0000000000002)
	c := 0.0
	for n := int64(1); n <= 480; n++ {
		ts := t0 + n*15000
		c += float64(1 + n%3)
		if n%37 == 0 || n == 241 {
			c = float64(n % 5)
		}
		counter = append(counter, &prompb.Sample{Timestamp: ts, Value: c})
		gauge = append(gauge, &prompb.Sample{Timestamp: ts, Value: float64(n*13%29) - 14})
		if n%53 == 0 {
			counter = append(counter, &prompb.Sample{Timestamp: ts + 5000, Value: stale})
			gauge = append(gauge, &prompb.Sample{Timestamp: ts + 5000, Value: stale})
		}
	}
	end := &prompb.Sample{Timestamp: t0 + 7220000, Value: stale}
	return append(counter, end), append(gauge, end)
}

// subBucketed reports whether a raw read at stepMs over rangeMs groups its samples into
// sub-buckets: gcd(step, range) of a minute or more and a range of 30 steps or more.
func subBucketed(stepMs, rangeMs int64) bool {
	a, b := stepMs, rangeMs
	for b != 0 {
		a, b = b, a%b
	}
	return rangeMs >= 30*stepMs && a >= 60000
}

func TestPromQLPushdownOverSubBucketsEqualsTheEngine(t *testing.T) {
	waitReady(t)
	name := fmt.Sprintf("it_subbucket_%d", time.Now().UnixNano())
	t0 := time.Now().Add(-3 * time.Hour).Truncate(5 * time.Minute).Add(time.Minute).UnixMilli()
	counter, gauge := subBucketProbe(t0)
	remoteWrite(t,
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "probe"}}, Samples: counter},
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "instance", Value: "b"}, {Name: "job", Value: "probe"}}, Samples: gauge})
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_samples WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), fmt.Sprint(len(counter)+len(gauge)))
	start, end := t0-300000, t0+7800000

	// Each query carries its own no-op matcher, so its SQL can be found in the query log.
	type query struct {
		expr       string
		stepMs     int64
		subBuckets bool
		tag        string
	}
	var queries []query
	add := func(expr string, stepMs, rangeMs int64) {
		tag := fmt.Sprintf("sb_q%d", len(queries))
		queries = append(queries, query{fmt.Sprintf(expr, fmt.Sprintf(`%s{job!="%s"}`, name, tag)), stepMs,
			subBucketed(stepMs, rangeMs), tag})
	}
	for _, r := range []struct {
		rng             string
		rangeMs, stepMs int64
	}{
		{"1h", 3600000, 120000}, {"75m", 4500000, 120000}, {"100m", 6000000, 180000}, {"1h", 3600000, 60000},
		{"150m", 9000000, 300000}, {"5m", 300000, 120000}, {"10m", 600000, 180000}, {"1h", 3600000, 300000},
		{"1h", 3600000, 30000}, {"2m", 120000, 120000}, {"5m", 300000, 15000},
	} {
		for _, fn := range []string{"rate", "increase", "delta", "irate", "idelta", "resets", "changes",
			"count_over_time", "sum_over_time", "min_over_time", "max_over_time", "avg_over_time",
			"stddev_over_time", "stdvar_over_time", "present_over_time", "last_over_time"} {
			q := fmt.Sprintf("%s(%%s[%s]%%%%s)", fn, r.rng)
			add(q, r.stepMs, r.rangeMs)
			add("sum by (job) ("+q+")", r.stepMs, r.rangeMs)
		}
	}
	// The instant selector looks back 5m.
	for _, stepMs := range []int64{120000, 180000, 300000} {
		add("%s%%s", stepMs, 300000)
		add("max by (job) (%s%%s)", stepMs, 300000)
	}
	for _, q := range queries {
		pushed, raw := fmt.Sprintf(q.expr, ""), fmt.Sprintf(q.expr, " offset 1m")
		got := rangeFrom(t, baseURL(), pushed, start, end, q.stepMs, 0)
		if len(got) == 0 {
			t.Errorf("query_range %s at %ds: no series", pushed, q.stepMs/1000)
		}
		if diff := sameSeries(got, rangeFrom(t, baseURL(), raw, start, end, q.stepMs, 60000)); diff != "" {
			t.Errorf("query_range %s at %ds: %s", pushed, q.stepMs/1000, diff)
		}
	}

	// The final marker ends the selector at 02:01, inside its lookback; range functions skip it.
	sel := fmt.Sprintf(`%s{job="probe"}`, name)
	selector := rangeFrom(t, baseURL(), sel, start, end, 120000, 0)
	lastOver := rangeFrom(t, baseURL(), "last_over_time("+sel+"[5m])", start, end, 120000, 0)
	for lbls, pts := range selector {
		if _, ok := pts[t0+7140000]; !ok {
			t.Errorf("%s{%s} absent at 01:59", sel, lbls)
		}
		if v, ok := pts[t0+7260000]; ok {
			t.Errorf("%s{%s} at 02:01 = %v, want the series ended by its stale marker", sel, lbls, v)
		}
	}
	for lbls, pts := range lastOver {
		if _, ok := pts[t0+7260000]; !ok {
			t.Errorf("last_over_time{%s} absent at 02:01, want the last sample before the marker", lbls)
		}
	}
	if len(selector) != 2 || len(lastOver) != 2 {
		t.Errorf("%d selector and %d last_over_time series, want 2 each", len(selector), len(lastOver))
	}

	clickhouseQuery(t, "SYSTEM FLUSH LOGS")
	for _, q := range queries {
		logged := func(cond string) string {
			return clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "+
				"AND query LIKE '%%''%s''%%' AND query LIKE '%%ARRAY JOIN range(k_min%%' AND %s "+
				"AND query NOT LIKE '%%system.query_log%%'", q.tag, cond))
		}
		bucketed, sampled := logged("query LIKE '%GROUP BY fingerprint, j%'"), logged("query NOT LIKE '%GROUP BY fingerprint, j%'")
		want := [2]string{"0", "1"}
		if q.subBuckets {
			want = [2]string{"1", "0"}
		}
		if [2]string{bucketed, sampled} != want {
			t.Errorf("%s at %ds: %s pushdowns through sub-buckets and %s per sample, want %s and %s",
				q.expr, q.stepMs/1000, bucketed, sampled, want[0], want[1])
		}
	}
}
