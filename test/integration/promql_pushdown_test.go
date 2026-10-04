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
