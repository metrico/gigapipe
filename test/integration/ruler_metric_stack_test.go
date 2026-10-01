//go:build integration

// Recording rules evaluate on their interval's grid and write back through the
// metric stack: a PromQL rate record and a LogQL record are readable by
// /api/v1/query under their record names at grid timestamps, and a recorded
// series whose source stops gets a stale marker at the next evaluation.
// The stack runs with the ruler enabled and a 1s rule poll interval.

package integration

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

// pollQuery polls sql until it returns a non-empty result or timeout passes.
func pollQuery(t *testing.T, sql string, timeout time.Duration) string {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(time.Second) {
		if got := clickhouseQuery(t, sql); got != "" {
			return got
		}
	}
	t.Fatalf("no rows within %s: %s", timeout, sql)
	return ""
}

func setRuleGroup(t *testing.T, path, yaml string) {
	t.Helper()
	if code, body := post(t, path, "application/yaml", yaml); code/100 != 2 {
		t.Fatalf("POST %s: status %d, body %s", path, code, body)
	}
	t.Cleanup(func() {
		group := yaml[strings.Index(yaml, "name: ")+6 : strings.Index(yaml, "\n")]
		req, _ := http.NewRequest(http.MethodDelete, baseURL()+path+"/"+group, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	})
}

// counter returns a counter rising by one every 5s over [from, to] (ms).
func counter(from, to int64) []*prompb.Sample {
	var res []*prompb.Sample
	for ts := from; ts <= to; ts += 5000 {
		res = append(res, &prompb.Sample{Timestamp: ts, Value: float64((ts - from) / 5000)})
	}
	return res
}

// recordedSamples is the SQL selecting (timestamp ms, value bits) rows of the
// recorded series named name whose label key equals val.
func recordedSamples(name, key, val, where string) string {
	return fmt.Sprintf("SELECT toUnixTimestamp64Milli(timestamp), reinterpretAsUInt64(value) FROM metric_samples "+
		"WHERE fingerprint IN (SELECT fingerprint FROM metric_series WHERE name = '%s' AND labels['%s'] = '%s') %s "+
		"ORDER BY timestamp LIMIT 1", name, key, val, where)
}

func firstRow(t *testing.T, row string) (int64, uint64) {
	t.Helper()
	f := strings.Fields(row)
	ts, err := strconv.ParseInt(f[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	bits, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return ts, bits
}

func TestRecordingRulesWriteBackThroughTheMetricStack(t *testing.T) {
	waitReady(t)
	id := time.Now().UnixNano()
	src := fmt.Sprintf("it_ruler_src_%d_total", id)
	rec := fmt.Sprintf("it_ruler_%d:rate20s", id)
	logRec := fmt.Sprintf("it_ruler_%d:lines", id)
	job := fmt.Sprintf("it_ruler_%d", id)

	// Instance a runs for ten minutes, b stops 20s from now.
	now := time.Now().UnixMilli() / 5000 * 5000
	remoteWrite(t,
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: src}, {Name: "instance", Value: "a"}},
			Samples: counter(now-120000, now+600000)},
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: src}, {Name: "instance", Value: "b"}},
			Samples: counter(now-120000, now+20000)})
	var lines [][]string
	for i := range 5 {
		lines = append(lines, []string{fmt.Sprint((now - 10000 + int64(i)*1000) * 1e6), fmt.Sprintf("line %d", i)})
	}
	pushStreams(t, []lokiPushStream{{Stream: map[string]string{"job": job}, Values: lines}})

	setRuleGroup(t, "/api/v1/rules/it", fmt.Sprintf(
		"name: prom_%d\ninterval: 15s\nrules:\n  - record: %s\n    expr: rate(%s[20s])\n    labels:\n      team: it\n",
		id, rec, src))
	setRuleGroup(t, "/loki/api/v1/rules/it", fmt.Sprintf(
		"name: loki_%d\ninterval: 15s\nrules:\n  - record: %s\n    expr: sum(count_over_time({job=\"%s\"}[5m]))\n",
		id, logRec, job))

	// The rate record: stamped on the 15s grid and readable under its name.
	ts, bits := firstRow(t, pollQuery(t, recordedSamples(rec, "instance", "a", ""), 90*time.Second))
	if ts%15000 != 0 {
		t.Errorf("recorded sample at %d, not on the 15s grid", ts)
	}
	if v := math.Float64frombits(bits); math.Abs(v-0.2) > 1e-9 {
		t.Errorf("stored rate = %v, want 0.2", v)
	}
	res := promGet(t, "/api/v1/query", map[string][]string{
		"query": {fmt.Sprintf(`%s{instance="a"}`, rec)}, "time": {fmt.Sprint(ts / 1000)}})
	if len(res.Data.Result) != 1 {
		t.Fatalf("query %s at %d: %d series, want 1", rec, ts, len(res.Data.Result))
	}
	if got := res.Data.Result[0].Metric; got["__name__"] != rec || got["team"] != "it" || got["instance"] != "a" {
		t.Errorf("recorded labels = %v", got)
	}
	if v, _ := strconv.ParseFloat(res.Data.Result[0].Value[1].(string), 64); math.Abs(v-0.2) > 1e-9 {
		t.Errorf("queried rate = %v, want 0.2", v)
	}

	// The LogQL record: one unlabelled series counting the five lines.
	lts, _ := firstRow(t, pollQuery(t, recordedSamples(logRec, "__name__", logRec, ""), 90*time.Second))
	if lts%15000 != 0 {
		t.Errorf("LogQL recorded sample at %d, not on the 15s grid", lts)
	}
	if got, ok := instantValue(t, logRec, lts); !ok || got != "5" {
		t.Errorf("query %s at %d = %q (found %v), want 5", logRec, lts, got, ok)
	}

	// Instance b's rate ends once its last two points leave the window: one
	// stale marker at that evaluation, none at the next, and the series reads
	// empty from the marker on.
	marker := pollQuery(t, recordedSamples(rec, "instance", "b",
		"AND reinterpretAsUInt64(value) = 0x7ff0000000000002"), 120*time.Second)
	mts, _ := firstRow(t, marker)
	if mts%15000 != 0 || mts <= now+20000 {
		t.Errorf("stale marker at %d, want a grid point after %d", mts, now+20000)
	}
	pollQuery(t, recordedSamples(rec, "instance", "a",
		fmt.Sprintf("AND timestamp > fromUnixTimestamp64Milli(%d)", mts)), 60*time.Second)
	if n := clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM metric_samples WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s' AND labels['instance'] = 'b') "+
		"AND reinterpretAsUInt64(value) = 0x7ff0000000000002", rec)); n != "1" {
		t.Errorf("stale markers for instance b = %s, want 1", n)
	}
	query := fmt.Sprintf(`%s{instance="b"}`, rec)
	if _, ok := instantValue(t, query, mts-15000); !ok {
		t.Errorf("query %s one tick before the marker is empty", query)
	}
	if got, ok := instantValue(t, query, mts); ok {
		t.Errorf("query %s at the marker = %s, want no result", query, got)
	}
	if _, ok := instantValue(t, fmt.Sprintf(`%s{instance="a"}`, rec), mts); !ok {
		t.Errorf("instance a is missing at %d", mts)
	}
}
