//go:build integration

// OTLP, Datadog and Influx metrics land in the metric stack and nowhere else,
// an Influx line with a message field stays a log, and a Loki JSON push that
// carries a value is rejected with nothing written.

package integration

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func post(t *testing.T, path, contentType, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(baseURL()+path, contentType, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func assertNotInLogTables(t *testing.T, name string) {
	t.Helper()
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM time_series WHERE labels LIKE '%%%s%%'", name)); got != "0" {
		t.Fatalf("time_series rows for %s: %s", name, got)
	}
}

func TestOTLPMetricsLandInTheMetricStack(t *testing.T) {
	waitReady(t)
	name := fmt.Sprintf("it_otlp_%d", time.Now().UnixNano())
	tsNs := time.Now().Add(-time.Minute).UnixNano()/int64(time.Millisecond)*int64(time.Millisecond) + 123456
	body := fmt.Sprintf(`{"resourceMetrics":[{"resource":{"attributes":[
		{"key":"service.name","value":{"stringValue":"it"}}]},
		"scopeMetrics":[{"metrics":[{"name":"%[1]s","histogram":{"aggregationTemporality":2,"dataPoints":[{
			"timeUnixNano":"%[2]d","count":"3","sum":1.5,"explicitBounds":[0.1,1],"bucketCounts":["1","1","1"],
			"exemplars":[{"timeUnixNano":"%[2]d","asDouble":0.5,
				"traceId":"MDEyMzQ1Njc4OWFiY2RlZg==","spanId":"MDEyMzQ1Njc="}]}]}}]}]}]}`, name, tsNs)
	if code, resp := post(t, "/v1/metrics", "application/json", body); code != http.StatusOK {
		t.Fatalf("OTLP status = %d: %s", code, resp)
	}
	bucket := name + "_bucket"
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_series WHERE name = '%s'", bucket), "3")
	eventually(t, fmt.Sprintf("SELECT toUnixTimestamp64Milli(timestamp), value FROM metric_samples FINAL "+
		"WHERE fingerprint IN (SELECT fingerprint FROM metric_series WHERE name = '%s_count')", name),
		fmt.Sprintf("%d\t3", tsNs/int64(time.Millisecond)))
	eventually(t, fmt.Sprintf("SELECT s.labels['le'], e.trace_id, e.value FROM metric_exemplars AS e FINAL "+
		"JOIN (SELECT fingerprint, any(labels) AS labels FROM metric_series WHERE name = '%s' GROUP BY fingerprint) AS s "+
		"ON e.fingerprint = s.fingerprint", bucket),
		"1\t30313233343536373839616263646566\t0.5")
	assertNotInLogTables(t, name)
}

func TestDatadogMetricsLandInTheMetricStack(t *testing.T) {
	waitReady(t)
	name := fmt.Sprintf("it_datadog_%d", time.Now().UnixNano())
	sec := time.Now().Add(-time.Minute).Unix()
	body := fmt.Sprintf(`{"series":[{"metric":"%s","points":[{"timestamp":%d,"value":7}]}]}`, name, sec)
	if code, resp := post(t, "/api/v2/series", "application/json", body); code != http.StatusAccepted {
		t.Fatalf("Datadog status = %d: %s", code, resp)
	}
	eventually(t, fmt.Sprintf("SELECT toUnixTimestamp64Milli(timestamp), value FROM metric_samples FINAL "+
		"WHERE fingerprint IN (SELECT fingerprint FROM metric_series WHERE name = '%s')", name),
		fmt.Sprintf("%d\t7", sec*1000))
	assertNotInLogTables(t, name)
}

func TestInfluxSplitsMetricsAndLogs(t *testing.T) {
	waitReady(t)
	measurement := fmt.Sprintf("it_influx_%d", time.Now().UnixNano())
	ns := time.Now().Add(-time.Minute).UnixNano()
	body := fmt.Sprintf("%[1]s,host=a idle=99.5,count=3i %[2]d\n%[1]s_log,host=a message=\"hello\" %[2]d\n",
		measurement, ns)
	if code, resp := post(t, "/influx/api/v2/write", "text/plain", body); code != http.StatusNoContent {
		t.Fatalf("Influx status = %d: %s", code, resp)
	}
	eventually(t, fmt.Sprintf("SELECT name, value FROM metric_samples AS s FINAL "+
		"JOIN (SELECT fingerprint, any(name) AS name FROM metric_series WHERE labels['measurement'] = '%s' "+
		"GROUP BY fingerprint) AS m ON s.fingerprint = m.fingerprint ORDER BY name", measurement),
		"count\t3\nidle\t99.5")
	eventually(t, fmt.Sprintf("SELECT s.string, s.type FROM samples_v3 AS s "+
		"WHERE s.fingerprint IN (SELECT fingerprint FROM time_series WHERE labels LIKE '%%%s_log%%')", measurement),
		"hello\t1")
	assertNotInLogTables(t, measurement+`"`)
}

func TestLokiJSONPushWithValueIsRejected(t *testing.T) {
	waitReady(t)
	job := fmt.Sprintf("it_loki_value_%d", time.Now().UnixNano())
	ns := time.Now().UnixNano()
	body := fmt.Sprintf(`{"streams":[{"stream":{"job":"%[1]s"},"values":[["%[2]d","kept?"]]},
		{"stream":{"job":"%[1]s_v"},"entries":[{"ts":"%[2]d","value":1}]}]}`, job, ns)
	code, resp := post(t, "/loki/api/v1/push", "application/json", body)
	if code != http.StatusBadRequest || !strings.Contains(resp, "entry 0") {
		t.Fatalf("Loki status = %d: %s", code, resp)
	}
	time.Sleep(3 * time.Second)
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM time_series WHERE labels LIKE '%%%s%%'", job)); got != "0" {
		t.Fatalf("time_series rows after a rejected push: %s", got)
	}
}
