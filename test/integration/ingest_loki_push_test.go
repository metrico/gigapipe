//go:build integration

// Ingestion conformance for the Loki push API (M8, #924). What a Loki client
// sends is checked where gigapipe stores it, in ClickHouse, rather than through
// the query API as the M2 round trips do: which tables get rows, and the
// fingerprint, labels, timestamps, line text and sample type in them.
//
// The expected values are what gigapipe stores today, measured against a real
// stack. If a case fails after an intended change (for example the samples
// table split in #972), review the difference and edit the case by hand.
//
// ClickHouse is read with clickhouseQuery (process_test.go), over its HTTP
// interface at CLICKHOUSE_HTTP_URL (default http://localhost:8123), in the
// database CLICKHOUSE_DB (default cloki).

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
)

// Where gigapipe stores logs today. The samples table split (#972) changes
// these; this is the one place that names them.
const (
	seriesTable     = "time_series"     // one row per stream, day and sample type
	samplesTable    = "samples_v3"      // one row per log line
	labelIndexTable = "time_series_gin" // one row per label, filled by a materialized view
	logsRollupTable = "metrics_15s"     // 15-second rollup, filled by a materialized view
)

// sampleTypeLog is the type column value of a log line (writer/model).
const sampleTypeLog = 1

// pinnedFingerprint is the fingerprint gigapipe gives the stream
// {app="checkout", case="pinned-fingerprint", pod="p1"}, measured with the
// default fingerprint function (CityHash). It covers the whole path: label
// cleanup, the added service_name label, and the hash. Measured on 2026-11-20.
const pinnedFingerprint = "18270485951633840138"

func TestLokiPushConformance(t *testing.T) {
	run := fmt.Sprintf("ingest-%d", time.Now().UnixNano())
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)

	// The M2 seed: 4 streams, 17 lines.
	t.Run("seed as JSON", func(t *testing.T) {
		streams := seed.Default(run+"-json", start).Logs()
		postBody(t, "/loki/api/v1/push", "application/json", seed.LokiPushJSON(streams))
		assertSeedStored(t, run+"-json", streams)
	})

	// The same streams the way Promtail and Alloy send them. The run label
	// differs, so these are new streams with their own rows.
	t.Run("seed as protobuf", func(t *testing.T) {
		streams := seed.Default(run+"-proto", start).Logs()
		postBody(t, "/loki/api/v1/push", "application/x-protobuf", seed.LokiPushProto(streams))
		assertSeedStored(t, run+"-proto", streams)
	})

	t.Run("fingerprint is pinned and the same for JSON and protobuf", func(t *testing.T) {
		labels := map[string]string{"app": "checkout", "case": "pinned-fingerprint", "pod": "p1"}
		// Nanosecond times only this run uses, so earlier runs' rows of this
		// fixed stream don't count.
		at := time.Now().Add(-5 * time.Minute)
		jsonLine := seed.LogLine{Time: at, Text: "sent as JSON"}
		protoLine := seed.LogLine{Time: at.Add(time.Second), Text: "sent as protobuf"}
		postBody(t, "/loki/api/v1/push", "application/json",
			seed.LokiPushJSON([]seed.LogStream{{Labels: labels, Lines: []seed.LogLine{jsonLine}}}))
		postBody(t, "/loki/api/v1/push", "application/x-protobuf",
			seed.LokiPushProto([]seed.LogStream{{Labels: labels, Lines: []seed.LogLine{protoLine}}}))

		got := waitForSamples(t, fmt.Sprintf("fingerprint = %s AND timestamp_ns IN (%d, %d)",
			pinnedFingerprint, jsonLine.Time.UnixNano(), protoLine.Time.UnixNano()), 2)
		want := []sampleRow{
			{pinnedFingerprint, jsonLine.Time.UnixNano(), 0, jsonLine.Text, sampleTypeLog},
			{pinnedFingerprint, protoLine.Time.UnixNano(), 0, protoLine.Text, sampleTypeLog},
		}
		if !slices.Equal(got, want) {
			t.Errorf("samples:\n got %v\nwant %v", got, want)
		}
		// Only the log-type row: this fixed stream's rows from earlier runs stay
		// in the table, and the samples above already check this run's type.
		wantLabels := map[string]string{"app": "checkout", "case": "pinned-fingerprint", "pod": "p1", "service_name": "checkout"}
		for _, s := range waitForSeries(t, fmt.Sprintf("fingerprint = %s AND type = %d", pinnedFingerprint, sampleTypeLog), 1) {
			if !maps.Equal(s.labelMap(t), wantLabels) {
				t.Errorf("series row %+v, want labels %v", s, wantLabels)
			}
		}
	})

	t.Run("label order does not change the fingerprint", func(t *testing.T) {
		ts := start.UnixNano()
		pushRaw(t, fmt.Sprintf(`{"streams":[{"stream":{%q:%q,"case":"order","app":"checkout","pod":"p1"},"values":[["%d","first"]]}]}`,
			seed.RunLabel, run, ts))
		pushRaw(t, fmt.Sprintf(`{"streams":[{"stream":{"pod":"p1","app":"checkout","case":"order",%q:%q},"values":[["%d","second"]]}]}`,
			seed.RunLabel, run, ts+int64(time.Second)))

		series := waitForCase(t, run, "order", 2, 1)
		fps := map[string]bool{}
		for _, s := range series {
			fps[s.Fingerprint] = true
		}
		if len(fps) != 1 {
			t.Errorf("%d fingerprints for one stream sent with two label orders, want 1: %+v", len(fps), series)
		}
	})

	t.Run("service_name is added from app, kept, or set to unknown", func(t *testing.T) {
		ts := start.UnixNano()
		pushRaw(t, fmt.Sprintf(`{"streams":[
			{"stream":{%[1]q:%[2]q,"case":"svc-app","app":"checkout"},"values":[["%[3]d","x"]]},
			{"stream":{%[1]q:%[2]q,"case":"svc-present","app":"checkout","service_name":"shop"},"values":[["%[3]d","x"]]},
			{"stream":{%[1]q:%[2]q,"case":"svc-none","pod":"p1"},"values":[["%[3]d","x"]]}]}`,
			seed.RunLabel, run, ts))
		for name, want := range map[string]string{"svc-app": "checkout", "svc-present": "shop", "svc-none": "unknown"} {
			series := waitForCase(t, run, name, 1, 1)
			if got := series[0].labelMap(t)["service_name"]; got != want {
				t.Errorf("%s: service_name = %q, want %q (labels %s)", name, got, want, series[0].Labels)
			}
		}
	})

	t.Run("label names are cleaned up", func(t *testing.T) {
		pushRaw(t, fmt.Sprintf(`{"streams":[{"stream":{%q:%q,"case":"names","k8s.pod-name":"p1"},"values":[["%d","x"]]}]}`,
			seed.RunLabel, run, start.UnixNano()))
		series := waitForCase(t, run, "names", 1, 1)
		want := map[string]string{seed.RunLabel: run, "case": "names", "k8s_pod_name": "p1", "service_name": "unknown"}
		if got := series[0].labelMap(t); !maps.Equal(got, want) {
			t.Errorf("labels %v, want %v", got, want)
		}
	})

	t.Run("label values over 100 characters are cut", func(t *testing.T) {
		long := "https://shop.example.com/checkout/cart/items?session=" + strings.Repeat("0", 39) + "7&ref=newsletter-campaign-2026-11"
		pushRaw(t, fmt.Sprintf(`{"streams":[{"stream":{%q:%q,"case":"long","url":%q},"values":[["%d","x"]]}]}`,
			seed.RunLabel, run, long, start.UnixNano()))
		series := waitForCase(t, run, "long", 1, 1)
		if got, want := series[0].labelMap(t)["url"], long[:100]+"..."; got != want {
			t.Errorf("url label (sent %d characters) = %q, want %q", len(long), got, want)
		}
	})

	t.Run("structured metadata is dropped and the line kept", func(t *testing.T) {
		ts := start.UnixNano()
		pushRaw(t, fmt.Sprintf(`{"streams":[{"stream":{%q:%q,"case":"metadata"},"values":[["%d","with metadata",{"trace_id":"0242ac120002"}]]}]}`,
			seed.RunLabel, run, ts))
		series := waitForCase(t, run, "metadata", 1, 1)
		want := map[string]string{seed.RunLabel: run, "case": "metadata", "service_name": "unknown"}
		if got := series[0].labelMap(t); !maps.Equal(got, want) {
			t.Errorf("labels %v, want %v", got, want)
		}
		got := samplesWhere(t, "fingerprint = "+series[0].Fingerprint)
		wantSamples := []sampleRow{{series[0].Fingerprint, ts, 0, "with metadata", sampleTypeLog}}
		if !slices.Equal(got, wantSamples) {
			t.Errorf("samples %v, want %v", got, wantSamples)
		}
	})

	t.Run("lines on two days give one series row per day", func(t *testing.T) {
		today := start
		yesterday := start.Add(-24 * time.Hour)
		pushRaw(t, fmt.Sprintf(`{"streams":[{"stream":{%q:%q,"case":"days"},"values":[["%d","yesterday"],["%d","today"]]}]}`,
			seed.RunLabel, run, yesterday.UnixNano(), today.UnixNano()))
		series := waitForCase(t, run, "days", 2, 2)
		var dates []string
		for _, s := range series {
			dates = append(dates, s.Date)
			if s.Fingerprint != series[0].Fingerprint {
				t.Errorf("rows have different fingerprints: %+v", series)
			}
		}
		want := []string{day(yesterday), day(today)}
		if !slices.Equal(dates, want) {
			t.Errorf("series dates %v, want %v", dates, want)
		}
	})

	t.Run("old push format with a labels string and entries", func(t *testing.T) {
		ts := start.Add(123456789 * time.Nanosecond)
		pushRaw(t, fmt.Sprintf(`{"streams":[{"labels":"{%s=\"%s\", case=\"legacy\", app=\"checkout\"}","entries":[{"ts":%q,"line":"legacy line"}]}]}`,
			seed.RunLabel, run, ts.UTC().Format(time.RFC3339Nano)))
		series := waitForCase(t, run, "legacy", 1, 1)
		want := map[string]string{seed.RunLabel: run, "case": "legacy", "app": "checkout", "service_name": "checkout"}
		if got := series[0].labelMap(t); !maps.Equal(got, want) {
			t.Errorf("labels %v, want %v", got, want)
		}
		got := samplesWhere(t, "fingerprint = "+series[0].Fingerprint)
		wantSamples := []sampleRow{{series[0].Fingerprint, ts.UnixNano(), 0, "legacy line", sampleTypeLog}}
		if !slices.Equal(got, wantSamples) {
			t.Errorf("samples %v, want %v", got, wantSamples)
		}
	})
}

// assertSeedStored checks every row the seed streams of one run should give:
// a series row per stream and day, a sample per line, the label index, and
// the 15-second rollup.
func assertSeedStored(t *testing.T, run string, streams []seed.LogStream) {
	t.Helper()
	total := 0
	for _, s := range streams {
		total += len(s.Lines)
	}
	runFilter := fmt.Sprintf("JSONExtractString(labels, %s) = %s", sqlString(seed.RunLabel), sqlString(run))
	waitForSamples(t, fmt.Sprintf("fingerprint IN (SELECT fingerprint FROM %s WHERE %s)", table(seriesTable), runFilter), total)
	seriesRows := 0
	for _, s := range streams {
		seriesRows += len(days(s.Lines))
	}

	// Labels as stored: in the order sent (names sorted, by both encoders),
	// with service_name, copied from app, last.
	byLabels := map[string][]seriesRow{}
	for _, s := range waitForSeries(t, runFilter, seriesRows) {
		byLabels[s.Labels] = append(byLabels[s.Labels], s)
	}
	if len(byLabels) != len(streams) {
		t.Errorf("%d distinct label sets in %s, want %d", len(byLabels), seriesTable, len(streams))
	}
	fingerprints := map[string]bool{}
	for _, s := range streams {
		labels := fmt.Sprintf(`{"app":%q,%q:%q,"pod":%q,"service_name":%q}`,
			s.Labels["app"], seed.RunLabel, s.Labels[seed.RunLabel], s.Labels["pod"], s.Labels["app"])
		rows := byLabels[labels]
		if len(rows) == 0 {
			t.Errorf("no %s row with labels %s", seriesTable, labels)
			continue
		}
		dates := days(s.Lines)
		fp := rows[0].Fingerprint
		var gotDates []string
		for _, r := range rows {
			gotDates = append(gotDates, r.Date)
			if r.Fingerprint != fp || r.Type != sampleTypeLog || r.Name != "" || r.Metadata != "" {
				t.Errorf("%s row %+v: want fingerprint %s, type %d, empty name and metadata", seriesTable, r, fp, sampleTypeLog)
			}
		}
		if !slices.Equal(gotDates, dates) {
			t.Errorf("%s: dates %v, want %v", labels, gotDates, dates)
		}
		if fingerprints[fp] {
			t.Errorf("fingerprint %s is shared by two streams", fp)
		}
		fingerprints[fp] = true

		var want []sampleRow
		textBytes := 0
		for _, l := range s.Lines {
			want = append(want, sampleRow{fp, l.Time.UnixNano(), 0, l.Text, sampleTypeLog})
			textBytes += len(l.Text)
		}
		if got := samplesWhere(t, "fingerprint = "+fp); !slices.Equal(got, want) {
			t.Errorf("%s: %s rows\n got %v\nwant %v", labels, samplesTable, got, want)
		}

		var index []string
		for _, row := range query(t, fmt.Sprintf("SELECT DISTINCT key, val, type FROM %s WHERE fingerprint = %s ORDER BY key",
			table(labelIndexTable), fp)) {
			index = append(index, fmt.Sprintf("%v=%v type %v", row["key"], row["val"], row["type"]))
		}
		var wantIndex []string
		for _, k := range []string{"app", seed.RunLabel, "pod", "service_name"} {
			v := s.Labels[k]
			if k == "service_name" {
				v = s.Labels["app"]
			}
			wantIndex = append(wantIndex, fmt.Sprintf("%s=%s type %d", k, v, sampleTypeLog))
		}
		if !slices.Equal(index, wantIndex) {
			t.Errorf("%s: %s rows %v, want %v", labels, labelIndexTable, index, wantIndex)
		}

		rollup := query(t, fmt.Sprintf("SELECT toString(countMerge(count)) AS lines, toString(sum(bytes)) AS bytes, groupUniqArray(type) AS types FROM %s WHERE fingerprint = %s",
			table(logsRollupTable), fp))
		wantRollup := fmt.Sprintf("lines %d, bytes %d, types [%d]", len(s.Lines), textBytes, sampleTypeLog)
		if got := fmt.Sprintf("lines %v, bytes %v, types %v", rollup[0]["lines"], rollup[0]["bytes"], rollup[0]["types"]); got != wantRollup {
			t.Errorf("%s: %s %s, want %s", labels, logsRollupTable, got, wantRollup)
		}
	}
}

// seriesRow is a row of the series table. Rows are read without FINAL: the
// table is a ReplacingMergeTree(date) ordered by fingerprint and type, and
// FINAL keeps only the newest day's row of a stream.
type seriesRow struct {
	Date        string `json:"date"`
	Fingerprint string `json:"fp"`
	Labels      string `json:"labels"`
	Name        string `json:"name"`
	Type        int    `json:"type"`
	Metadata    string `json:"metadata"`
}

func (s seriesRow) labelMap(t *testing.T) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal([]byte(s.Labels), &m); err != nil {
		t.Fatalf("labels %q are not a JSON object of strings: %v", s.Labels, err)
	}
	return m
}

// sampleRow is a row of the samples table.
type sampleRow struct {
	Fingerprint string
	TimestampNs int64
	Value       float64
	String      string
	Type        int
}

func seriesWhere(t *testing.T, where string) []seriesRow {
	t.Helper()
	var out []seriesRow
	for _, row := range query(t, fmt.Sprintf("SELECT DISTINCT date, toString(fingerprint) AS fp, labels, name, type, metadata FROM %s WHERE %s ORDER BY date, fp",
		table(seriesTable), where)) {
		raw, _ := json.Marshal(row)
		var s seriesRow
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("%s row %s: %v", seriesTable, raw, err)
		}
		out = append(out, s)
	}
	return out
}

func samplesWhere(t *testing.T, where string) []sampleRow {
	t.Helper()
	var out []sampleRow
	for _, row := range query(t, fmt.Sprintf("SELECT toString(fingerprint) AS fp, toString(timestamp_ns) AS ts, value, string, type FROM %s WHERE %s ORDER BY fingerprint, timestamp_ns",
		table(samplesTable), where)) {
		ts, err := strconv.ParseInt(fmt.Sprint(row["ts"]), 10, 64)
		if err != nil {
			t.Fatalf("%s row %v: timestamp: %v", samplesTable, row, err)
		}
		value, _ := row["value"].(float64)
		typ, _ := row["type"].(float64)
		out = append(out, sampleRow{fmt.Sprint(row["fp"]), ts, value, fmt.Sprint(row["string"]), int(typ)})
	}
	return out
}

// waitForSamples waits until the samples table has want rows matching where,
// since gigapipe inserts asynchronously, and returns them.
func waitForSamples(t *testing.T, where string, want int) []sampleRow {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := samplesWhere(t, where)
		if len(got) == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 30s %s has %d rows where %s, want %d: %v", samplesTable, len(got), where, want, got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForCase waits until the stream of one hand-written case has lines
// samples and at least seriesRows series rows, then returns its series rows.
func waitForCase(t *testing.T, run, name string, lines, seriesRows int) []seriesRow {
	t.Helper()
	where := fmt.Sprintf("JSONExtractString(labels, %s) = %s AND JSONExtractString(labels, 'case') = %s",
		sqlString(seed.RunLabel), sqlString(run), sqlString(name))
	waitForSamples(t, fmt.Sprintf("fingerprint IN (SELECT fingerprint FROM %s WHERE %s)", table(seriesTable), where), lines)
	return waitForSeries(t, where, seriesRows)
}

// waitForSeries waits until the series table has at least want rows matching
// where, and returns them. gigapipe writes series rows and samples separately,
// so samples being there doesn't mean the series rows are.
func waitForSeries(t *testing.T, where string, want int) []seriesRow {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := seriesWhere(t, where)
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 30s %s has %d rows where %s, want %d: %+v", seriesTable, len(got), where, want, got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func pushRaw(t *testing.T, body string) {
	t.Helper()
	postBody(t, "/loki/api/v1/push", "application/json", []byte(body))
}

func postBody(t *testing.T, path, contentType string, body []byte) {
	t.Helper()
	resp, err := http.Post(baseURL()+path, contentType, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s (%s): status %d, body %q", path, contentType, resp.StatusCode, raw)
	}
}

// query runs a SELECT through clickhouseQuery (process_test.go) and returns
// its rows as JSON objects.
func query(t *testing.T, sql string) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for _, line := range strings.Split(clickhouseQuery(t, sql+" FORMAT JSONEachRow"), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("ClickHouse row %q: %v", line, err)
		}
		rows = append(rows, row)
	}
	return rows
}

// table qualifies a table name with gigapipe's database.
func table(name string) string {
	return envOr("CLICKHOUSE_DB", "cloki") + "." + name
}

// sqlString quotes s as a ClickHouse string literal.
func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// days lists the UTC dates of lines, in order, each once.
func days(lines []seed.LogLine) []string {
	var out []string
	for _, l := range lines {
		if d := day(l.Time); !slices.Contains(out, d) {
			out = append(out, d)
		}
	}
	return out
}

// day is the UTC date gigapipe files a timestamp under in the series table.
func day(ts time.Time) string {
	return ts.UTC().Format(time.DateOnly)
}
