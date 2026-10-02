//go:build integration

// The metric import: a dataset stored the way earlier releases stored metrics (rows of type 2
// in time_series, samples_v3 and metrics_15s) is copied into the metric stack, and reads back
// as the same samples written through remote write do.

package integration

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/qryn/helputils"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

const staleMarkerExpr = "reinterpretAsFloat64(toUInt64(9218868437227405314))"

func clickhouseConn(t *testing.T) clickhouse.Conn {
	t.Helper()
	addr := "localhost:8123"
	if base := os.Getenv("CLICKHOUSE_HTTP_URL"); base != "" {
		u, err := url.Parse(base)
		if err != nil {
			t.Fatal(err)
		}
		addr = u.Host
	}
	db := os.Getenv("CLICKHOUSE_DB")
	if db == "" {
		db = "cloki"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{Database: db}, ReadTimeout: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func clickhouseDB() string {
	if db := os.Getenv("CLICKHOUSE_DB"); db != "" {
		return db
	}
	return "cloki"
}

// clickhouseQueryAs runs sql under queryID, ignoring its outcome.
func clickhouseQueryAs(queryID, sql string) {
	base := os.Getenv("CLICKHOUSE_HTTP_URL")
	if base == "" {
		base = "http://localhost:8123"
	}
	resp, err := http.Post(strings.TrimRight(base, "/")+"/?database="+clickhouseDB()+"&query_id="+url.QueryEscape(queryID),
		"text/plain", strings.NewReader(sql))
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func runImport(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := maintenance.ImportMetrics(ctx, conn, maintenance.MetricImportOptions{
		Instance: "integration", Database: clickhouseDB(), SamplesDays: 7, RollupDays: 30,
		Settle: 10 * time.Millisecond, Poll: 200 * time.Millisecond, Quiet: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func putImportRecord(t *testing.T, name, value string) {
	t.Helper()
	fp := helputils.FingerprintLabelsDJBHashPrometheus(
		fmt.Appendf(nil, `{"type":%s, "name":%s`, strconv.Quote("metric_import"), strconv.Quote(name)))
	clickhouseQuery(t, fmt.Sprintf("INSERT INTO settings (fingerprint, type, name, value, inserted_at) "+
		"VALUES (%d, 'metric_import', '%s', '%s', now64(9))", fp, name, value))
}

func dropImportCompletion(t *testing.T) {
	t.Helper()
	clickhouseQuery(t, "ALTER TABLE settings DELETE WHERE type = 'update' AND name = 'metric_import' "+
		"SETTINGS mutations_sync = 1")
}

// importSample is one sample of the dataset; ns is the instant stored in samples_v3.
type importSample struct {
	ns    int64
	value float64
	stale bool
}

func (s importSample) ms() int64 { return s.ns / int64(time.Millisecond) }

// importDataset is a counter around two midnights and H0 with a reset, a stale marker,
// a duplicate instant and two more rows on its ms, the last with a larger value.
func importDataset(h0 time.Time) (samples []importSample, late importSample) {
	m1, m2 := h0.Truncate(24*time.Hour).Add(-24*time.Hour), h0.Truncate(24*time.Hour)
	seen := map[int64]bool{}
	var times []int64
	cluster := func(at time.Time, before, after time.Duration) {
		for ts := at.Add(-before); !ts.After(at.Add(after)); ts = ts.Add(time.Minute) {
			if !seen[ts.UnixNano()] {
				seen[ts.UnixNano()] = true
				times = append(times, ts.UnixNano())
			}
		}
	}
	cluster(m1, 10*time.Minute, 10*time.Minute)
	cluster(m2, 10*time.Minute, 10*time.Minute)
	cluster(h0, 10*time.Minute, 20*time.Minute)
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	value := 10.0
	for _, ns := range times {
		value++
		if ns == m1.UnixNano() {
			value = 2
		}
		samples = append(samples, importSample{ns: ns, value: value})
	}
	at := func(d time.Duration) int64 { return m1.Add(d).UnixNano() }
	find := func(ns int64) float64 {
		for _, s := range samples {
			if s.ns == ns {
				return s.value
			}
		}
		panic("no sample")
	}
	samples = append(samples,
		importSample{ns: at(5*time.Minute + 30*time.Second), stale: true},
		importSample{ns: at(2 * time.Minute), value: find(at(2 * time.Minute))},
		importSample{ns: at(3*time.Minute) + 123, value: find(at(3 * time.Minute))},
		importSample{ns: at(3*time.Minute) + 456, value: find(at(3*time.Minute)) + 5},
	)
	for i, s := range samples {
		if s.ns == at(7*time.Minute) {
			late = s
			samples = append(samples[:i], samples[i+1:]...)
			break
		}
	}
	return samples, late
}

func samplesV3Insert(fp uint64, tp int, rows []importSample) string {
	var values []string
	for _, s := range rows {
		v := strconv.FormatFloat(s.value, 'g', -1, 64)
		if s.stale {
			v = staleMarkerExpr
		}
		values = append(values, fmt.Sprintf("(%d, %d, %s, '', %d)", fp, s.ns, v, tp))
	}
	return "INSERT INTO samples_v3 (fingerprint, timestamp_ns, value, string, type) VALUES " + strings.Join(values, ", ")
}

// tierRows renders a series' buckets in tier, one line per bucket.
func tierRows(t *testing.T, tier string, fp uint64) string {
	t.Helper()
	return clickhouseQuery(t, fmt.Sprintf("SELECT bucket, minIfMerge(first), maxIfMerge(last), sum(count), "+
		"round(sum(sum), 6), round(varPopStableIfMerge(var), 6), minIfMerge(min), maxIfMerge(max), "+
		"sum(resets), sum(reset_drop), sum(changes), max(stale_at) "+
		"FROM %s WHERE fingerprint = %d GROUP BY bucket ORDER BY bucket", tier, fp))
}

func rawRows(t *testing.T, fp uint64) string {
	t.Helper()
	return clickhouseQuery(t, fmt.Sprintf("SELECT timestamp, value FROM metric_samples FINAL "+
		"WHERE fingerprint = %d ORDER BY timestamp", fp))
}

func fingerprintOf(t *testing.T, name string) uint64 {
	t.Helper()
	fp, err := strconv.ParseUint(clickhouseQuery(t, fmt.Sprintf(
		"SELECT any(fingerprint) FROM metric_series WHERE name = '%s'", name)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

func TestMetricImportCopiesTheSharedTablesIntoTheMetricStack(t *testing.T) {
	waitReady(t)
	waitReadyAt(t, forced5m)
	waitReadyAt(t, forced1h)
	conn := clickhouseConn(t)

	// The instance started on a deployment without metric rows and completed the import at once.
	eventually(t, "SELECT count() > 0 FROM settings WHERE type = 'update' AND name = 'metric_import'", "1")

	t0, err := strconv.ParseInt(clickhouseQuery(t,
		"SELECT argMax(value, inserted_at) FROM settings WHERE type = 'update' AND name = 'metric_stack'"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	h0 := time.Unix(t0, 0).UTC().Truncate(time.Hour).Add(-time.Hour)
	floor := h0.Truncate(24 * time.Hour).Add(-48 * time.Hour)
	rollupAt := floor.Add(-48*time.Hour + 10*time.Hour)

	stamp := time.Now().UnixNano()
	imported, reference := fmt.Sprintf("it_import_%d", stamp), fmt.Sprintf("it_import_ref_%d", stamp)
	fp, logFp := uint64(stamp)|1<<63, uint64(stamp)|1<<62
	samples, late := importDataset(h0)

	// The shared tables as earlier releases filled them.
	// time_series rows exist on the raw days only: they expire with samples_v3.
	days := map[string]bool{}
	for _, s := range append(samples, late) {
		days[time.Unix(0, s.ns).UTC().Format(time.DateOnly)] = true
	}
	labels := fmt.Sprintf(`{"__name__":"%s","job":"import"}`, imported)
	var series []string
	for d := range days {
		series = append(series, fmt.Sprintf("('%s', %d, '%s', '%s', 2, '{\"type\":\"counter\",\"help\":\"Imported\",\"unit\":\"\"}', 1)",
			d, fp, labels, imported))
	}
	series = append(series, fmt.Sprintf(`('%s', %d, '{"job":"import","level":"info"}', '', 1, '', 1)`,
		h0.Format(time.DateOnly), logFp))
	clickhouseQuery(t, "INSERT INTO time_series (date, fingerprint, labels, name, type, metadata, updated_at_ns) VALUES "+
		strings.Join(series, ", "))
	clickhouseQuery(t, samplesV3Insert(fp, 2, samples))
	clickhouseQuery(t, samplesV3Insert(fp, 2, []importSample{late}))
	// A series stored as type 0.
	both, bothFp := imported+"_both", uint64(stamp)|1<<61
	clickhouseQuery(t, fmt.Sprintf(`INSERT INTO time_series (date, fingerprint, labels, name, type, metadata, updated_at_ns) `+
		`VALUES ('%s', %d, '{"__name__":"%s"}', '%s', 0, '', 1)`, h0.Format(time.DateOnly), bothFp, both, both))
	clickhouseQuery(t, samplesV3Insert(bothFp, 0, []importSample{{ns: h0.Add(-3 * time.Minute).UnixNano(), value: 1},
		{ns: h0.Add(-2 * time.Minute).UnixNano(), value: 2}, {ns: h0.Add(-time.Minute).UnixNano(), value: 3}}))
	clickhouseQuery(t, fmt.Sprintf("INSERT INTO samples_v3 (fingerprint, timestamp_ns, value, string, type) "+
		"VALUES (%d, %d, 0, 'a log line', 1)", logFp, h0.Add(-5*time.Minute).UnixNano()))
	rollup := []float64{1, 2, 3, 1}
	clickhouseQuery(t, fmt.Sprintf("INSERT INTO metrics_15s (fingerprint, timestamp_ns, last, max, min, count, sum, bytes, type) "+
		"SELECT %d, ts, argMaxState(v, ts), max(v), min(v), countState(), sum(v), 0, 2 FROM "+
		"(SELECT arrayJoin(arrayEnumerate([1., 2., 3., 1.])) AS i, [1., 2., 3., 1.][i] AS v, "+
		"%d + (i - 1) * 15000000000 AS ts) GROUP BY ts", fp, rollupAt.UnixNano()))

	// The same samples through remote write, in time order.
	all := append(append([]importSample(nil), samples...), late)
	sort.SliceStable(all, func(i, j int) bool { return all[i].ns < all[j].ns })
	var written []*prompb.Sample
	for i, v := range rollup {
		written = append(written, &prompb.Sample{Timestamp: rollupAt.Add(time.Duration(i) * 15 * time.Second).UnixMilli(), Value: v})
	}
	for _, s := range all {
		v := s.value
		if s.stale {
			v = math.Float64frombits(0x7ff0000000000002)
		}
		written = append(written, &prompb.Sample{Timestamp: s.ms(), Value: v})
	}
	for _, s := range written {
		remoteWrite(t, &prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: reference},
			{Name: "job", Value: "import"}}, Samples: []*prompb.Sample{s}})
	}
	distinct := map[int64]bool{}
	for _, s := range written {
		distinct[s.Timestamp] = true
	}
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_samples FINAL WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", reference), fmt.Sprint(len(distinct)))
	refFp := fingerprintOf(t, reference)

	clickhouseQuery(t, "ALTER TABLE settings DELETE WHERE type = 'metric_import' SETTINGS mutations_sync = 1")
	dropImportCompletion(t)
	runImport(t, conn)

	if got := clickhouseQuery(t, fmt.Sprintf("SELECT name, labels, first_seen, last_seen FROM metric_series FINAL "+
		"WHERE fingerprint = %d", fp)); !strings.HasPrefix(got, imported+"\t{'__name__':'"+imported+"','job':'import'}\t") {
		t.Errorf("series = %q", got)
	}
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT (SELECT any(name) FROM metric_series WHERE fingerprint = %d), "+
		"(SELECT count() FROM metric_samples FINAL WHERE fingerprint = %d)", bothFp, bothFp)); got != both+"\t3" {
		t.Errorf("the type 0 series = %q, want its three samples", got)
	}
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT type, help FROM metric_metadata FINAL WHERE name = '%s'", imported)); got != "counter\tImported" {
		t.Errorf("metadata = %q", got)
	}
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT (SELECT count() FROM metric_series WHERE fingerprint = %d) + "+
		"(SELECT count() FROM metric_samples WHERE fingerprint = %d)", logFp, logFp)); got != "0" {
		t.Errorf("the log series reached the metric stack: %s rows", got)
	}
	// The 15s rows 1, 2, 3, 1 at a 15s scrape: the bucket ending 5m after the first holds three samples.
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT sum(count), sum(resets), sum(reset_drop), sum(changes) "+
		"FROM metrics_5m WHERE fingerprint = %d AND bucket = fromUnixTimestamp64Milli(%d)",
		fp, rollupAt.Add(5*time.Minute).UnixMilli())); got != "3\t1\t3\t2" {
		t.Errorf("15s rows in the 5m tier = %q, want 3 1 3 2", got)
	}

	// The reference is read once: a redo deletes every series' buckets in the chunk.
	want := map[string]string{"raw": rawRows(t, refFp)}
	for _, tier := range []string{"metrics_5m", "metrics_1h"} {
		want[tier] = tierRows(t, tier, refFp)
	}
	sameAsReference := func(stage string) {
		t.Helper()
		if got := rawRows(t, fp); got != want["raw"] {
			t.Errorf("%s: raw differs from remote write:\n%s\nwant\n%s", stage, got, want["raw"])
		}
		for _, tier := range []string{"metrics_5m", "metrics_1h"} {
			if got := tierRows(t, tier, fp); got != want[tier] {
				t.Errorf("%s: %s differs from remote write:\n%s\nwant\n%s", stage, tier, got, want[tier])
			}
		}
	}
	sameAsReference("import")

	m1 := h0.Truncate(24 * time.Hour).Add(-24 * time.Hour)
	windows := [][2]time.Time{
		{m1.Add(-15 * time.Minute), m1.Add(15 * time.Minute)},
		{m1.Add(24*time.Hour - 15*time.Minute), m1.Add(24*time.Hour + 15*time.Minute)},
		{h0.Add(-15 * time.Minute), h0.Add(30 * time.Minute)},
		{rollupAt.Add(-5 * time.Minute), rollupAt.Add(10 * time.Minute)},
	}
	for _, reader := range []struct {
		name, base string
		step       int64
	}{{"default", baseURL(), 60000}, {"5m", forced5m, 300000}, {"1h", forced1h, 3600000}} {
		for _, q := range []string{"sum(rate(%s[5m]))", "sum(increase(%s[10m]))", "sum(changes(%s[10m]))",
			"sum(resets(%s[10m]))", "sum(count_over_time(%s[10m]))", "sum(last_over_time(%s[10m]))",
			"sum(max_over_time(%s[10m]))", "sum(%s)"} {
			answered := 0
			for _, w := range windows {
				start := w[0].UnixMilli() - w[0].UnixMilli()%reader.step
				got := rangeFrom(t, reader.base, fmt.Sprintf(q, imported), start, w[1].UnixMilli(), reader.step, 0)
				want := rangeFrom(t, reader.base, fmt.Sprintf(q, reference), start, w[1].UnixMilli(), reader.step, 0)
				if len(want) > 0 {
					answered++
				}
				if d := sameSeries(got, want); d != "" {
					t.Errorf("%s reader, %s over %s: %s", reader.name, fmt.Sprintf(q, "imported"), w[0], d)
				}
			}
			if answered == 0 {
				t.Errorf("%s reader, %s: no result in any window", reader.name, fmt.Sprintf(q, "reference"))
			}
		}
	}

	// The 15s history is reachable though no time_series row covers its day.
	rollupWindow := windows[3]
	if got := rangeQuery(t, fmt.Sprintf("sum(count_over_time(%s[10m]))", imported),
		rollupWindow[0].UnixMilli(), rollupWindow[1].UnixMilli(), 0); len(got) == 0 {
		t.Errorf("no series answers over the 15s history")
	}

	// A chunk recorded started whose insert is still running elsewhere, one row per half second,
	// is killed and redone without double counting.
	chunk := "chunk:" + strconv.FormatInt(h0.Truncate(24*time.Hour).UnixMilli(), 10)
	putImportRecord(t, chunk, "started")
	dropImportCompletion(t)
	orphanID := "metric_import-" + clickhouseDB() + "-" + chunk
	orphan := make(chan struct{})
	go func() {
		defer close(orphan)
		clickhouseQueryAs(orphanID, fmt.Sprintf("INSERT INTO metric_samples_in "+
			"(fingerprint, timestamp, value, prev_timestamp, prev_value, aggregate) "+
			"WITH (SELECT groupArray((timestamp, value)) FROM metric_samples FINAL WHERE fingerprint = %d "+
			"AND timestamp > fromUnixTimestamp64Milli(%d) AND timestamp <= fromUnixTimestamp64Milli(%d)) AS rows "+
			"SELECT %d, rows[number + 1].1, rows[number + 1].2, toDateTime64(0, 3), 0, 1 FROM numbers(100) "+
			"WHERE number < length(rows) AND sleepEachRow(0.5) = 0 "+
			"SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1",
			fp, m1.UnixMilli(), m1.Add(24*time.Hour).UnixMilli(), fp))
	}()
	for deadline := time.Now().Add(30 * time.Second); tierRows(t, "metrics_5m", fp) == want["metrics_5m"]; {
		if time.Now().After(deadline) {
			t.Fatal("the orphaned insert staged nothing")
		}
		time.Sleep(200 * time.Millisecond)
	}
	runImport(t, conn)
	<-orphan
	sameAsReference("redo")

	// A completed import is a no-op.
	records := "SELECT count() FROM settings WHERE type IN ('metric_import', 'update')"
	before, tiers := clickhouseQuery(t, records), tierRows(t, "metrics_5m", fp)+tierRows(t, "metrics_1h", fp)
	runImport(t, conn)
	if after := clickhouseQuery(t, records); after != before {
		t.Errorf("a second run wrote settings: %s rows, then %s", before, after)
	}
	if got := tierRows(t, "metrics_5m", fp) + tierRows(t, "metrics_1h", fp); got != tiers {
		t.Errorf("a second run changed the tiers")
	}
}
