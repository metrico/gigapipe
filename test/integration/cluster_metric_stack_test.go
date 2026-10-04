//go:build integration

// The metric stack on a ClickHouse cluster. These tests run only with CLUSTER_NAME set, against
// the two-shard stack of scripts/test/e2e/docker-compose.yml overridden by
// docker-compose.cluster.yml in this directory:
//
//	docker compose -f scripts/test/e2e/docker-compose.yml -f test/integration/docker-compose.cluster.yml up -d --build
//	CLUSTER_NAME=test_cluster_two_shards GIGAPIPE_URL=http://a:b@localhost:3102 \
//	CLICKHOUSE_HTTP_URL=http://qryn:q1w2e3r4@localhost:18123 CLICKHOUSE2_HTTP_URL=http://qryn:q1w2e3r4@localhost:18124 \
//	go test -tags integration -count=1 -run TestCluster ./test/integration/
//
// CLICKHOUSE_HTTP_URL is the shard gigapipe connects to; CLICKHOUSE2_HTTP_URL is the other.

package integration

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

// clusterName returns CLUSTER_NAME, skipping the test without it.
func clusterName(t *testing.T) string {
	t.Helper()
	name := os.Getenv("CLUSTER_NAME")
	if name == "" {
		t.Skip("CLUSTER_NAME is not set")
	}
	return name
}

// shards returns the HTTP URLs of the two shards.
func shards(t *testing.T) []string {
	t.Helper()
	res := []string{os.Getenv("CLICKHOUSE_HTTP_URL"), os.Getenv("CLICKHOUSE2_HTTP_URL")}
	if res[0] == "" || res[1] == "" {
		t.Fatal("CLICKHOUSE_HTTP_URL and CLICKHOUSE2_HTTP_URL must name the two shards")
	}
	return res
}

// holders returns, for each fingerprint of fps found in the local table, the shards holding it.
func holders(t *testing.T, table string, fps []uint64) map[uint64][]int {
	t.Helper()
	list := make([]string, len(fps))
	for i, fp := range fps {
		list[i] = strconv.FormatUint(fp, 10)
	}
	res := map[uint64][]int{}
	for i, base := range shards(t) {
		out := clickhouseQueryAt(t, base, fmt.Sprintf("SELECT DISTINCT fingerprint FROM %s WHERE fingerprint IN (%s)",
			table, strings.Join(list, ",")))
		for _, line := range strings.Fields(out) {
			fp, err := strconv.ParseUint(line, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			res[fp] = append(res[fp], i)
		}
	}
	return res
}

// fingerprintsNamed returns the fingerprint of each series whose name starts with prefix.
func fingerprintsNamed(t *testing.T, prefix string) []uint64 {
	t.Helper()
	var res []uint64
	for _, line := range strings.Fields(clickhouseQuery(t, fmt.Sprintf(
		"SELECT DISTINCT fingerprint FROM metric_series_dist WHERE startsWith(name, '%s')", prefix))) {
		fp, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, fp)
	}
	return res
}

func TestClusterKeepsEverythingOfASeriesOnOneShard(t *testing.T) {
	clusterName(t)
	waitReady(t)
	prefix := fmt.Sprintf("it_cluster_coloc_%d_", time.Now().UnixNano())
	t0 := time.Now().Add(-2 * time.Hour).Truncate(time.Hour).UnixMilli()
	const n = 16
	var req []*prompb.TimeSeries
	for i := range n {
		req = append(req, &prompb.TimeSeries{
			Labels: []*prompb.Label{{Name: "__name__", Value: fmt.Sprintf("%s%d", prefix, i)}, {Name: "job", Value: "coloc"}},
			Samples: []*prompb.Sample{{Timestamp: t0 + 60000, Value: 1}, {Timestamp: t0 + 120000, Value: 2},
				{Timestamp: t0 + 360000, Value: 3}},
			Exemplars: []*prompb.Exemplar{{Labels: []*prompb.Label{{Name: "trace_id", Value: fmt.Sprint(i)}},
				Value: 2, Timestamp: t0 + 120000}},
		})
	}
	remoteWrite(t, req...)
	in := fmt.Sprintf("fingerprint IN (SELECT fingerprint FROM metric_series WHERE startsWith(name, '%s'))", prefix)
	eventually(t, "SELECT count() FROM metric_samples_dist WHERE "+in, fmt.Sprint(3*n))
	eventually(t, "SELECT count() FROM metric_exemplars_dist WHERE "+in, fmt.Sprint(n))
	eventually(t, "SELECT sum(count) FROM metrics_1h_dist WHERE "+in, fmt.Sprint(3*n))

	fps := fingerprintsNamed(t, prefix)
	if len(fps) != n {
		t.Fatalf("%d series in metric_series_dist, want %d", len(fps), n)
	}
	series := holders(t, "metric_series", fps)
	perShard := map[int]int{}
	for _, fp := range fps {
		if len(series[fp]) != 1 {
			t.Fatalf("series %d is on shards %v", fp, series[fp])
		}
		perShard[series[fp][0]]++
	}
	if len(perShard) != 2 {
		t.Errorf("every series is on one shard: %v", perShard)
	}
	for _, table := range []string{"metric_samples", "metrics_5m", "metrics_1h", "metric_exemplars"} {
		got := holders(t, table, fps)
		for _, fp := range fps {
			if !slices.Equal(got[fp], series[fp]) {
				t.Errorf("%s of series %d is on shards %v, its series row on %v", table, fp, got[fp], series[fp])
			}
		}
	}
}

func TestClusterPromQLReproducesTheProbe(t *testing.T) {
	clusterName(t)
	waitReady(t)
	name := fmt.Sprintf("it_cluster_probe_%d", time.Now().UnixNano())
	t0 := offFiveMinuteGrid()
	end := t0 + 600000
	remoteWrite(t,
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "probe"}},
			Samples: probeSamples(t0)},
		&prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "instance", Value: "b"}, {Name: "job", Value: "probe"}},
			Samples: gaugeSamples(t0)})
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_samples_dist WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), "54")

	sel := name + `{instance=""}`
	for _, tc := range []struct {
		query   string
		instant bool
		ts      int64
		want    string
	}{
		{"rate(%s[5m])", false, t0 + 300000, "0.06666667"},
		{"rate(%s[5m])", false, end, "0.04561404"},
		{"rate(%s[10m])", true, end, "0.05641026"},
		{"increase(%s[10m])", true, end, "33.84615"},
		{"resets(%s[10m])", true, end, "2"},
		{"changes(%s[10m])", true, end, "32"},
		{"count_over_time(%s[10m])", true, end, "33"},
		{"sum_over_time(%s[10m])", true, end, "273"},
		{"max_over_time(%s[10m])", true, end, "19"},
		{"sum by (job) (rate(%s[5m]))", false, end, "0.04561404"},
		{"%s", false, t0 + 300000, "17"},
		{"%s", false, t0 + 420000, "absent"},
		{"%s", true, end, "12"},
		// offset keeps the read from being pushed down: the engine reads raw samples
		{"rate(%s[5m] offset 1m)", true, end + 60000, "0.04561404"},
		{"%s offset 1m", true, end + 60000, "12"},
	} {
		query := fmt.Sprintf(tc.query, sel)
		var got series
		if tc.instant {
			got = instantQuery(t, query, tc.ts, 0)
		} else {
			got = rangeQuery(t, query, t0, end, 0)
		}
		if v := pointAt(got, tc.ts); v != tc.want {
			t.Errorf("%s at +%ds = %s, want %s", query, (tc.ts-t0)/1000, v, tc.want)
		}
	}
}

// pointAt formats the value of the one series of s at ts as the probe prints it.
func pointAt(s series, ts int64) string {
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

func TestClusterTierAlignedReadReproducesTheProbe(t *testing.T) {
	clusterName(t)
	waitReady(t)
	name := fmt.Sprintf("it_cluster_tier_%d", time.Now().UnixNano())
	t0 := time.Now().Add(-3 * time.Hour).Truncate(time.Hour).UnixMilli()
	end := t0 + 600000
	remoteWrite(t, &prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "probe"}},
		Samples: probeSamples(t0)})
	eventually(t, fmt.Sprintf("SELECT sum(count) FROM metrics_5m_dist WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), "33")
	for _, tc := range []struct {
		query, tag string
		ts         int64
		want       string
	}{
		{"rate(%s[5m])", "r5a", t0 + 300000, "0.06666667"},
		{"rate(%s[5m])", "r5b", end, "0.04561404"},
		{"increase(%s[10m])", "inc10", end, "33.84615"},
		{"%s", "x", end, "12"},
	} {
		tag := fmt.Sprintf("cluster_tier_%s_%d", tc.tag, t0)
		query := fmt.Sprintf(tc.query, fmt.Sprintf(`%s{job!="%s"}`, name, tag))
		if v := pointAt(rangeFrom(t, baseURL(), query, t0, end, 300000, 0), tc.ts); v != tc.want {
			t.Errorf("%s at +%ds = %s, want %s", tc.query, (tc.ts-t0)/1000, v, tc.want)
		}
		if tables := tablesRead(t, tag); !strings.Contains(tables, "metrics_5m_dist") || strings.Contains(tables, "metric_samples") {
			t.Errorf("%s read %q, want the 5m tier's distributed table", tc.query, tables)
		}
	}
}

func TestClusterLabelEndpointsAndMetadata(t *testing.T) {
	clusterName(t)
	waitReady(t)
	sfx := time.Now().UnixNano()
	name := fmt.Sprintf("it_cluster_lbl_%d", sfx)
	key := fmt.Sprintf("it_cluster_key_%d", sfx)
	t0 := time.Now().Add(-time.Hour).Truncate(time.Second).UnixMilli()
	var req []*prompb.TimeSeries
	for i := range 8 {
		req = append(req, &prompb.TimeSeries{
			Labels: []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "lbl"}, {Name: "pod", Value: fmt.Sprint(i)},
				{Name: key, Value: "v"}},
			Samples:   []*prompb.Sample{{Timestamp: t0, Value: float64(i)}},
			Exemplars: []*prompb.Exemplar{{Labels: []*prompb.Label{{Name: "trace_id", Value: fmt.Sprintf("t%d", i)}}, Value: float64(i), Timestamp: t0}},
		})
	}
	remoteWriteRequest(t, &prompb.WriteRequest{Timeseries: req,
		Metadata: []*prompb.MetricMetadata{{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: name, Help: "First."}}})
	in := fmt.Sprintf("fingerprint IN (SELECT fingerprint FROM metric_series WHERE name = '%s')", name)
	eventually(t, "SELECT count() FROM metric_exemplars_dist WHERE "+in, "8")
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_metadata_dist WHERE name = '%s'", name), "1")
	remoteWriteRequest(t, &prompb.WriteRequest{
		Metadata: []*prompb.MetricMetadata{{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: name, Help: "Second."}}})
	eventually(t, fmt.Sprintf("SELECT help FROM metric_metadata_dist FINAL WHERE name = '%s'", name), "Second.")
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT count() FROM metric_metadata_dist FINAL WHERE name = '%s'", name)); got != "1" {
		t.Errorf("metadata FINAL holds %s rows of %s, want 1", got, name)
	}
	var holding []string
	for i, base := range shards(t) {
		if c := clickhouseQueryAt(t, base, fmt.Sprintf("SELECT count() FROM metric_metadata WHERE name = '%s'", name)); c != "0" {
			holding = append(holding, fmt.Sprintf("shard %d: %s rows", i, c))
		}
	}
	if len(holding) != 1 {
		t.Errorf("metadata of %s on %v, want one shard", name, holding)
	}

	match := url.Values{"match[]": {name}}
	names, _ := labelGet(t, "/api/v1/labels", match)
	if got := decode[[]string](t, names.Data); !slices.Contains(got, "job") || !slices.Contains(got, "pod") {
		t.Errorf("labels = %v", got)
	}
	// Each shard's view indexes the label names of its own series; the wrapper reads them all.
	eventually(t, fmt.Sprintf("SELECT count() > 0 FROM metric_label_names_dist WHERE label = '%s'", key), "1")
	all, _ := labelGet(t, "/api/v1/labels", url.Values{})
	if got := decode[[]string](t, all.Data); !slices.Contains(got, key) || !slices.Contains(got, "pod") {
		t.Errorf("labels without a selector = %v", got)
	}
	values, _ := labelGet(t, "/api/v1/label/pod/values", match)
	if got := decode[[]string](t, values.Data); len(got) != 8 {
		t.Errorf("pod values = %v, want 8", got)
	}
	list, _ := labelGet(t, "/api/v1/series", match)
	if got := decode[[]map[string]string](t, list.Data); len(got) != 8 {
		t.Errorf("series = %v, want 8", got)
	}
	meta, _ := labelGet(t, "/api/v1/metadata", url.Values{"metric": {name}})
	type entry struct {
		Type string `json:"type"`
		Help string `json:"help"`
	}
	if got := decode[map[string][]entry](t, meta.Data); len(got[name]) != 1 || got[name][0].Help != "Second." {
		t.Errorf("metadata = %v", got)
	}
	ex, _ := labelGet(t, "/api/v1/query_exemplars", url.Values{"query": {name},
		"start": {fmt.Sprint(t0/1000 - 60)}, "end": {fmt.Sprint(t0/1000 + 60)}})
	type exSeries struct {
		SeriesLabels map[string]string `json:"seriesLabels"`
		Exemplars    []any             `json:"exemplars"`
	}
	got := decode[[]exSeries](t, ex.Data)
	total := 0
	for _, s := range got {
		total += len(s.Exemplars)
	}
	if len(got) != 8 || total != 8 {
		t.Errorf("exemplars: %d series, %d exemplars, want 8 and 8", len(got), total)
	}
}

// clusterConn connects to the shard at CLICKHOUSE_HTTP_URL.
func clusterConn(t *testing.T) clickhouse.Conn {
	t.Helper()
	u, err := url.Parse(shards(t)[0])
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := u.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{u.Host}, Protocol: clickhouse.HTTP,
		Auth:        clickhouse.Auth{Database: clickhouseDB(), Username: u.User.Username(), Password: pass},
		ReadTimeout: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestClusterImportCopiesEachSeriesOntoItsShardOnce(t *testing.T) {
	cluster := clusterName(t)
	waitReady(t)
	eventually(t, "SELECT count() > 0 FROM settings_dist WHERE type = 'update' AND name = 'metric_import'", "1")
	t0, err := strconv.ParseInt(clickhouseQuery(t,
		"SELECT argMax(value, inserted_at) FROM settings_dist WHERE type = 'update' AND name = 'metric_stack'"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	h0 := time.Unix(t0, 0).UTC().Truncate(time.Hour).Add(-time.Hour)
	from := h0.Add(-5 * time.Hour)

	stamp := time.Now().UnixNano()
	prefix := fmt.Sprintf("it_cluster_import_%d_", stamp)
	const n, perSeries = 12, 30
	var fps []uint64
	var series, rows []string
	for i := range n {
		fp := uint64(stamp)&^0xff | uint64(i) | 1<<63
		fps = append(fps, fp)
		name := fmt.Sprintf("%s%d", prefix, i)
		series = append(series, fmt.Sprintf(`('%s', %d, '{"__name__":"%s","job":"import"}', '%s', 2, '', 1)`,
			from.Format(time.DateOnly), fp, name, name))
		for k := range perSeries {
			rows = append(rows, fmt.Sprintf("(%d, %d, %d, '', 2)", fp, from.Add(time.Duration(k)*time.Minute).UnixNano(), k+1))
		}
	}
	sync := " SETTINGS insert_distributed_sync = 1"
	clickhouseQuery(t, "INSERT INTO time_series_dist (date, fingerprint, labels, name, type, metadata, updated_at_ns)"+
		sync+" VALUES "+strings.Join(series, ", "))
	clickhouseQuery(t, "INSERT INTO samples_v3_dist (fingerprint, timestamp_ns, value, string, type)"+sync+
		" VALUES "+strings.Join(rows, ", "))

	reset := func() {
		clickhouseQuery(t, fmt.Sprintf("ALTER TABLE settings ON CLUSTER `%s` DELETE WHERE type = 'metric_import' "+
			"OR (type = 'update' AND name = 'metric_import') SETTINGS mutations_sync = 2", cluster))
	}
	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		err := maintenance.ImportMetrics(ctx, clusterConn(t), maintenance.MetricImportOptions{
			Instance: "integration", Database: clickhouseDB(), Cluster: cluster, SamplesDays: 7, RollupDays: 30,
			Settle: 10 * time.Millisecond, Poll: 200 * time.Millisecond, Quiet: 2 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	check := func(stage string) {
		t.Helper()
		source := holders(t, "samples_v3", fps)
		for _, table := range []string{"metric_series", "metric_samples", "metrics_5m", "metrics_1h"} {
			got := holders(t, table, fps)
			for _, fp := range fps {
				if len(got[fp]) != 1 || !slices.Equal(got[fp], source[fp]) {
					t.Errorf("%s: %s of %d on shards %v, its samples_v3 rows on %v", stage, table, fp, got[fp], source[fp])
				}
			}
		}
		perShard := map[int]bool{}
		for _, fp := range fps {
			perShard[source[fp][0]] = true
		}
		if len(perShard) != 2 {
			t.Errorf("every imported series is on one shard")
		}
		want := strings.Repeat(fmt.Sprintf("%d\t%d\t%d\n", perSeries, perSeries, perSeries), n)
		var lines []string
		for _, fp := range fps {
			base := shards(t)[source[fp][0]]
			lines = append(lines, clickhouseQueryAt(t, base, fmt.Sprintf("SELECT "+
				"(SELECT count() FROM metric_samples FINAL WHERE fingerprint = %[1]d), "+
				"(SELECT sum(count) FROM metrics_5m WHERE fingerprint = %[1]d), "+
				"(SELECT sum(count) FROM metrics_1h WHERE fingerprint = %[1]d)", fp)))
		}
		sort.Strings(lines)
		if g := strings.Join(lines, "\n") + "\n"; g != want {
			t.Errorf("%s: raw, 5m and 1h counts per series:\n%s\nwant\n%s", stage, g, want)
		}
	}

	reset()
	run()
	check("import")

	// A chunk found started is redone: its tier buckets are deleted on every shard first.
	chunkTo := from.Truncate(24 * time.Hour).Add(24 * time.Hour)
	if from.After(h0.Truncate(24 * time.Hour)) {
		chunkTo = h0
	}
	chunk := fmt.Sprintf("chunk:%d", chunkTo.UnixMilli())
	if got := clickhouseQuery(t, fmt.Sprintf("SELECT argMax(value, inserted_at) FROM settings_dist "+
		"WHERE type = 'metric_import' AND name = '%s'", chunk)); got != "done" {
		t.Fatalf("%s recorded %q, want done", chunk, got)
	}
	clickhouseQuery(t, "ALTER TABLE settings ON CLUSTER `"+cluster+"` DELETE WHERE type = 'update' AND name = 'metric_import' "+
		"SETTINGS mutations_sync = 2")
	clickhouseQuery(t, fmt.Sprintf("INSERT INTO settings_dist (fingerprint, type, name, value, inserted_at) "+
		"SELECT fingerprint, type, name, 'started', now64(9) FROM settings_dist WHERE type = 'metric_import' AND name = '%s' "+
		"LIMIT 1 SETTINGS insert_distributed_sync = 1", chunk))
	run()
	check("redo")
}
