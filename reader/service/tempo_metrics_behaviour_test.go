package service

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/tempo"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/shared/chtest"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// updateMetricsGolden rewrites metricsGoldenFile from the results of this run.
var updateMetricsGolden = flag.Bool("update-metrics-golden", false,
	"rewrite "+metricsGoldenFile+"; run it on the commit whose results the file pins")

// metricsGoldenFile holds the expected metrics and compare() results, as
// sorted series lines, by step, endpoint and query.
const metricsGoldenFile = "testdata/metrics_golden.json"

var metricsGoldenMu sync.Mutex

// metricsGoldenRun holds the results written under -update-metrics-golden in
// this run, by key.
var metricsGoldenRun = map[string][]string{}

// metricsStore is a migrated database, single node or Distributed tables over
// the shards of a cluster, served by a TempoService.
type metricsStore struct {
	conn    clickhouse.Conn
	svc     *TempoService
	name    string
	cluster string
	golden  map[string][]string
}

// openMetricsStore migrates a database, on every shard of the
// chtest.ClusterEnv cluster when cluster is set, and serves it over HTTP when
// http is set.
func openMetricsStore(t *testing.T, cluster, http bool) *metricsStore {
	t.Helper()
	opts := &clickhouse.Options{Addr: []string{os.Getenv(chtest.AddrEnv)}}
	if http {
		addr := os.Getenv(chtest.HTTPAddrEnv)
		if addr == "" {
			t.Skipf("%s is not set", chtest.HTTPAddrEnv)
		}
		opts = &clickhouse.Options{Addr: []string{addr}, Protocol: clickhouse.HTTP}
	}
	s := &metricsStore{golden: map[string][]string{}}
	mode := maintenance.CLUST_MODE_SINGLE
	if cluster {
		s.conn, s.name, s.cluster = chtest.OpenCluster(t)
		mode |= maintenance.CLUST_MODE_DISTRIBUTED
	} else {
		s.conn, s.name = chtest.Open(t)
	}
	if err := maintenance.Update(s.conn, s.name, s.cluster, mode, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	distconfig.Init()
	opts.Auth.Database = s.name
	var opened []*sqlx.DB
	open := func() *sqlx.DB {
		d := sqlx.NewDb(clickhouse.OpenDB(opts), "clickhouse")
		opened = append(opened, d)
		return d
	}
	t.Cleanup(func() {
		for _, d := range opened {
			_ = d.Close()
		}
	})
	session := &dsn.StableSqlxDBWrapper{DB: open(), GetDB: open, Name: s.name}
	s.svc = &TempoService{ServiceData: model.ServiceData{Session: testRegistry{&model.DataDatabasesMap{
		Config: &config.ClokiBaseDataBase{Name: s.name, ClusterName: s.cluster}, Session: session,
	}}}}
	if b, err := os.ReadFile(metricsGoldenFile); err == nil {
		if err := json.Unmarshal(b, &s.golden); err != nil {
			t.Fatal(err)
		}
	} else if !*updateMetricsGolden {
		t.Fatal(err)
	}
	return s
}

func (s *metricsStore) exec(t *testing.T, query string) {
	t.Helper()
	if err := s.conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("%v\n%s", err, query)
	}
}

// metricsSpan is a seeded span: trace, id, parent (0 for a root), attributes
// "k" = "1" for each k in attrs, and its numbering, if any, as left/right/parent.
type metricsSpan struct {
	trace, id, parent int
	attrs             []string
	ns                []int
}

// metricsSpanStart is the start of the i-th seeded span: one per second
// from metricsStart.
func metricsSpanStart(i int) int64 {
	return metricsStart.Add(time.Duration(i) * time.Second).UnixNano()
}

// seed inserts spans, with name "s<id>" and duration id ms. On a cluster each
// span goes to the shard of its id's parity, so a trace's spans sit on two
// shards.
func (s *metricsStore) seed(t *testing.T, spans []metricsSpan) {
	t.Helper()
	var rows, ns []string
	for i, sp := range spans {
		parent := ""
		if sp.parent != 0 {
			parent = fmt.Sprintf("%016x", sp.parent)
		}
		tags := []string{fmt.Sprintf("('name', 's%d')", sp.id)}
		for _, a := range sp.attrs {
			tags = append(tags, fmt.Sprintf("('%s', '1')", a))
		}
		rows = append(rows, fmt.Sprintf("('%032x', '%016x', '%s', 's%d', %d, %d, [%s])",
			sp.trace, sp.id, parent, sp.id, metricsSpanStart(i), sp.id*1000000, strings.Join(tags, ", ")))
		if sp.ns != nil {
			ns = append(ns, fmt.Sprintf("(unhex('%032x'), unhex('%016x'), '%s', %d, %d, %d, 1)",
				sp.trace, sp.id, metricsStart.Format(time.DateOnly), sp.ns[0], sp.ns[1], sp.ns[2]))
		}
	}
	const cols = "(trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, tags)"
	nsTable := "tempo_traces_nested_set"
	if s.cluster == "" {
		s.exec(t, "INSERT INTO traces_input "+cols+" VALUES "+strings.Join(rows, ", "))
	} else {
		nsTable = "seed_nested_set_split"
		locals := map[string]string{
			"seed_traces_split":     "tempo_traces",
			"seed_attrs_split":      "tempo_traces_attrs_gin",
			"seed_nested_set_split": "tempo_traces_nested_set",
		}
		for table, local := range locals {
			s.exec(t, fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s AS %s_dist ENGINE = Distributed('%s', '%s', '%s', "+
					"reinterpretAsUInt8(substring(span_id, 8, 1)))",
				table, local, s.cluster, s.name, local))
		}
		s.exec(t, "CREATE TABLE IF NOT EXISTS seed_input AS traces_input ENGINE = Memory")
		s.exec(t, "TRUNCATE TABLE seed_input")
		s.exec(t, "INSERT INTO seed_input "+cols+" VALUES "+strings.Join(rows, ", "))
		s.exec(t, `INSERT INTO seed_traces_split (oid, trace_id, span_id, parent_id, name, timestamp_ns, duration_ns)
			SELECT oid, unhex(trace_id)::FixedString(16), unhex(span_id)::FixedString(8), unhex(parent_id), name,
			       timestamp_ns, duration_ns
			FROM seed_input`)
		s.exec(t, `INSERT INTO seed_attrs_split (oid, date, key, val, trace_id, span_id, timestamp_ns, duration)
			SELECT oid, toDate(intDiv(timestamp_ns, 1000000000)), tags.1, tags.2, unhex(trace_id)::FixedString(16),
			       unhex(span_id)::FixedString(8), timestamp_ns, duration_ns
			FROM seed_input ARRAY JOIN tags`)
	}
	if len(ns) > 0 {
		s.exec(t, "INSERT INTO "+nsTable+" (trace_id, span_id, date, left, right, parent, version) VALUES "+
			strings.Join(ns, ", "))
	}
	if s.cluster != "" {
		for table := range map[string]bool{"seed_traces_split": true, "seed_attrs_split": true, "seed_nested_set_split": true} {
			s.exec(t, "SYSTEM FLUSH DISTRIBUTED "+table)
		}
		var shards uint64
		if err := s.conn.QueryRow(context.Background(), `SELECT min(c) FROM
			(SELECT trace_id, uniqExact(_shard_num) c FROM tempo_traces_dist GROUP BY trace_id)`).Scan(&shards); err != nil {
			t.Fatal(err)
		}
		if shards < 2 {
			t.Fatalf("a trace's spans on %d shards, want 2", shards)
		}
	}
}

// metricsTraces are a numbered tree, a tree with a dangling subtree, a
// rootless trace, siblings, rooted traces with chains under missing parents
// and one-sided traces.
func metricsTraces() []metricsSpan {
	var spans []metricsSpan
	// 10: a numbered tree of 7 spans.
	nested := map[int][]int{1: {1, 14, -1}, 2: {2, 7, 1}, 3: {8, 13, 1}, 4: {3, 4, 2}, 5: {5, 6, 2}, 6: {9, 10, 3}, 7: {11, 12, 3}}
	for i := 1; i <= 7; i++ {
		sp := metricsSpan{trace: 10, id: i, parent: i / 2, ns: nested[i]}
		switch i {
		case 1, 3:
			sp.attrs = []string{"a"}
		case 6:
			sp.attrs = []string{"a", "b"}
		case 2, 7:
			sp.attrs = []string{"b"}
		}
		spans = append(spans, sp)
	}
	spans = append(spans,
		// 16: a root and a subtree under a missing parent.
		metricsSpan{16, 1, 0, []string{"a"}, nil}, metricsSpan{16, 2, 1, []string{"b"}, nil},
		metricsSpan{16, 3, 99, []string{"a"}, nil}, metricsSpan{16, 4, 3, []string{"b"}, nil},
		metricsSpan{16, 5, 4, []string{"b"}, nil},
		// 12: no root.
		metricsSpan{12, 1, 77, []string{"a"}, nil}, metricsSpan{12, 2, 1, []string{"b"}, nil},
		// 18: siblings.
		metricsSpan{18, 1, 0, nil, nil}, metricsSpan{18, 2, 1, []string{"a"}, nil},
		metricsSpan{18, 3, 1, []string{"b"}, nil}, metricsSpan{18, 4, 1, []string{"b"}, nil},
		metricsSpan{18, 5, 2, []string{"b"}, nil})
	// 33: a root with children, and a chain of 8 under a missing parent.
	spans = append(spans, metricsChain(33, 8, 99)...)
	spans = append(spans,
		metricsSpan{33, 20, 0, []string{"a"}, nil}, metricsSpan{33, 21, 20, []string{"b"}, nil},
		metricsSpan{33, 22, 20, []string{"a"}, nil}, metricsSpan{33, 23, 20, []string{"b"}, nil})
	// 34: a rooted chain of 9 and a span under a missing parent.
	spans = append(spans, metricsChain(34, 9, 0)...)
	spans = append(spans, metricsSpan{34, 20, 98, []string{"a"}, nil})
	// 40: a root, siblings under the root and under a missing parent, and a
	// chain 13 deep below the missing parent.
	spans = append(spans,
		metricsSpan{40, 1, 0, []string{"a"}, nil}, metricsSpan{40, 20, 1, []string{"b"}, nil},
		metricsSpan{40, 21, 1, []string{"a"}, nil}, metricsSpan{40, 2, 50, []string{"a"}, nil},
		metricsSpan{40, 3, 50, []string{"b"}, nil})
	prev := 3
	for _, id := range []int{4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16} {
		sp := metricsSpan{trace: 40, id: id, parent: prev}
		switch id {
		case 8, 13:
			sp.attrs = []string{"a"}
		case 4, 9, 14, 16:
			sp.attrs = []string{"b"}
		}
		spans = append(spans, sp)
		prev = id
	}
	return append(spans,
		// 41: only b spans, one under a missing parent.
		metricsSpan{41, 1, 0, []string{"b"}, nil}, metricsSpan{41, 2, 1, []string{"b"}, nil},
		metricsSpan{41, 3, 99, []string{"b"}, nil}, metricsSpan{41, 4, 3, []string{"b"}, nil},
		// 42: only a spans.
		metricsSpan{42, 1, 0, []string{"a"}, nil}, metricsSpan{42, 2, 1, []string{"a"}, nil},
		metricsSpan{42, 3, 1, nil, nil})
}

// metricsChain is a chain of n spans under top (0 for a root): an a span on
// top, a b span at the bottom.
func metricsChain(trace, n, top int) []metricsSpan {
	var res []metricsSpan
	for i := 1; i <= n; i++ {
		sp := metricsSpan{trace: trace, id: i, parent: i - 1}
		if i == 1 {
			sp.parent = top
		}
		switch i {
		case 1:
			sp.attrs = []string{"a"}
		case n:
			sp.attrs = []string{"b"}
		}
		res = append(res, sp)
	}
	return res
}

// metricsStructuralOps are the structural operators.
var metricsStructuralOps = []string{">>", "<<", "!>>", "!<<", "&>>", "&<<", ">", "<", "~", "!>", "!<", "!~", "&>", "&<", "&~"}

// metricsQueries count, aggregate and group the spans of plain, structural
// and empty spanset expressions.
func metricsQueries() []string {
	res := []string{
		`{} | rate()`,
		`{} | count_over_time() by (name)`,
		`{.a = "1"} | count_over_time()`,
		`{.a = "1"} || {.b = "1"} | count_over_time() by (name)`,
		`{.b = "1"} | count() > 1 | count_over_time()`,
		`{.c = "1"} | rate()`,
		`{.a = "1"} >> {.b = "1"} | rate()`,
		`{.a = "1"} >> {.b = "1"} | count_over_time() by (name)`,
		`{.a = "1"} >> {.b = "1"} | avg_over_time(duration)`,
		`{.a = "1"} >> {.b = "1"} | max_over_time(duration) by (name)`,
		`{.a = "1"} >> {.b = "1"} | quantile_over_time(duration, 0.5)`,
		`{.a = "1"} >> {.b = "1"} | histogram_over_time(duration)`,
		`{.c = "1"} >> {.b = "1"} | rate()`,
		`{} | count_over_time() by (nestedSetLeft)`,
		`{.b = "1"} | max_over_time(nestedSetRight) by (nestedSetParent, name)`,
		`{.a = "1"} >> {.b = "1"} | avg_over_time(nestedSetRight) by (nestedSetLeft)`,
		`{.a = "1"} >> {} | count_over_time() with(sample=0.5)`,
		`{.a = "1"} && {.b = "1"} | rate() with(sample=0.5)`,
		`{.a = "1"} >> {.b = "1"} | count_over_time() by (name) with(sample=0.3)`,
		`{.a = "1"} | count_over_time() with(sample=0.7)`,
	}
	for _, op := range metricsStructuralOps {
		res = append(res,
			fmt.Sprintf(`{.a = "1"} %s {.b = "1"} | count_over_time()`, op),
			fmt.Sprintf(`{.b = "1"} %s {} | count_over_time() by (name)`, op))
	}
	return res
}

// compareQueries compare a selection with a baseline: structural, plain,
// empty on either side, without a selection and over an explicit baseline
// window.
func compareQueries() []string {
	from, to := metricsStart.UnixNano(), metricsStart.Add(10*time.Second).UnixNano()
	return []string{
		`{.a = "1"} >> {.b = "1"} | compare({.a = "1"})`,
		`{} | compare({.b = "1"})`,
		`{.a = "1"} | compare({})`,
		`{.c = "1"} | compare({.b = "1"})`,
		`{.a = "1"} | compare({.c = "1"})`,
		`{.c = "1"} | compare({.c = "1"})`,
		`{.b = "1"} &<< {.a = "1"} | compare({name = "s2"}, 3)`,
		fmt.Sprintf(`{} | compare({.a = "1"}, 10, %d, %d)`, from, to),
		fmt.Sprintf(`{.a = "1"} >> {} | compare({.b = "1"}, 10, %d, %d)`, from, to),
		`{.a = "1"} >> {.b = "1"} | compare({.a = "1"}) with(sample=0.5)`,
		`{.a = "1"} >> {.b = "1"} | compare({.a = "1"}) with(sample=0.3)`,
	}
}

// sampledQueries pairs sampled queries with the same queries unsampled. The
// sample drops traces 16, 18, 34, 40, 41 and 42 by trace id, or spans 2, 3, 7,
// 11, 13 and 15 by span id.
var sampledQueries = map[string]string{
	`{.a = "1"} >> {.b = "1"} | count_over_time() by (name) with(sample=0.3)`: `{.a = "1"} >> {.b = "1"} | count_over_time() by (name)`,
	`{.a = "1"} | count_over_time() with(sample=0.7)`:                         `{.a = "1"} | count_over_time()`,
	`{.a = "1"} >> {.b = "1"} | compare({.a = "1"}) with(sample=0.3)`:         `{.a = "1"} >> {.b = "1"} | compare({.a = "1"})`,
}

// exemplarQueries are the queries whose exemplars are checked.
var exemplarQueries = []string{
	`{} | rate()`,
	`{.a = "1"} >> {.b = "1"} | rate()`,
	`{.a = "1"} >> {.b = "1"} | count_over_time() by (name)`,
	`{.a = "1"} >> {.b = "1"} | avg_over_time(duration)`,
	`{.a = "1"} >> {.b = "1"} | histogram_over_time(duration)`,
	`{.a = "1"} >> {.b = "1"} | avg_over_time(nestedSetRight) by (nestedSetLeft)`,
	`{.a = "1"} >> {.b = "1"} | count_over_time() by (name) with(sample=0.3)`,
}

// metricsRequestRange is the request of query over the seeded spans in 30 s steps.
func metricsRequestRange(query string) *model.MetricsQueryRequest {
	return &model.MetricsQueryRequest{
		Query: query,
		From:  metricsStart.Add(-30 * time.Second),
		To:    metricsStart.Add(90 * time.Second),
		Step:  30 * time.Second,
	}
}

// labelsLine is labels in protobuf JSON.
func labelsLine(t *testing.T, labels []model.MetricsKeyValue) string {
	t.Helper()
	js, err := json.Marshal(labels)
	if err != nil {
		t.Fatal(err)
	}
	return string(js)
}

// rangeResult runs query on the range endpoint: a sorted line per series with
// its labels, samples and exemplar count, or the error.
func (s *metricsStore) rangeResult(t *testing.T, query string) []string {
	t.Helper()
	res, err := s.svc.MetricsQueryRange(context.Background(), metricsRequestRange(query))
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	lines := []string{}
	for _, series := range res.Series {
		var samples []string
		for _, p := range series.Samples {
			samples = append(samples, fmt.Sprintf("%s=%v", p.TimestampMs, p.Value))
		}
		lines = append(lines, fmt.Sprintf("%s %s exemplars=%d",
			labelsLine(t, series.Labels), strings.Join(samples, ","), len(series.Exemplars)))
	}
	sort.Strings(lines)
	return lines
}

// instantResult runs query on the instant endpoint: a sorted line per series
// with its labels and value, or the error.
func (s *metricsStore) instantResult(t *testing.T, query string) []string {
	t.Helper()
	res, err := s.svc.MetricsQueryInstant(context.Background(), metricsRequestRange(query))
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	lines := []string{}
	for _, series := range res.Series {
		lines = append(lines, fmt.Sprintf("%s %v", labelsLine(t, series.Labels), series.Value))
	}
	sort.Strings(lines)
	return lines
}

// exemplarResult runs query on the range endpoint in 1 s steps, which hold
// one seeded span each: a sorted line per series with its labels and its
// exemplars' trace id, timestamp and value.
func (s *metricsStore) exemplarResult(t *testing.T, query string) []string {
	t.Helper()
	req := metricsRequestRange(query)
	req.Step = time.Second
	res, err := s.svc.MetricsQueryRange(context.Background(), req)
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	lines := []string{}
	for _, series := range res.Series {
		exemplars := []string{}
		for _, e := range series.Exemplars {
			ex, ok := e.(tempo.HistogramExemplar)
			if !ok {
				t.Fatalf("exemplar %T", e)
			}
			traceID := ""
			for _, l := range ex.Labels {
				if l.Key == "trace:id" {
					traceID = l.Value.StringValue
				}
			}
			exemplars = append(exemplars, fmt.Sprintf("%s@%s=%v", traceID, ex.TimestampMs, ex.Value))
		}
		sort.Strings(exemplars)
		lines = append(lines, fmt.Sprintf("%s %s", labelsLine(t, series.Labels), strings.Join(exemplars, ",")))
	}
	sort.Strings(lines)
	return lines
}

// assertSampleDrops checks that each of sampledQueries returns other results
// than its unsampled query.
func (s *metricsStore) assertSampleDrops(t *testing.T) {
	t.Helper()
	for sampled, unsampled := range sampledQueries {
		for _, got := range []func(*testing.T, string) []string{s.rangeResult, s.instantResult} {
			if reflect.DeepEqual(got(t, sampled), got(t, unsampled)) {
				t.Errorf("%s: same results as %s", sampled, unsampled)
			}
		}
	}
}

// assertResults checks queries on both endpoints, or exemplars on the range
// endpoint under step "exemplars", against the golden results under step, or
// records them under -update-metrics-golden.
func (s *metricsStore) assertResults(t *testing.T, step string, queries []string) {
	t.Helper()
	endpoints := map[string]func(*testing.T, string) []string{"range": s.rangeResult, "instant": s.instantResult}
	if step == "exemplars" {
		endpoints = map[string]func(*testing.T, string) []string{"range": s.exemplarResult}
	}
	for _, query := range queries {
		for endpoint, got := range endpoints {
			key := step + " | " + endpoint + " | " + query
			lines := got(t, query)
			for _, l := range lines {
				if strings.HasPrefix(l, "error: ") {
					t.Errorf("%s: %s", key, l)
				}
			}
			if *updateMetricsGolden {
				s.golden[key] = lines
				continue
			}
			want, ok := s.golden[key]
			if !ok {
				t.Errorf("%s: no golden result", key)
				continue
			}
			if !reflect.DeepEqual(lines, want) {
				t.Errorf("%s:\ngot  %s\nwant %s", key, strings.Join(lines, "\n     "), strings.Join(want, "\n     "))
			}
		}
	}
}

// writeGolden merges s's results into metricsGoldenFile under
// -update-metrics-golden. A key written earlier in the run must get the same
// results.
func (s *metricsStore) writeGolden(t *testing.T) {
	t.Helper()
	if !*updateMetricsGolden {
		return
	}
	metricsGoldenMu.Lock()
	defer metricsGoldenMu.Unlock()
	all := map[string][]string{}
	if b, err := os.ReadFile(metricsGoldenFile); err == nil {
		if err := json.Unmarshal(b, &all); err != nil {
			t.Fatal(err)
		}
	}
	for k, v := range s.golden {
		if prev, ok := metricsGoldenRun[k]; ok && !reflect.DeepEqual(prev, v) {
			t.Errorf("%s: results differ from an earlier test in this run:\ngot  %s\nwant %s",
				k, strings.Join(v, "\n     "), strings.Join(prev, "\n     "))
		}
		metricsGoldenRun[k] = v
		all[k] = v
	}
	b, err := json.MarshalIndent(all, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(metricsGoldenFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metricsGoldenFile, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func testMetricsAndCompare(t *testing.T, cluster bool) {
	s := openMetricsStore(t, cluster, false)
	defer s.writeGolden(t)
	s.seed(t, metricsTraces())
	s.assertResults(t, "metrics", metricsQueries())
	s.assertResults(t, "compare", compareQueries())
	s.assertResults(t, "exemplars", exemplarQueries)
	s.assertSampleDrops(t)
}

// testMetricsAndCompareOverHTTP runs some of testMetricsAndCompare's queries
// over HTTP.
func testMetricsAndCompareOverHTTP(t *testing.T, cluster bool) {
	s := openMetricsStore(t, cluster, true)
	s.seed(t, metricsTraces())
	s.assertResults(t, "metrics", []string{
		`{} | rate()`,
		`{.a = "1"} >> {.b = "1"} | count_over_time() by (name)`,
		`{.a = "1"} >> {.b = "1"} | avg_over_time(nestedSetRight) by (nestedSetLeft)`,
		`{.a = "1"} >> {.b = "1"} | count_over_time() by (name) with(sample=0.3)`,
	})
	s.assertResults(t, "compare", compareQueries()[:2])
	s.assertResults(t, "exemplars", exemplarQueries[:2])
}

func testMetricsRoundLimit(t *testing.T, cluster bool) {
	s := openMetricsStore(t, cluster, false)
	defer s.writeGolden(t)
	t.Cleanup(func() { _ = traceqlconfig.Init() })
	t.Setenv(traceqlconfig.AncestryRoundsEnv, "3")
	if err := traceqlconfig.Init(); err != nil {
		t.Fatal(err)
	}
	oneHop := []string{
		`{.a = "1"} > {.b = "1"} | count_over_time()`,
		`{.a = "1"} ~ {.b = "1"} | rate()`,
		`{.a = "1"} !> {.b = "1"} | count_over_time() by (name)`,
		`{.a = "1"} > {.b = "1"} | compare({.a = "1"})`,
	}
	queries := append([]string{
		`{.a = "1"} >> {.b = "1"} | count_over_time()`,
		`{.b = "1"} &<< {.a = "1"} | count_over_time() by (name)`,
		`{.a = "1"} >> {.b = "1"} | compare({.a = "1"})`,
	}, oneHop...)
	// 30: a chain of 8 under a missing parent; 32: a rooted chain of 8; 33: a
	// root with children, and a chain of 8 under a missing parent.
	spans := append(metricsChain(30, 8, 99), metricsChain(32, 8, 0)...)
	spans = append(spans, metricsChain(33, 8, 99)...)
	spans = append(spans,
		metricsSpan{33, 20, 0, []string{"a"}, nil}, metricsSpan{33, 21, 20, []string{"b"}, nil},
		metricsSpan{33, 22, 20, []string{"a"}, nil}, metricsSpan{33, 23, 20, []string{"b"}, nil})
	s.seed(t, spans)
	s.assertResults(t, "rounds-resolved", queries)
	// 34: a rooted chain of 9 and a span under a missing parent.
	s.seed(t, append(metricsChain(34, 9, 0), metricsSpan{34, 20, 98, []string{"a"}, nil}))
	msg := "round limit of 3 rounds"
	for _, query := range oneHop {
		for endpoint, got := range map[string]func(*testing.T, string) []string{"range": s.rangeResult, "instant": s.instantResult} {
			if lines := got(t, query); len(lines) != 1 || !strings.HasPrefix(lines[0], "error: ") || !strings.Contains(lines[0], msg) {
				t.Errorf("%s %s: got %q, want the unresolved-ancestry error", endpoint, query, lines)
			}
		}
	}
}

// At 3 rounds, metrics and compare() resolve chains of 8 spans, also under a
// missing parent of a rooted trace, and fail on a rooted chain of 9 with a
// span under a missing parent.
func TestMetricsRoundLimit(t *testing.T) { testMetricsRoundLimit(t, false) }

func TestMetricsRoundLimitOnCluster(t *testing.T) { testMetricsRoundLimit(t, true) }

// Metrics and compare() return the golden series on numbered, dangling,
// rootless, sibling and one-sided traces, on the range and instant endpoints.
// On a cluster each trace's spans sit on two shards.
func TestMetricsAndCompare(t *testing.T) { testMetricsAndCompare(t, false) }

func TestMetricsAndCompareOnCluster(t *testing.T) { testMetricsAndCompare(t, true) }

func TestMetricsAndCompareOverHTTP(t *testing.T) { testMetricsAndCompareOverHTTP(t, false) }

func TestMetricsAndCompareOverHTTPOnCluster(t *testing.T) { testMetricsAndCompareOverHTTP(t, true) }

// queryRecorder records the statements a session runs.
type queryRecorder struct {
	model.ISqlxDB
	mu      sync.Mutex
	queries []string
}

func (r *queryRecorder) QueryCtx(ctx context.Context, query string, args ...any) (*stdsql.Rows, error) {
	r.mu.Lock()
	r.queries = append(r.queries, query)
	r.mu.Unlock()
	return r.ISqlxDB.QueryCtx(ctx, query, args...)
}

// take returns the recorded statements that list span ids and forgets all.
func (r *queryRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ids []string
	for _, q := range r.queries {
		if strings.Contains(q, "groupArray(span_id)") {
			ids = append(ids, q)
		}
	}
	r.queries = nil
	return ids
}

// Each ids statement runs once per request, also when the series and their
// exemplars both read its ids.
func TestMetricsRunEachIdsStatementOncePerRequest(t *testing.T) {
	s := openMetricsStore(t, false, false)
	s.seed(t, metricsTraces())
	db, err := s.svc.Session.GetDB(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rec := &queryRecorder{ISqlxDB: db.Session}
	db.Session = rec

	check := func(endpoint, query string, want int) {
		t.Helper()
		ids := rec.take()
		if len(ids) != want {
			t.Errorf("%s %s: %d ids statements, want %d", endpoint, query, len(ids), want)
		}
		for i := range ids {
			for _, other := range ids[:i] {
				if ids[i] == other {
					t.Errorf("%s %s: ids statement ran twice:\n%s", endpoint, query, ids[i])
				}
			}
		}
	}
	for _, query := range exemplarQueries {
		s.exemplarResult(t, query)
		check("range", query, 1)
	}
	for _, query := range metricsQueries() {
		want := 1
		if strings.Contains(query, "histogram_over_time") {
			want = 0
		}
		s.instantResult(t, query)
		check("instant", query, want)
	}
	for _, query := range compareQueries() {
		want := 2
		if strings.Contains(query, "compare({})") {
			want = 1
		}
		s.rangeResult(t, query)
		check("range", query, want)
		s.instantResult(t, query)
		check("instant", query, want)
	}
}
