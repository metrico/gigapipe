package traceql_transpiler

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/chtest"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// updateGolden rewrites searchGoldenFile from the results of this run.
var updateGolden = flag.Bool("update-search-golden", false,
	"rewrite "+searchGoldenFile+"; run it on the commit whose results the file pins")

// searchGoldenFile holds the expected search results, as summary lines, by
// test, step, query and ancestry rounds.
const searchGoldenFile = "testdata/search_golden.json"

// searchLimit is the trace limit of a search that the limit does not cut.
const searchLimit = 50

// searchStore is a migrated database: single node, or Distributed tables
// over the shards of a cluster.
type searchStore struct {
	conn    clickhouse.Conn
	db      model.ISqlxDB
	name    string
	cluster string
	golden  map[string][]string
	// skipUnavailable is the reader's SkipUnavailableShards.
	skipUnavailable bool
	// user is the ClickHouse user reading s, when it is not the admin.
	user string
}

var goldenMu sync.Mutex

// openSearchStore migrates a database, on every shard of the
// chtest.ClusterEnv cluster when cluster is set. Searches read it over HTTP
// when http is set.
func openSearchStore(t *testing.T, cluster, http bool) *searchStore {
	t.Helper()
	opts := &clickhouse.Options{Addr: []string{os.Getenv(chtest.AddrEnv)}}
	if http {
		addr := os.Getenv(chtest.HTTPAddrEnv)
		if addr == "" {
			t.Skipf("%s is not set", chtest.HTTPAddrEnv)
		}
		opts = &clickhouse.Options{Addr: []string{addr}, Protocol: clickhouse.HTTP}
	}
	s := &searchStore{golden: map[string][]string{}}
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
	s.db = &dsn.StableSqlxDBWrapper{DB: open(), GetDB: open}
	if b, err := os.ReadFile(searchGoldenFile); err == nil {
		if err := json.Unmarshal(b, &s.golden); err != nil {
			t.Fatal(err)
		}
	} else if !*updateGolden {
		t.Fatal(err)
	}
	return s
}

// span is a seeded span: trace, id, parent (0 for a root) and attributes.
type span struct {
	trace, id, parent int
	attrs             []string
}

// seed inserts spans, attribute "k" = "1" for each k in attrs. On a cluster
// the spans go through Distributed tables, sharded by trace.
func (s *searchStore) seed(t *testing.T, spans []span) {
	t.Helper()
	s.seedStarts(t, spans, func(i int) int64 { return spanStart.UnixNano() + int64(i) })
}

// seedStarts inserts spans, the i-th starting at start(i), attribute "k" = "1"
// for each k in attrs. On a cluster the spans go through Distributed tables,
// sharded by trace.
func (s *searchStore) seedStarts(t *testing.T, spans []span, start func(i int) int64) {
	t.Helper()
	var rows []string
	for i, sp := range spans {
		parent := ""
		if sp.parent != 0 {
			parent = fmt.Sprintf("%016x", sp.parent)
		}
		tags := []string{fmt.Sprintf("('name', 's%d')", sp.id)}
		for _, a := range sp.attrs {
			tags = append(tags, fmt.Sprintf("('%s', '1')", a))
		}
		rows = append(rows, fmt.Sprintf("('%032x', '%016x', '%s', 's%d', %d, 10, [%s])",
			sp.trace, sp.id, parent, sp.id, start(i), strings.Join(tags, ", ")))
	}
	const cols = "(trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, tags)"
	if s.cluster == "" {
		execSQL(t, s.conn, "INSERT INTO traces_input "+cols+" VALUES "+strings.Join(rows, ", "))
		return
	}
	traces, attrs := "tempo_traces_dist", "tempo_traces_attrs_gin_dist"
	execSQL(t, s.conn, "CREATE TABLE IF NOT EXISTS seed_input AS traces_input ENGINE = Memory")
	execSQL(t, s.conn, "TRUNCATE TABLE seed_input")
	execSQL(t, s.conn, "INSERT INTO seed_input "+cols+" VALUES "+strings.Join(rows, ", "))
	execSQL(t, s.conn, `INSERT INTO `+traces+` (oid, trace_id, span_id, parent_id, name, timestamp_ns, duration_ns)
		SELECT oid, unhex(trace_id)::FixedString(16), unhex(span_id)::FixedString(8), unhex(parent_id), name,
		       timestamp_ns, duration_ns
		FROM seed_input`)
	execSQL(t, s.conn, `INSERT INTO `+attrs+` (oid, date, key, val, trace_id, span_id, timestamp_ns, duration)
		SELECT oid, toDate(intDiv(timestamp_ns, 1000000000)), tags.1, tags.2, unhex(trace_id)::FixedString(16),
		       unhex(span_id)::FixedString(8), timestamp_ns, duration_ns
		FROM seed_input ARRAY JOIN tags`)
	execSQL(t, s.conn, "SYSTEM FLUSH DISTRIBUTED "+traces)
	execSQL(t, s.conn, "SYSTEM FLUSH DISTRIBUTED "+attrs)
}

// searchRun is how a search runs: ancestry rounds, trace limit, and the
// number of complexity portions (0 for the complexity evaluator's choice).
type searchRun struct {
	rounds, limit, portions int
}

// result runs query: the summary of its traces, or the error.
func (s *searchStore) result(t *testing.T, query string, run searchRun) []string {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &shared.PlannerContext{
		Ctx: context.Background(), CHDb: s.db, IsCluster: s.cluster != "",
		From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Minute),
		Limit: int64(run.limit), SpansPerSpanSet: 100, AncestryRounds: run.rounds,
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	distconfig.Init()
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
		Name: s.name, ClusterName: s.cluster, SkipUnavailableShards: s.skipUnavailable,
	}})
	var ch chan []model.TraceInfo
	if run.portions == 0 {
		ch, err = planner.Process(ctx)
	} else {
		portioned := planner.(interface {
			ProcessComplexReq(*shared.PlannerContext, int64) (chan []model.TraceInfo, error)
		})
		ch, err = portioned.ProcessComplexReq(ctx, int64(run.portions)*traceqlconfig.PortionSpans())
	}
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	var res []model.TraceInfo
	for traces := range ch {
		res = append(res, traces...)
	}
	return summary(t, res)
}

// summary is the golden form of traces: a line per trace with its id, start
// after spanStart, duration and root, then a line per spanset.
func summary(t *testing.T, traces []model.TraceInfo) []string {
	t.Helper()
	res := []string{}
	trim := func(id string) string { return strings.TrimLeft(id, "0") }
	for _, tr := range traces {
		start, err := strconv.ParseInt(tr.StartTimeUnixNano, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, fmt.Sprintf("trace %s start=+%d ms=%d root=%s/%s",
			trim(tr.TraceID), start-spanStart.UnixNano(), tr.DurationMs, tr.RootServiceName, tr.RootTraceName))
		if len(tr.SpanSets) > 0 && !reflect.DeepEqual(tr.SpanSet, tr.SpanSets[0]) {
			t.Errorf("trace %s: spanSet is not the first of spanSets", tr.TraceID)
		}
		for _, set := range tr.SpanSets {
			line := []string{fmt.Sprintf("  set matched=%d", set.Matched)}
			for _, a := range set.Attributes {
				line = append(line, fmt.Sprintf("%s=%s", a.Key, anyValue(a.Value)))
			}
			for _, sp := range set.Spans {
				start, err := strconv.ParseInt(sp.StartTimeUnixNano, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				item := fmt.Sprintf("span %s@+%d/%s", trim(sp.SpanID), start-spanStart.UnixNano(), sp.DurationNanos)
				if sp.Name != "" {
					item += " name=" + sp.Name
				}
				for _, a := range sp.Attributes {
					v := model.AnyValue{StringValue: a.Value.StringValue, IntValue: a.Value.IntValue}
					item += fmt.Sprintf(" %s=%s", a.Key, anyValue(v))
				}
				line = append(line, "["+item+"]")
			}
			res = append(res, strings.Join(line, " "))
		}
	}
	return res
}

// anyValue is v's set field: a quoted string, or a typed number or bool.
func anyValue(v model.AnyValue) string {
	switch {
	case v.StringValue != nil:
		return strconv.Quote(*v.StringValue)
	case v.IntValue != nil:
		return "int:" + *v.IntValue
	case v.DoubleValue != nil:
		return fmt.Sprintf("double:%v", float64(*v.DoubleValue))
	case v.BoolValue != nil:
		return fmt.Sprintf("bool:%t", *v.BoolValue)
	}
	return "none"
}

// assertResults runs each query, which must not fail, and compares it with the
// golden result of step. A search whose limit is below
// searchLimit must return limit traces.
func (s *searchStore) assertResults(t *testing.T, step string, queries []string, run searchRun) {
	t.Helper()
	if run.limit == 0 {
		run.limit = searchLimit
	}
	test := strings.SplitN(t.Name(), "/", 2)[0]
	test = strings.TrimSuffix(strings.TrimSuffix(test, "OnCluster"), "OverHTTP")
	for _, query := range queries {
		key := fmt.Sprintf("%s/%s/%s/%d", test, step, query, run.rounds)
		got := s.result(t, query, run)
		isErr := len(got) == 1 && strings.HasPrefix(got[0], "error: ")
		if isErr {
			t.Errorf("%s: got %.300q", key, got)
		}
		if traces := countTraces(got); !isErr && run.limit < searchLimit && traces != run.limit {
			t.Errorf("%s: %d traces, want the limit %d", key, traces, run.limit)
		}
		if *updateGolden {
			goldenMu.Lock()
			s.golden[key] = got
			goldenMu.Unlock()
			continue
		}
		if want, ok := s.golden[key]; !ok {
			t.Errorf("%s: no golden result", key)
		} else if !slices.Equal(got, want) {
			t.Errorf("%s:\ngot:  %q\nwant: %q", key, got, want)
		}
	}
}

// countTraces counts the traces of a summary.
func countTraces(lines []string) int {
	n := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "trace ") {
			n++
		}
	}
	return n
}

// writeGolden merges s's results into searchGoldenFile under -update-search-golden.
func (s *searchStore) writeGolden(t *testing.T) {
	t.Helper()
	if !*updateGolden {
		return
	}
	goldenMu.Lock()
	defer goldenMu.Unlock()
	all := map[string][]string{}
	if b, err := os.ReadFile(searchGoldenFile); err == nil {
		if err := json.Unmarshal(b, &all); err != nil {
			t.Fatal(err)
		}
	}
	for k, v := range s.golden {
		all[k] = v
	}
	b, err := json.MarshalIndent(all, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(searchGoldenFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(searchGoldenFile, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// structuralOps are the structural operators.
var structuralOps = []string{">>", "<<", "!>>", "!<<", "&>>", "&<<", ">", "<", "~", "!>", "!<", "!~", "&>", "&<", "&~"}

// structuralSearches relate .a and .b spans by every structural operator.
func structuralSearches() []string {
	var res []string
	for _, op := range structuralOps {
		res = append(res,
			fmt.Sprintf("{.a = \"1\"} %s {.b = \"1\"}", op),
			fmt.Sprintf("{.b = \"1\"} %s {.a = \"1\"}", op),
			fmt.Sprintf("{.a = \"1\"} %s {}", op),
			fmt.Sprintf("{.b = \"1\"} %s {.b = \"1\"}", op))
	}
	return append(res,
		`({nestedSetParent < 0 && .a = "1"} &>> {.b = "1"}) || ({nestedSetParent < 0 && .a = "1"}) | select(name, nestedSetParent)`,
		`{.a = "1"} >> {.b = "1"} >> {}`,
		`({.a = "1"} > {}) ~ {.b = "1"}`,
		`{.a = "1"} >> {.b = "1"} | select(.a)`)
}

// plainSearches are searches without structural operators.
var plainSearches = []string{
	`{}`,
	`{.a = "1"}`,
	`{.a = "1"} || {.b = "1"}`,
	`{.a = "1"} && {.b = "1"}`,
	`{.b = "1"} | count() > 1`,
	`{} | by(.a)`,
	`{.b = "1"} | by(.a) | select(name)`,
	`{.c = "1"}`,
}

// structuralTraces are well formed, dangling, rootless, deep, duplicated,
// sibling and one-sided traces.
func structuralTraces() []span {
	var spans []span
	// 10: a tree of 15 spans.
	for i := 1; i <= 15; i++ {
		s := span{trace: 10, id: i, parent: i / 2}
		switch i {
		case 1, 3:
			s.attrs = []string{"a"}
		case 6:
			s.attrs = []string{"a", "b"}
		case 2, 7, 12, 15:
			s.attrs = []string{"b"}
		}
		spans = append(spans, s)
	}
	spans = append(spans,
		// 16: a root and a subtree under a missing parent.
		span{16, 1, 0, []string{"a"}}, span{16, 2, 1, []string{"b"}},
		span{16, 3, 99, []string{"a"}}, span{16, 4, 3, []string{"b"}}, span{16, 5, 4, []string{"b"}},
		// 12: no root.
		span{12, 1, 77, []string{"a"}}, span{12, 2, 1, []string{"b"}},
		// 14: a span stored twice.
		span{14, 1, 0, []string{"a"}}, span{14, 2, 1, []string{"b"}}, span{14, 2, 1, []string{"b"}},
		// 18: siblings.
		span{18, 1, 0, nil}, span{18, 2, 1, []string{"a"}}, span{18, 3, 1, []string{"b"}}, span{18, 4, 1, []string{"b"}},
		span{18, 5, 2, []string{"b"}})
	// 17: a chain 40 deep.
	for i := 1; i <= 40; i++ {
		s := span{trace: 17, id: i, parent: i - 1}
		switch i {
		case 1, 21:
			s.attrs = []string{"a"}
		case 11, 40:
			s.attrs = []string{"b"}
		}
		spans = append(spans, s)
	}
	// 40: a root, siblings under the root and under a missing parent, and a
	// chain 13 deep below the missing parent.
	spans = append(spans,
		span{40, 1, 0, []string{"a"}}, span{40, 20, 1, []string{"b"}}, span{40, 21, 1, []string{"a"}},
		span{40, 2, 50, []string{"a"}}, span{40, 3, 50, []string{"b"}})
	prev := 3
	for _, id := range []int{4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16} {
		s := span{trace: 40, id: id, parent: prev}
		switch id {
		case 8, 13:
			s.attrs = []string{"a"}
		case 4, 9, 14, 16:
			s.attrs = []string{"b"}
		}
		spans = append(spans, s)
		prev = id
	}
	return append(spans,
		// 41: only b spans, one under a missing parent.
		span{41, 1, 0, []string{"b"}}, span{41, 2, 1, []string{"b"}},
		span{41, 3, 99, []string{"b"}}, span{41, 4, 3, []string{"b"}},
		// 42: only a spans.
		span{42, 1, 0, []string{"a"}}, span{42, 2, 1, []string{"a"}}, span{42, 3, 1, nil})
}

// limitSearches match more traces than the limits they run with.
var limitSearches = []string{
	`{}`,
	`{.a = "1"} >> {.b = "1"}`,
	`{.a = "1"} >> {.b = "1"} | select(.a)`,
}

// portionedSearches run in complexity portions.
var portionedSearches = []string{
	`{}`,
	`{.a = "1"} >> {.b = "1"} | select(.a)`,
	`{.b = "1"} | by(.a) | select(name)`,
	`({nestedSetParent < 0 && .a = "1"} &>> {.b = "1"}) || ({nestedSetParent < 0 && .a = "1"}) | select(name, nestedSetParent)`,
}

// seedStructural seeds structuralTraces; on a cluster each trace's spans sit
// on one shard, and the traces span every shard.
func (s *searchStore) seedStructural(t *testing.T) {
	t.Helper()
	s.seed(t, structuralTraces())
	if s.cluster == "" {
		return
	}
	var perTrace, shards uint64
	if err := s.conn.QueryRow(context.Background(), `SELECT max(c), uniqExact(shard) FROM
		(SELECT trace_id, uniqExact(_shard_num) c, any(_shard_num) shard FROM tempo_traces_dist GROUP BY trace_id)`).
		Scan(&perTrace, &shards); err != nil {
		t.Fatal(err)
	}
	if perTrace != 1 || shards < 2 {
		t.Fatalf("traces on %d shards with up to %d shards per trace, want 2 and 1", shards, perTrace)
	}
}

func testSearchStructuralOperators(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	defer s.writeGolden(t)
	s.seedStructural(t)
	s.assertResults(t, "plain", plainSearches, searchRun{})
	s.assertResults(t, "structural", structuralSearches(), searchRun{})
	// 6 rounds resolve 64 deep; 5 rounds leave the 40-deep chain unresolved.
	s.assertResults(t, "deep", []string{`{.a = "1"} >> {.b = "1"}`, `{.a = "1"} > {.b = "1"}`}, searchRun{rounds: 6})
	s.assertResults(t, "shallow", []string{`{.a = "1"} >> {.b = "1"}`, `{.b = "1"} &<< {.a = "1"}`}, searchRun{rounds: 5})
	for _, limit := range []int{2, 3} {
		s.assertResults(t, fmt.Sprintf("limit-%d", limit), limitSearches, searchRun{limit: limit})
	}
	for _, portions := range []int{2, 3} {
		s.assertResults(t, fmt.Sprintf("portions-%d", portions), portionedSearches,
			searchRun{portions: portions})
		s.assertResults(t, fmt.Sprintf("portions-%d-limit-3", portions), portionedSearches,
			searchRun{portions: portions, limit: 3})
	}
}

// testSearchOverHTTP runs some of testSearchStructuralOperators' searches over
// HTTP.
func testSearchOverHTTP(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, true)
	s.seedStructural(t)
	s.assertResults(t, "plain", []string{`{}`, `{.b = "1"} | by(.a) | select(name)`}, searchRun{})
	s.assertResults(t, "structural", []string{
		`{.a = "1"} >> {.b = "1"} | select(.a)`,
		`({nestedSetParent < 0 && .a = "1"} &>> {.b = "1"}) || ({nestedSetParent < 0 && .a = "1"}) | select(name, nestedSetParent)`,
	}, searchRun{})
	// 5 rounds leave the 40-deep chain unresolved.
	s.assertResults(t, "shallow", []string{`{.a = "1"} >> {.b = "1"}`}, searchRun{rounds: 5})
	s.assertResults(t, "limit-2", limitSearches, searchRun{limit: 2})
	s.assertResults(t, "portions-2", portionedSearches[:2], searchRun{portions: 2})
}

// repeatedOperandSearches pair a search whose `||` repeats an operand of its
// structural side with a spelling of it, `X && X` for X, that plans both sides.
func repeatedOperandSearches() [][2]string {
	const a, b = `{.a = "1"}`, `{.b = "1"}`
	both := func(x string) string { return "(" + x + " && " + x + ")" }
	var res [][2]string
	for _, op := range structuralOps {
		rel := fmt.Sprintf("(%s %s %s)", a, op, b)
		res = append(res,
			[2]string{rel + " || " + a, rel + " || " + both(a)},
			[2]string{a + " || " + rel, both(a) + " || " + rel},
			[2]string{rel + " || " + b, rel + " || " + both(b)},
			[2]string{b + " || " + rel, both(b) + " || " + rel})
	}
	const root = `{nestedSetParent < 0 && .a = "1"}`
	return append(res, [2]string{
		fmt.Sprintf(`(%s &>> %s) || (%s) | select(name, nestedSetParent)`, root, b, root),
		fmt.Sprintf(`(%s &>> %s) || %s | select(name, nestedSetParent)`, root, b, both(root)),
	})
}

func testSearchRepeatedOperand(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	s.seedStructural(t)
	assertSame := func(pairs [][2]string, run searchRun) {
		t.Helper()
		if run.limit == 0 {
			run.limit = searchLimit
		}
		for _, c := range pairs {
			got, want := s.result(t, c[0], run), s.result(t, c[1], run)
			isErr := len(want) == 1 && strings.HasPrefix(want[0], "error: ")
			if len(want) == 0 || isErr {
				t.Errorf("%s %+v: %.300q", c[1], run, want)
			}
			if !slices.Equal(got, want) {
				t.Errorf("%s %+v:\ngot:  %q\nwant: %q", c[0], run, got, want)
			}
		}
	}
	pairs := repeatedOperandSearches()
	assertSame(pairs, searchRun{})
	// 5 rounds leave the 40-deep chain unresolved.
	assertSame([][2]string{
		{`({.a = "1"} >> {.b = "1"}) || {.a = "1"}`, `({.a = "1"} >> {.b = "1"}) || ({.a = "1"} && {.a = "1"})`},
		{`{.a = "1"} || ({.b = "1"} &<< {.a = "1"})`, `({.a = "1"} && {.a = "1"}) || ({.b = "1"} &<< {.a = "1"})`},
	}, searchRun{rounds: 5})
	assertSame(pairs, searchRun{limit: 2})
	assertSame(pairs, searchRun{portions: 2})
}

func testSearchCycles(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	defer s.writeGolden(t)
	s.seed(t, []span{
		{20, 1, 0, []string{"a"}}, {20, 2, 1, []string{"b"}},
		{20, 3, 4, []string{"a", "b"}}, {20, 4, 3, []string{"b"}},
	})
	s.assertResults(t, "one-hop", []string{`{.a = "1"} > {.b = "1"}`, `{.a = "1"} ~ {.b = "1"}`, `{.a = "1"} !< {.b = "1"}`}, searchRun{})
	s.assertResults(t, "transitive", []string{`{.a = "1"} >> {.b = "1"}`, `{.a = "1"} !<< {.b = "1"}`}, searchRun{})
	s.seed(t, []span{{20, 5, 98, []string{"b"}}})
	s.assertResults(t, "dangling", []string{`{.a = "1"} > {.b = "1"}`, `{.a = "1"} &~ {.b = "1"}`}, searchRun{})
}

func testSearchRoundLimit(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	defer s.writeGolden(t)
	chain := func(trace, n, top int) []span {
		var res []span
		for i := 1; i <= n; i++ {
			sp := span{trace: trace, id: i, parent: i - 1}
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
	queries := []string{`{.a = "1"} >> {.b = "1"}`, `{.a = "1"} !>> {.b = "1"}`, `{.a = "1"} > {}`, `{.b = "1"} &<< {.a = "1"}`,
		`{.a = "1"} > {.b = "1"}`, `{.a = "1"} ~ {.b = "1"}`, `{.a = "1"} !> {.b = "1"}`}
	// 33: a root with children, and a chain of 8 under a missing parent.
	rooted := append(chain(33, 8, 99), span{33, 20, 0, []string{"a"}},
		span{33, 21, 20, []string{"b"}}, span{33, 22, 20, []string{"a"}}, span{33, 23, 20, []string{"b"}})
	s.seed(t, append(append(chain(30, 8, 99), chain(32, 8, 0)...), rooted...))
	s.assertResults(t, "resolved", queries, searchRun{rounds: 3})
	s.seed(t, chain(31, 9, 0))
	s.assertResults(t, "unresolved", queries[:2], searchRun{rounds: 3})
	// 34: a rooted chain of 9 and a span under a missing parent.
	s.seed(t, append(chain(34, 9, 0), span{34, 20, 98, []string{"a"}}))
	s.assertResults(t, "unresolved-one-hop", queries[4:7], searchRun{rounds: 3})
}

// Search returns the traces, spansets and selected values of the golden
// results, on well formed, dangling, rootless, deep and duplicated traces,
// under a limit that cuts the result and in complexity portions. On a
// cluster each trace's spans sit on one shard.
func TestSearchStructuralOperators(t *testing.T) { testSearchStructuralOperators(t, false) }

func TestSearchStructuralOperatorsOnCluster(t *testing.T) { testSearchStructuralOperators(t, true) }

// Search over HTTP returns the golden results.
func TestSearchStructuralOperatorsOverHTTP(t *testing.T) { testSearchOverHTTP(t, false) }

func TestSearchStructuralOperatorsOverHTTPOnCluster(t *testing.T) { testSearchOverHTTP(t, true) }

// A search whose `||` repeats an operand of its structural side returns what
// it returns with both sides planned.
func TestSearchRepeatedOperand(t *testing.T) { testSearchRepeatedOperand(t, false) }

func TestSearchRepeatedOperandOnCluster(t *testing.T) { testSearchRepeatedOperand(t, true) }

// A cycle's spans are unreachable or skip the trace, by operator; a one-hop
// operator checks them only in a trace with a root and a missing parent.
func TestSearchCycles(t *testing.T) { testSearchCycles(t, false) }

func TestSearchCyclesOnCluster(t *testing.T) { testSearchCycles(t, true) }

// At 3 rounds, a chain of 8 spans under a missing parent resolves, in a
// rooted trace too, and a rooted chain of 9 does not.
func TestSearchRoundLimit(t *testing.T) { testSearchRoundLimit(t, false) }

func TestSearchRoundLimitOnCluster(t *testing.T) { testSearchRoundLimit(t, true) }

// testSearchSmoke checks trace ids and order, whatever the portion count.
func testSearchSmoke(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	s.seedStructural(t)
	want := []string{"28", "11", "12", "e", "10", "a"}
	for _, portions := range []int{0, 1, 3} {
		var got []string
		for _, line := range s.result(t, `{.a = "1"} >> {.b = "1"}`, searchRun{limit: searchLimit, portions: portions}) {
			if id, ok := strings.CutPrefix(line, "trace "); ok {
				got = append(got, strings.Fields(id)[0])
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("portions %d: got %q, want %q", portions, got, want)
		}
	}
}

func TestSearchSmoke(t *testing.T) { testSearchSmoke(t, false) }

func TestSearchSmokeOnCluster(t *testing.T) { testSearchSmoke(t, true) }

// unresolvedSpanRE finds a span id in a summary line.
var unresolvedSpanRE = regexp.MustCompile(`span ([0-9a-f]+)@`)

// unresolvedSpans runs query: the sorted span ids it returns, by trace id.
func unresolvedSpans(t *testing.T, s *searchStore, query string, run searchRun) map[string]string {
	t.Helper()
	spans := map[string][]string{}
	var trace string
	for _, line := range s.result(t, query, run) {
		if strings.HasPrefix(line, "error: ") {
			t.Errorf("%s: %s", query, line)
			return nil
		}
		if id, ok := strings.CutPrefix(line, "trace "); ok {
			trace = strings.Fields(id)[0]
			spans[trace] = nil
			continue
		}
		for _, m := range unresolvedSpanRE.FindAllStringSubmatch(line, -1) {
			spans[trace] = append(spans[trace], m[1])
		}
	}
	res := map[string]string{}
	for id, ids := range spans {
		slices.Sort(ids)
		res[id] = strings.Join(slices.Compact(ids), " ")
	}
	return res
}

// testSearchUnresolvedTraces checks what each operator returns from traces
// with a parent cycle or deeper than the round limit.
func testSearchUnresolvedTraces(t *testing.T, cluster bool) {
	a, b, ab := []string{"a"}, []string{"b"}, []string{"a", "b"}
	type traces = map[string]string
	check := func(t *testing.T, s *searchStore, rounds int, query string, want traces) {
		t.Helper()
		for _, portions := range []int{0, 3} {
			got := unresolvedSpans(t, s, query, searchRun{limit: searchLimit, rounds: rounds, portions: portions})
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s portions %d: got %v, want %v", query, portions, got, want)
			}
		}
	}

	t.Run("cycles", func(t *testing.T) {
		s := openSearchStore(t, cluster, false)
		s.seed(t, []span{
			// 0x28, 0x2b: healthy, a above b and b above a.
			{40, 1, 0, a}, {40, 2, 1, b},
			{43, 1, 0, b}, {43, 2, 1, a},
			// 0x29: its own parent.
			{41, 1, 1, ab},
			// 0x2a: a healthy branch and a cycle, no missing parent.
			{42, 1, 0, a}, {42, 6, 1, b}, {42, 2, 4, a}, {42, 3, 2, b}, {42, 4, 3, ab}, {42, 5, 3, b},
			// 0x2c: a healthy branch, a missing parent and a cycle.
			{44, 1, 0, a}, {44, 2, 1, b}, {44, 3, 99, b}, {44, 4, 5, a}, {44, 5, 4, b},
			// 0x2d: a cycle of b spans only, never a candidate.
			{45, 1, 2, b}, {45, 2, 1, b},
		})
		for op, want := range map[string]traces{
			">>":  {"28": "2", "2a": "6", "2c": "2"},
			"!>>": {"29": "1", "2a": "3 4 5", "2b": "1", "2c": "3 5", "2d": "1 2"},
			"<<":  {"2b": "1"},
			"!<<": {"28": "2", "2d": "1 2"},
			"&>>": {"28": "1 2"},
			"&<<": {"2b": "1 2"},
			">":   {"28": "2", "2a": "3 6", "2c": "2"},
			"!>":  {"29": "1", "2a": "4 5", "2b": "1", "2c": "3 5", "2d": "1 2"},
			"&>":  {"28": "1 2", "2a": "1 2 3 6", "2c": "1 2"},
			"<":   {"2a": "3 4", "2b": "1"},
			"!<":  {"28": "2", "29": "1", "2a": "5 6", "2c": "2 3 5", "2d": "1 2"},
			"&<":  {"2a": "2 3 4", "2b": "1 2"},
			"~":   {"2a": "5"},
			"!~":  {"28": "2", "29": "1", "2a": "3 4 6", "2b": "1", "2c": "2 3 5", "2d": "1 2"},
			"&~":  {"2a": "4 5"},
		} {
			check(t, s, 0, fmt.Sprintf(`{.a = "1"} %s {.b = "1"}`, op), want)
		}
		check(t, s, 0, `{.a = "1"} >> {.b = "1"} | count() > 0`, traces{"28": "2", "2a": "6", "2c": "2"})
		check(t, s, 0, `{.a = "1"} !<< {.b = "1"} | count() > 0`, traces{"28": "2", "2d": "1 2"})
	})

	t.Run("round limit", func(t *testing.T) {
		s := openSearchStore(t, cluster, false)
		chain := func(trace, depth int) []span {
			var res []span
			for id := 1; id <= depth; id++ {
				attrs := ab
				switch id {
				case 1:
					attrs = a
				case depth:
					attrs = b
				}
				res = append(res, span{trace, id, id - 1, attrs})
			}
			return res
		}
		// 0x32 fits 3 rounds, 0x33 is one span too deep.
		s.seed(t, append(chain(50, 8), chain(51, 9)...))
		for op, want := range map[string]traces{
			">>":  {"32": "2 3 4 5 6 7 8", "33": "2 3 4 5 6 7 8"},
			"!>>": {"33": "9"},
			"<<":  {"32": "2 3 4 5 6"},
			"!<<": {"32": "7 8"},
			"&>>": {"32": "1 2 3 4 5 6 7 8"},
			"&<<": {"32": "2 3 4 5 6 7"},
			">":   {"32": "2 3 4 5 6 7 8", "33": "2 3 4 5 6 7 8 9"},
		} {
			check(t, s, 3, fmt.Sprintf(`{.a = "1"} %s {.b = "1"}`, op), want)
		}
	})
}

func TestSearchUnresolvedTraces(t *testing.T) { testSearchUnresolvedTraces(t, false) }

func TestSearchUnresolvedTracesOnCluster(t *testing.T) { testSearchUnresolvedTraces(t, true) }

// testProbeUnresolved checks what the probe reports for cycles, whatever the
// portion count, and that well-formed and dangling traces stay out of it.
func testProbeUnresolved(t *testing.T, cluster, http bool) {
	s := openSearchStore(t, cluster, http)
	a, b, ab := []string{"a"}, []string{"b"}, []string{"a", "b"}
	s.seed(t, []span{
		// well formed
		{40, 1, 0, a}, {40, 2, 1, b},
		// self-parent
		{41, 1, 1, ab},
		// cycle under a root
		{42, 1, 0, a}, {42, 6, 1, b}, {42, 2, 4, a}, {42, 3, 2, b}, {42, 4, 3, ab}, {42, 5, 3, b},
		// root, dangling span and cycle
		{44, 1, 0, a}, {44, 2, 1, b}, {44, 3, 99, b}, {44, 4, 5, a}, {44, 5, 4, b},
		// root and dangling span
		{45, 1, 0, a}, {45, 2, 1, b}, {45, 3, 99, ab},
	})
	const id41, id42, id44 = "00000000000000000000000000000029", "0000000000000000000000000000002a", "0000000000000000000000000000002c"
	for _, c := range []struct {
		query string
		n     uint64
		ids   []string
		jumps []uint64
	}{
		{`{.a = "1"} >> {.b = "1"}`, 3, []string{id41, id42, id44}, []uint64{3}},
		{`{.a = "1"} > {.b = "1"}`, 1, []string{id44}, []uint64{1}},
		{`({.a = "1"} >> {.b = "1"}) &<< ({.a = "1"} > {.b = "1"})`, 3, []string{id41, id42, id44}, []uint64{3, 1, 2}},
		{`{.a = "nope"} >> {.b = "nope"}`, 0, nil, []uint64{0}},
	} {
		for _, portions := range []int64{0, 3} {
			t.Run(fmt.Sprintf("%s portions %d", c.query, portions), func(t *testing.T) {
				q, err := traceql_ir.Parse(c.query)
				if err != nil {
					t.Fatal(err)
				}
				ctx := &shared.PlannerContext{
					Ctx: context.Background(), CHDb: s.db, IsCluster: s.cluster != "",
					From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Minute),
					AncestryRounds: 20, Portions: portions,
					VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
				}
				distconfig.Init()
				tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
					Name: s.name, ClusterName: s.cluster,
				}})
				found, err := probeUnresolved(context.Background(), q, ctx)
				if err != nil {
					t.Fatal(err)
				}
				n, ids := mergeUnresolved(found)
				if n != c.n || !slices.Equal(ids, c.ids) {
					t.Errorf("n = %d ids = %q, want %d %q", n, ids, c.n, c.ids)
				}
				sums := map[string]uint64{}
				var order []string
				for _, f := range found {
					if _, ok := sums[f.Jump]; !ok {
						order = append(order, f.Jump)
					}
					sums[f.Jump] += f.N
				}
				var jumps []uint64
				for _, j := range order {
					jumps = append(jumps, sums[j])
				}
				if !slices.Equal(jumps, c.jumps) {
					t.Errorf("per-jump counts %v, want %v", jumps, c.jumps)
				}
			})
		}
	}
}

func TestProbeUnresolved(t *testing.T) { testProbeUnresolved(t, false, false) }

func TestProbeUnresolvedOnCluster(t *testing.T) { testProbeUnresolved(t, true, false) }

func TestProbeUnresolvedOverHTTP(t *testing.T) { testProbeUnresolved(t, false, true) }

func TestProbeUnresolvedOverHTTPOnCluster(t *testing.T) { testProbeUnresolved(t, true, true) }

// A structural query's estimate is the spans in range on the fullest shard, so
// the budget that holds them reads it at once and one span less splits it.
func TestSearchPortionsByTheSpansInRange(t *testing.T) { testSearchPortionsByTheSpansInRange(t, false) }

func TestSearchPortionsByTheSpansInRangeOnCluster(t *testing.T) {
	testSearchPortionsByTheSpansInRange(t, true)
}

// fullestShard returns the spans of the fullest shard, a single node being one.
func (s *searchStore) fullestShard(t *testing.T) int {
	t.Helper()
	total := len(structuralTraces())
	if s.cluster == "" {
		return total
	}
	var fullest uint64
	if err := s.conn.QueryRow(context.Background(), `SELECT max(c) FROM
		(SELECT count() c FROM tempo_traces_dist GROUP BY _shard_num)`).Scan(&fullest); err != nil {
		t.Fatal(err)
	}
	if int(fullest) >= total {
		t.Fatalf("fullest shard holds %d of %d spans, want them split", fullest, total)
	}
	return int(fullest)
}

func testSearchPortionsByTheSpansInRange(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	s.seedStructural(t)
	t.Cleanup(func() {
		if err := traceqlconfig.Init(); err != nil {
			t.Error(err)
		}
	})
	fullest := s.fullestShard(t)
	for _, tc := range []struct {
		budget              int
		portions, totalJobs int64
	}{{fullest, 0, 1}, {fullest - 1, 2, 2}} {
		t.Setenv(traceqlconfig.PortionSpansEnv, strconv.Itoa(tc.budget))
		if err := traceqlconfig.Init(); err != nil {
			t.Fatal(err)
		}
		q, err := traceql_ir.Parse(`{.a = "1"} >> {.b = "1"}`)
		if err != nil {
			t.Fatal(err)
		}
		planner, err := Plan(q)
		if err != nil {
			t.Fatal(err)
		}
		ctx := &shared.PlannerContext{
			Ctx: context.Background(), CHDb: s.db, IsCluster: s.cluster != "",
			From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Minute),
			Limit: searchLimit, SpansPerSpanSet: 100, AncestryRounds: 20,
			VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
		}
		distconfig.Init()
		tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
			Name: s.name, ClusterName: s.cluster,
		}})
		ch, err := planner.Process(ctx)
		if err != nil {
			t.Fatal(err)
		}
		traces := 0
		for batch := range ch {
			traces += len(batch)
		}
		if ctx.Portions != tc.portions || traces == 0 {
			t.Errorf("budget %d: %d portions and %d traces, want %d portions and some traces",
				tc.budget, ctx.Portions, traces, tc.portions)
		}
		if ctx.TotalJobs() != tc.totalJobs {
			t.Errorf("budget %d: TotalJobs %d, want %d", tc.budget, ctx.TotalJobs(), tc.totalJobs)
		}
	}
}
