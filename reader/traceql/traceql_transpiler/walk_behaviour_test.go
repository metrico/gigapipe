package traceql_transpiler

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// walkQueries cover every operator a walk evaluates, a pipeline headed by each
// reader of every span, and select() in each position.
var walkQueries = []string{
	`{.a = "1"} >> {.b = "1"}`, `{.a = "1"} << {.b = "1"}`, `{.a = "1"} &>> {.b = "1"}`, `{.a = "1"} !>> {.b = "1"}`,
	`{.a = "1"} > {.b = "1"}`, `{.a = "1"} ~ {.b = "1"}`, `{.a = "1"} && {.b = "1"}`, `{.a = "1"} || {.b = "1"}`,
	`{.a = "1"} !> {.b = "1"}`, `{} !>> {.b = "1"}`, `{} >> {}`,
	`{.a = "1"} | count() > 2`, `{}`, `{} | count() > 2`, `count() > 2`, `by(.a)`,
	`{.a = "1" && .b = "1"}`, `{.a = "1" || .b = "1"}`, `{.a = .b}`, `{.a = "1"} >> {.a = .b}`,
	`{.a = "1"} | {.b = "1"}`, `{.a != nil}`, `{.a = nil}`, `{!(.a = "1")}`, `{duration > 1ns}`, `{nestedSetParent < 0}`,
	`({.a = "1"} | count() > 1) >> {.b = "1"}`, `{.a = "1"} >> {.b = "1"} || {.b = "1"}`, `{.a = "1"} | by(.b) | count() > 1`,
	`{.a = "1"} | select(.b)`, `{} | select(.a)`, `select(.a)`, `{.a = "1"} >> {.b = "1"} | select(name)`,
	`{.a = "1"} | by(.b) | select(name)`,
}

// A range across a walk's slices, with spans seeded on both sides of it.
var (
	walkEnd   = spanStart.Add(40 * time.Minute)
	walkBegin = spanStart.Add(10 * time.Minute)
)

// walkContext is a search over (from, to] at limit.
func (s *searchStore) walkContext(from, to time.Time, limit int) *shared.PlannerContext {
	ctx := &shared.PlannerContext{
		Ctx: context.Background(), CHDb: s.db, IsCluster: s.cluster != "",
		From: from, To: to, Limit: int64(limit), SpansPerSpanSet: 100, AncestryRounds: 8,
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	distconfig.Init()
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
		Name: s.name, ClusterName: s.cluster, SkipUnavailableShards: s.skipUnavailable,
	}})
	return ctx
}

func planWalk(t *testing.T, query string) (*walkProcessor, *traceql_ir.Query) {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	planner, err := Plan(q)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return planner.(*walkProcessor), q
}

// walkLines walks query over ctx: the summary of its traces.
func (s *searchStore) walkLines(t *testing.T, walk *walkProcessor, query string, ctx *shared.PlannerContext) []string {
	t.Helper()
	ch, err := walk.Process(ctx)
	if err != nil {
		t.Fatalf("%s limit %d: %v", query, ctx.Limit, err)
	}
	var res []model.TraceInfo
	for traces := range ch {
		res = append(res, traces...)
	}
	return summary(t, res)
}

// wholeLines is query read in one statement over the whole range.
func (s *searchStore) wholeLines(t *testing.T, query string, from, to time.Time, limit int) []string {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := clickhouse_transpiler.Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	traces, err := readTraces(s.walkContext(from, to, limit), plan)
	if err != nil {
		t.Fatalf("%s limit %d: %v", query, limit, err)
	}
	return summary(t, traces)
}

// seedRandomTrees seeds 60 random trees in 15 s steps from spanStart, so spans
// share starts and hit slice boundaries, and some lie outside walkBegin to walkEnd.
func (s *searchStore) seedRandomTrees(t *testing.T, seed int64) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	var spans []span
	var starts []int64
	for trace := 1; trace <= 60; trace++ {
		centre := r.Intn(50 * 4)
		spread := []int{1, 4, 20, 200}[r.Intn(4)]
		for id, n := 1, 1+r.Intn(7); id <= n; id++ {
			parent := 0
			if id > 1 {
				parent = 1 + r.Intn(id-1)
			}
			var attrs []string
			if r.Intn(2) == 0 {
				attrs = append(attrs, "a")
			}
			if r.Intn(2) == 0 {
				attrs = append(attrs, "b")
			}
			spans = append(spans, span{trace, id, parent, attrs})
			starts = append(starts, spanStart.UnixNano()+int64(centre+r.Intn(spread))*int64(15*time.Second))
		}
	}
	s.seedStarts(t, spans, func(i int) int64 { return starts[i] })
}

// A walk returns what one statement over the whole range returns: the same
// traces in the same order with the same spansets, at any limit.
func testWalkMatchesTheWholeRange(t *testing.T, cluster bool, seeds ...int64) {
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			s := openSearchStore(t, cluster, false)
			s.seedRandomTrees(t, seed)
			for _, query := range walkQueries {
				for _, limit := range []int{1, 10, 0} {
					walk, _ := planWalk(t, query)
					ctx := s.walkContext(walkBegin, walkEnd, limit)
					got := s.walkLines(t, walk, query, ctx)
					want := s.wholeLines(t, query, walkBegin, walkEnd, limit)
					if !reflect.DeepEqual(got, want) {
						t.Errorf("%s limit %d in %d slices:\nwalk:\n%s\nwhole:\n%s", query, limit, ctx.Portions,
							strings.Join(got, "\n"), strings.Join(want, "\n"))
					}
				}
			}
		})
	}
}

func TestWalkMatchesTheWholeRange(t *testing.T) { testWalkMatchesTheWholeRange(t, false, 1, 2, 3, 4) }

func TestWalkMatchesTheWholeRangeOnCluster(t *testing.T) { testWalkMatchesTheWholeRange(t, true, 1) }

// A limit that the newest spans fill stops the walk before older slices.
func testWalkStopsEarly(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	s.seedRandomTrees(t, 1)
	for _, query := range []string{`{}`, `{.a = "1"}`, `{.a = "1"} >> {.b = "1"}`} {
		walkedAt := func(limit int) int64 {
			walk, _ := planWalk(t, query)
			ctx := s.walkContext(walkBegin, walkEnd, limit)
			s.walkLines(t, walk, query, ctx)
			return ctx.Portions
		}
		if one, all := walkedAt(1), walkedAt(0); one >= all {
			t.Errorf("%s: %d slices at limit 1, %d at limit 0", query, one, all)
		}
	}
}

func TestWalkStopsEarly(t *testing.T) { testWalkStopsEarly(t, false) }

func TestWalkStopsEarlyOnCluster(t *testing.T) { testWalkStopsEarly(t, true) }

// A range's time zone doesn't change what a search finds, attributes included.
func TestSearchInAnotherReaderZone(t *testing.T) {
	s := openSearchStore(t, false, false)
	s.seed(t, []span{{trace: 1, id: 1, attrs: []string{"a", "b"}}, {trace: 1, id: 2, parent: 1, attrs: []string{"b"}}})
	from, to := spanStart.Add(-time.Minute), spanStart.Add(time.Minute)
	for _, query := range []string{`{.a = "1"} >> {.b = "1"}`, `{.a = .b}`, `{.a = "1"} | select(.b)`} {
		run := searchRun{rounds: 20, limit: searchLimit, from: from, to: to}
		want := s.result(t, query, run)
		if len(want) == 0 {
			t.Fatalf("%s: nothing found", query)
		}
		zone := time.FixedZone("reader", -8*3600)
		run.from, run.to = from.In(zone), to.In(zone)
		if got := s.result(t, query, run); !slices.Equal(got, want) {
			t.Errorf("%s: %q, want %q", query, got, want)
		}
	}
}

// traceIDs are the ids of a summary's traces.
func traceIDs(lines []string) []string {
	ids := []string{}
	for _, line := range lines {
		if rest, ok := strings.CutPrefix(line, "trace "); ok {
			ids = append(ids, strings.Fields(rest)[0])
		}
	}
	return ids
}

// beforeEnd is the start d before walkEnd.
func beforeEnd(d time.Duration) int64 { return walkEnd.Add(-d).UnixNano() }

// walkCase is a named tree of spans with starts, walked at limit 1.
type walkCase struct {
	name   string
	spans  []span
	starts []int64
	query  string
	want   []string
}

func testWalkNamedCases(t *testing.T, cluster bool) {
	a, b, ab := []string{"a"}, []string{"b"}, []string{"a", "b"}
	boundary := beforeEnd(time.Minute)
	for _, c := range []walkCase{
		{
			name:   ".a and .b in different slices",
			spans:  []span{{1, 1, 0, a}, {1, 2, 1, b}},
			starts: []int64{beforeEnd(10 * time.Second), beforeEnd(2 * time.Minute)},
			query:  `{.a = "1"} >> {.b = "1"}`,
			want:   []string{"1"},
		},
		{
			name:   ".a and .b in different slices, && across them",
			spans:  []span{{1, 1, 0, a}, {1, 2, 1, b}},
			starts: []int64{beforeEnd(2 * time.Minute), beforeEnd(10 * time.Second)},
			query:  `{.a = "1"} && {.b = "1"}`,
			want:   []string{"1"},
		},
		{
			name: "the trace's newest span doesn't match, an older one does",
			spans: []span{
				{1, 1, 0, nil}, {1, 2, 1, a},
				{2, 1, 0, a},
			},
			starts: []int64{beforeEnd(time.Second), beforeEnd(5 * time.Minute), beforeEnd(2 * time.Minute)},
			query:  `{.a = "1"}`,
			want:   []string{"2"},
		},
		{
			name:   "equal ranks take the higher trace id",
			spans:  []span{{1, 1, 0, a}, {2, 1, 0, a}, {3, 1, 0, a}},
			starts: []int64{beforeEnd(time.Second), beforeEnd(time.Second), beforeEnd(2 * time.Second)},
			query:  `{.a = "1"}`,
			want:   []string{"2"},
		},
		{
			// Trace 2 is no candidate of the first slice, and outranks trace 1.
			name: "equal ranks at a slice boundary read the next slice",
			spans: []span{
				{1, 1, 0, a}, {1, 2, 1, b},
				{2, 1, 0, a}, {2, 2, 1, b},
			},
			starts: []int64{beforeEnd(10 * time.Second), boundary, boundary, boundary},
			query:  `{.a = "1"} >> {.b = "1"}`,
			want:   []string{"2"},
		},
		{
			name: "a trace spanning every slice comes once",
			spans: []span{
				{1, 1, 0, ab}, {1, 2, 1, ab}, {1, 3, 2, ab}, {1, 4, 3, ab}, {1, 5, 4, ab},
			},
			starts: []int64{
				beforeEnd(10 * time.Second), beforeEnd(2 * time.Minute), beforeEnd(5 * time.Minute),
				beforeEnd(10 * time.Minute), beforeEnd(20 * time.Minute),
			},
			query: `{.a = "1"} >> {.b = "1"}`,
			want:  []string{"1"},
		},
		{
			name: "a negated operator returns the older side",
			spans: []span{
				{1, 1, 0, a}, {1, 2, 1, b},
				{2, 1, 0, nil}, {2, 2, 1, b},
			},
			starts: []int64{
				beforeEnd(10 * time.Second), beforeEnd(5 * time.Minute),
				beforeEnd(8 * time.Minute), beforeEnd(7 * time.Minute),
			},
			query: `{.a = "1"} !>> {.b = "1"}`,
			want:  []string{"2"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := openSearchStore(t, cluster, false)
			s.seedStarts(t, c.spans, func(i int) int64 { return c.starts[i] })
			from := walkEnd.Add(-30 * time.Minute)
			walk, _ := planWalk(t, c.query)
			got := s.walkLines(t, walk, c.query, s.walkContext(from, walkEnd, 1))
			if ids := traceIDs(got); !slices.Equal(ids, c.want) {
				t.Errorf("got %q, want %q", ids, c.want)
			}
			if want := s.wholeLines(t, c.query, from, walkEnd, 1); !reflect.DeepEqual(got, want) {
				t.Errorf("walk:\n%s\nwhole:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
		})
	}
}

func TestWalkNamedCases(t *testing.T) { testWalkNamedCases(t, false) }

func TestWalkNamedCasesOnCluster(t *testing.T) { testWalkNamedCases(t, true) }

// A span of a known trace arriving between two slices doesn't lose the trace.
func testWalkSpanArrivesBetweenSlices(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	s.seedStarts(t, []span{{1, 1, 0, []string{"a"}}}, func(int) int64 { return beforeEnd(10 * time.Second) })
	const query = `{.a = "1"} >> {.b = "1"}`
	walk, _ := planWalk(t, query)
	extent, slice := walk.extent, 0
	walk.extent = func(ctx *shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) {
		if slice++; slice == 2 {
			s.seedStarts(t, []span{{1, 2, 1, []string{"b"}}}, func(int) int64 { return beforeEnd(90 * time.Second) })
		}
		return extent(ctx)
	}
	got := s.walkLines(t, walk, query, s.walkContext(walkEnd.Add(-10*time.Minute), walkEnd, 1))
	if ids := traceIDs(got); !slices.Equal(ids, []string{"1"}) {
		t.Errorf("got %q, want the trace", ids)
	}
}

func TestWalkSpanArrivesBetweenSlices(t *testing.T) { testWalkSpanArrivesBetweenSlices(t, false) }

func TestWalkSpanArrivesBetweenSlicesOnCluster(t *testing.T) {
	testWalkSpanArrivesBetweenSlices(t, true)
}
