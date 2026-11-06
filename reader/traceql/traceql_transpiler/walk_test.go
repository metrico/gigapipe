package traceql_transpiler

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

var walkTo = time.Unix(100000, 0).UTC()

// A row that widens no slice: a read's range is the slice's own.
var sliceOnly = clickhouse_transpiler.ShardExtent{Lo: math.MaxInt64, Hi: 0, Spans: 1}

// nanosBefore is the time d before walkTo, in nanoseconds.
func nanosBefore(d time.Duration) int64 { return walkTo.Add(-d).UnixNano() }

// ranked is a trace with a search rank and a start.
func ranked(id string, newest, start int64) model.TraceInfo {
	return model.TraceInfo{TraceID: id, Newest: newest, StartTimeUnixNano: strconv.FormatInt(start, 10)}
}

// scriptedWalk answers its slices with parts in turn, then with nothing,
// recording the context of each read.
type scriptedWalk struct {
	*walkProcessor
	reads []shared.PlannerContext
	parts [][]model.TraceInfo
}

// newScriptedWalk walks 1 minute, then 2, 4 and so on, every slice having
// candidates.
func newScriptedWalk(parts ...[]model.TraceInfo) *scriptedWalk {
	s := &scriptedWalk{parts: parts}
	s.walkProcessor = &walkProcessor{
		main: &clickhouse_transpiler.SearchPlan{}, first: time.Minute,
		next: func(d time.Duration, _, _ int64) time.Duration { return 2 * d },
		extent: func(*shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) {
			return []clickhouse_transpiler.ShardExtent{sliceOnly}, nil
		},
		read: func(ctx *shared.PlannerContext, _ *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
			i := len(s.reads)
			s.reads = append(s.reads, *ctx)
			if i < len(s.parts) {
				return s.parts[i], nil
			}
			return nil, nil
		},
	}
	return s
}

// run walks limit over the 15 minutes before walkTo: the trace ids returned.
func (s *scriptedWalk) run(t *testing.T, limit int64) ([]string, *shared.PlannerContext) {
	t.Helper()
	ctx := &shared.PlannerContext{From: walkTo.Add(-15 * time.Minute), To: walkTo, Limit: limit}
	ch, err := s.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for traces := range ch {
		for _, trace := range traces {
			ids = append(ids, trace.TraceID)
		}
	}
	return ids, ctx
}

// The first slice's lower boundary, 1 minute before walkTo, and the second's, 3.
var (
	boundary1 = nanosBefore(time.Minute)
	boundary2 = nanosBefore(3 * time.Minute)
)

func TestWalkStopRule(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limit  int64
		parts  [][]model.TraceInfo
		slices int
		want   []string
	}{
		{
			name: "stops when limit traces rank above the boundary", limit: 1,
			parts:  [][]model.TraceInfo{{ranked("aa", boundary1+1, 1)}},
			slices: 1, want: []string{"aa"},
		},
		{
			name: "a rank at the boundary doesn't stop it", limit: 1,
			parts:  [][]model.TraceInfo{{ranked("aa", boundary1, 1)}, {ranked("bb", boundary1, 2)}},
			slices: 2, want: []string{"bb"},
		},
		{
			name: "a rank below the boundary doesn't stop it", limit: 1,
			parts:  [][]model.TraceInfo{{ranked("aa", boundary1-5, 1)}, {ranked("bb", boundary1-1, 2)}},
			slices: 2, want: []string{"bb"},
		},
		{
			name: "fewer than limit don't stop it", limit: 2,
			parts:  [][]model.TraceInfo{{ranked("aa", boundary1+1, 1)}, {ranked("bb", boundary2+1, 2)}},
			slices: 2, want: []string{"bb", "aa"},
		},
		{
			name: "a trace counts once across its slices", limit: 2,
			parts: [][]model.TraceInfo{
				{ranked("aa", boundary1+1, 1)}, {ranked("aa", boundary1+1, 1)}, {ranked("aa", boundary1+1, 1)},
			},
			slices: 4, want: []string{"aa"},
		},
		{
			name: "traces of older slices count once above the boundary", limit: 2,
			parts: [][]model.TraceInfo{
				{ranked("aa", boundary1+1, 1)}, {ranked("aa", boundary1+1, 1), ranked("bb", boundary2+1, 2)},
			},
			slices: 2, want: []string{"bb", "aa"},
		},
		{
			name: "limit 0 reads every slice", limit: 0,
			parts:  [][]model.TraceInfo{{ranked("aa", boundary1+1, 1)}},
			slices: 4, want: []string{"aa"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walk := newScriptedWalk(tc.parts...)
			got, ctx := walk.run(t, tc.limit)
			if !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
			if len(walk.reads) != tc.slices || ctx.Portions != int64(tc.slices) || ctx.TotalJobs() != int64(tc.slices) {
				t.Errorf("%d reads, Portions %d, TotalJobs %d, want %d", len(walk.reads), ctx.Portions, ctx.TotalJobs(), tc.slices)
			}
		})
	}
}

// Slices cover the range newest first, each ending where the last began.
func TestWalkSlicesAreAdjacent(t *testing.T) {
	walk := newScriptedWalk()
	_, ctx := walk.run(t, 0)
	want := []time.Duration{0, time.Minute, 3 * time.Minute, 7 * time.Minute, 15 * time.Minute}
	if len(walk.reads) != len(want)-1 {
		t.Fatalf("%d reads", len(walk.reads))
	}
	for i, read := range walk.reads {
		if !read.To.Equal(walkTo.Add(-want[i])) || !read.From.Equal(walkTo.Add(-want[i+1])) {
			t.Errorf("slice %d: (%v, %v]", i, read.From, read.To)
		}
	}
	if !ctx.From.Equal(walkTo.Add(-15*time.Minute)) || !ctx.To.Equal(walkTo) {
		t.Errorf("request range changed: (%v, %v]", ctx.From, ctx.To)
	}
}

// A slice is read over its candidates' spans on every shard, from 1ns before
// the earliest, and over the slice itself.
func TestWalkReadsTheExtent(t *testing.T) {
	loc := time.FixedZone("zone", 3600)
	lo, hi := walkTo.Add(-10*time.Minute).UnixNano(), walkTo.Add(-5*time.Second).UnixNano()
	for _, tc := range []struct {
		name     string
		rows     []clickhouse_transpiler.ShardExtent
		from, to time.Time
	}{
		{"wider than the slice", []clickhouse_transpiler.ShardExtent{{Lo: lo, Hi: hi}},
			walkTo.Add(-10*time.Minute - time.Nanosecond), walkTo},
		{"across shards", []clickhouse_transpiler.ShardExtent{
			{Lo: lo + 1, Hi: hi}, {Lo: lo, Hi: hi - 1, Shard: 1}, {Lo: lo + 2, Hi: hi - 2, Shard: 2},
		}, walkTo.Add(-10*time.Minute - time.Nanosecond), walkTo},
		{"inside the slice", []clickhouse_transpiler.ShardExtent{
			{Lo: walkTo.Add(-30 * time.Second).UnixNano(), Hi: walkTo.Add(-20 * time.Second).UnixNano()},
		}, walkTo.Add(-time.Minute), walkTo},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walk := newScriptedWalk([]model.TraceInfo{ranked("aa", boundary1+1, 1)})
			walk.extent = func(*shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) { return tc.rows, nil }
			ctx := &shared.PlannerContext{From: walkTo.Add(-15 * time.Minute).In(loc), To: walkTo.In(loc), Limit: 1}
			if _, err := walk.Process(ctx); err != nil {
				t.Fatal(err)
			}
			read := walk.reads[0]
			if !read.From.Equal(tc.from) || !read.To.Equal(tc.to) {
				t.Errorf("read (%v, %v], want (%v, %v]", read.From, read.To, tc.from, tc.to)
			}
			if read.From.Location() != loc || read.To.Location() != loc {
				t.Errorf("read in %v and %v", read.From.Location(), read.To.Location())
			}
			if read.TracesCandidates == nil {
				t.Error("read without candidates")
			}
			if len(ctx.TracesSlices) != 1 || !ctx.TracesSlices[0].From.Equal(tc.from) || !ctx.TracesSlices[0].To.Equal(tc.to) ||
				ctx.TracesSlices[0].Candidates != read.TracesCandidates {
				t.Errorf("recorded %+v", ctx.TracesSlices)
			}
		})
	}
}

// A slice without candidates isn't read and counts as a slice.
func TestWalkSkipsASliceWithoutCandidates(t *testing.T) {
	walk := newScriptedWalk([]model.TraceInfo{ranked("aa", boundary2+1, 1)})
	calls := 0
	walk.extent = func(*shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) {
		calls++
		if calls == 1 {
			return nil, nil
		}
		return []clickhouse_transpiler.ShardExtent{sliceOnly}, nil
	}
	got, ctx := walk.run(t, 1)
	if !slices.Equal(got, []string{"aa"}) || calls != 2 || len(walk.reads) != 1 || ctx.Portions != 2 || len(ctx.TracesSlices) != 1 {
		t.Errorf("got %v, %d extents, %d reads, %d slices, %d evaluated", got, calls, len(walk.reads), ctx.Portions, len(ctx.TracesSlices))
	}
}

func TestWalkMerge(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int64
		parts [][]model.TraceInfo
		want  []string
	}{
		{
			name:  "the newer match wins over the later start",
			limit: 1,
			parts: [][]model.TraceInfo{{ranked("aa", 5, 1)}, {ranked("bb", 4, 9)}},
			want:  []string{"aa"},
		},
		{
			name:  "a later slice displaces a lower rank",
			limit: 1,
			parts: [][]model.TraceInfo{{ranked("aa", 4, 1)}, {ranked("bb", 5, 1)}},
			want:  []string{"bb"},
		},
		{
			name:  "equal ranks keep the higher trace id",
			limit: 1,
			parts: [][]model.TraceInfo{{ranked("bb", 5, 1)}, {ranked("aa", 5, 1)}, {ranked("cc", 5, 1)}},
			want:  []string{"cc"},
		},
		{
			name:  "returns the top limit across slices",
			limit: 2,
			parts: [][]model.TraceInfo{
				{ranked("aa", 9, 1), ranked("bb", 3, 2)},
				{ranked("cc", 8, 3), ranked("dd", 2, 4)},
				{ranked("ee", 1, 5), ranked("ff", 7, 6)},
			},
			want: []string{"cc", "aa"},
		},
		{
			name:  "limit 0 keeps every trace",
			limit: 0,
			parts: [][]model.TraceInfo{{ranked("aa", 1, 1), ranked("bb", 2, 2)}, {ranked("cc", 3, 3)}},
			want:  []string{"cc", "bb", "aa"},
		},
		{
			name:  "the result is by start, newest first",
			limit: 3,
			parts: [][]model.TraceInfo{{ranked("aa", 9, 1), ranked("bb", 8, 3)}, {ranked("cc", 7, 2)}},
			want:  []string{"bb", "cc", "aa"},
		},
		{
			name:  "the start compares as a number",
			limit: 2,
			parts: [][]model.TraceInfo{{ranked("aa", 2, 9)}, {ranked("bb", 1, 10)}},
			want:  []string{"bb", "aa"},
		},
		{
			name:  "duplicates don't fill the top",
			limit: 2,
			parts: [][]model.TraceInfo{{ranked("aa", 9, 2)}, {ranked("aa", 9, 2), ranked("bb", 8, 1)}},
			want:  []string{"aa", "bb"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := newScriptedWalk(tc.parts...).run(t, tc.limit)
			if !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// A trace read in two slices keeps the later copy; one a later slice doesn't
// return keeps its earlier copy.
func TestWalkKeepsTheLaterCopy(t *testing.T) {
	copyOf := func(id string, matched int, newest int64) model.TraceInfo {
		trace := ranked(id, newest, 1)
		trace.SpanSet.Matched = matched
		return trace
	}
	ch, err := newScriptedWalk(
		[]model.TraceInfo{copyOf("aa", 1, 5), copyOf("bb", 1, 3)},
		[]model.TraceInfo{copyOf("aa", 2, 7)},
	).Process(&shared.PlannerContext{From: walkTo.Add(-2 * time.Minute), To: walkTo})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][2]int64{}
	for traces := range ch {
		for _, trace := range traces {
			got[trace.TraceID] = [2]int64{int64(trace.SpanSet.Matched), trace.Newest}
		}
	}
	if want := (map[string][2]int64{"aa": {2, 7}, "bb": {1, 3}}); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestTracesWithEqualStartsSortByTraceID(t *testing.T) {
	const traces = 200
	var parts [3][]model.TraceInfo
	want := make([]string, traces)
	for i := range traces {
		id := fmt.Sprintf("t%03d", i)
		want[traces-1-i] = id
		parts[i%3] = append(parts[i%3], ranked(id, int64(traces-i), 1))
	}
	if got, _ := newScriptedWalk(parts[:]...).run(t, traces); !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestWalkErrorFailsTheSearch(t *testing.T) {
	boom := errors.New("boom")
	failAt := func(calls *int, at int) bool {
		*calls++
		return *calls == at
	}
	for _, at := range []int{1, 2} {
		t.Run(fmt.Sprintf("extent read, slice %d", at), func(t *testing.T) {
			walk, calls := newScriptedWalk(), 0
			walk.extent = func(*shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) {
				if failAt(&calls, at) {
					return nil, boom
				}
				return []clickhouse_transpiler.ShardExtent{sliceOnly}, nil
			}
			if _, err := walk.Process(&shared.PlannerContext{From: walkTo.Add(-time.Hour), To: walkTo}); !errors.Is(err, boom) {
				t.Errorf("got %v", err)
			}
		})
		t.Run(fmt.Sprintf("evaluation, slice %d", at), func(t *testing.T) {
			walk, calls := newScriptedWalk(), 0
			walk.read = func(*shared.PlannerContext, *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
				if failAt(&calls, at) {
					return nil, boom
				}
				return []model.TraceInfo{ranked("aa", 1, 1)}, nil
			}
			if _, err := walk.Process(&shared.PlannerContext{From: walkTo.Add(-time.Hour), To: walkTo}); !errors.Is(err, boom) {
				t.Errorf("got %v", err)
			}
		})
	}
}

// A range under the first slice is one slice.
func TestWalkClipsToTheRange(t *testing.T) {
	walk := newScriptedWalk()
	ctx := &shared.PlannerContext{From: walkTo.Add(-time.Second), To: walkTo, Limit: 1}
	if _, err := walk.Process(ctx); err != nil {
		t.Fatal(err)
	}
	if len(walk.reads) != 1 || !walk.reads[0].From.Equal(ctx.From) {
		t.Errorf("reads %+v", walk.reads)
	}
}

// A query the server can't run fails with no candidates to evaluate, before any read.
func TestWalkFailsOnAnUnsupportedServer(t *testing.T) {
	query, err := traceql_ir.Parse(`{.a = "1"} >> {.b = "1"}`)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := Plan(query)
	if err != nil {
		t.Fatal(err)
	}
	walk := planner.(*walkProcessor)
	walk.extent = func(*shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) {
		t.Error("the extent read ran")
		return nil, nil
	}
	if _, err := walk.Process(&shared.PlannerContext{From: walkTo.Add(-time.Hour), To: walkTo}); err == nil {
		t.Error("no error")
	}
}

func TestKeepTop(t *testing.T) {
	traces := func() []model.TraceInfo {
		return []model.TraceInfo{ranked("aa", 1, 9), ranked("bb", 3, 8), ranked("cc", 2, 7), ranked("dd", 3, 6)}
	}
	for _, tc := range []struct {
		limit int64
		want  []string
	}{
		{0, []string{"dd", "bb", "cc", "aa"}},
		{4, []string{"dd", "bb", "cc", "aa"}},
		{9, []string{"dd", "bb", "cc", "aa"}},
		{3, []string{"dd", "bb", "cc"}},
		{1, []string{"dd"}},
	} {
		ids := []string{}
		for _, trace := range keepTop(traces(), tc.limit) {
			ids = append(ids, trace.TraceID)
		}
		if !slices.Equal(ids, tc.want) {
			t.Errorf("limit %d: got %v, want %v", tc.limit, ids, tc.want)
		}
	}
}

// Search walks; autocomplete reads by estimate and portions.
func TestOnlySearchWalks(t *testing.T) {
	query, err := traceql_ir.Parse(`{.a = "1"}`)
	if err != nil {
		t.Fatal(err)
	}
	search, err := Plan(query)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := search.(*walkProcessor); !ok {
		t.Errorf("search is %T", search)
	}
	tags, err := PlanTagsV2(query)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tags.(*TraceQLComplexityEvaluator); !ok {
		t.Errorf("tags are %T", tags)
	}
}

// row is one shard's extent that widens no slice.
func row(spans, traces int64) clickhouse_transpiler.ShardExtent {
	return clickhouse_transpiler.ShardExtent{Lo: math.MaxInt64, Spans: spans, Traces: traces}
}

// withExtents gives the walk budget and answers its extent reads with attempts
// in turn, then with one row within budget. calls counts the reads.
func (s *scriptedWalk) withExtents(budget int64, attempts ...[]clickhouse_transpiler.ShardExtent) (calls *int) {
	calls = new(int)
	s.budget = budget
	s.extent = func(*shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) {
		*calls++
		if *calls <= len(attempts) {
			return attempts[*calls-1], nil
		}
		return []clickhouse_transpiler.ShardExtent{row(1, 1)}, nil
	}
	return calls
}

// checkReads compares each read's range with its (from, to] before walkTo.
func checkReads(t *testing.T, reads []shared.PlannerContext, want ...[2]time.Duration) {
	t.Helper()
	if len(reads) != len(want) {
		t.Fatalf("%d reads, want %d", len(reads), len(want))
	}
	for i, read := range reads {
		if from, to := walkTo.Sub(read.From), walkTo.Sub(read.To); from != want[i][0] || to != want[i][1] {
			t.Errorf("read %d: from %v to %v before the end, want %v", i, from, to, want[i])
		}
	}
}

// A slice with a shard over budget halves, keeping its newest half; shards are
// never summed, and a shard at the budget is within it.
func TestWalkBudget(t *testing.T) {
	type rows = []clickhouse_transpiler.ShardExtent
	for _, tc := range []struct {
		name   string
		budget int64
		first  rows
		calls  int
		want   time.Duration
	}{
		{"one shard of two over", 10, rows{row(5, 2), row(11, 2)}, 2, 30 * time.Second},
		{"at the budget", 10, rows{row(10, 2)}, 1, time.Minute},
		{"not summed across shards", 10, rows{row(6, 2), row(6, 2)}, 1, time.Minute},
		{"budget 0 is no budget", 0, rows{row(1<<40, 2)}, 1, time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walk := newScriptedWalk([]model.TraceInfo{ranked("aa", walkTo.UnixNano(), 1)})
			calls := walk.withExtents(tc.budget, tc.first)
			_, ctx := walk.run(t, 1)
			if *calls != tc.calls {
				t.Errorf("%d extent reads, want %d", *calls, tc.calls)
			}
			checkReads(t, walk.reads, [2]time.Duration{tc.want, 0})
			if ctx.TotalJobs() != 1 || len(ctx.TracesSlices) != 1 {
				t.Errorf("TotalJobs %d, %d evaluated slices, want 1", ctx.TotalJobs(), len(ctx.TracesSlices))
			}
		})
	}
}

// The slice after a halved one starts at the halved boundary.
func TestWalkContinuesFromTheHalvedSlice(t *testing.T) {
	walk := newScriptedWalk()
	walk.withExtents(10, []clickhouse_transpiler.ShardExtent{row(11, 2)})
	walk.run(t, 0)
	if len(walk.reads) < 2 {
		t.Fatalf("%d reads", len(walk.reads))
	}
	checkReads(t, walk.reads[:2], [2]time.Duration{30 * time.Second, 0}, [2]time.Duration{90 * time.Second, 30 * time.Second})
}

// windowLeaf records the range of each candidates read.
type windowLeaf struct{ windows [][2]time.Duration }

func (l *windowLeaf) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	l.windows = append(l.windows, [2]time.Duration{walkTo.Sub(ctx.From), walkTo.Sub(ctx.To)})
	return sql.NewSelect(), nil
}

// Each extent read counts the candidates of its attempt's window, over the
// request's range, and the slice is evaluated with the last attempt's.
func TestWalkRecountsTheHalvedWindow(t *testing.T) {
	leaf := &windowLeaf{}
	walk := newScriptedWalk([]model.TraceInfo{ranked("aa", walkTo.UnixNano(), 1)})
	walk.main.Leaves = []shared.SQLRequestPlanner{leaf}
	over := []clickhouse_transpiler.ShardExtent{row(11, 2)}
	walk.withExtents(10, over, over)
	scripted := walk.extent
	walk.extent = func(ctx *shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error) {
		if !ctx.From.Equal(walkTo.Add(-15*time.Minute)) || !ctx.To.Equal(walkTo) {
			t.Errorf("extent over (%v, %v]", ctx.From, ctx.To)
		}
		if _, err := ctx.TracesCandidates.Process(ctx); err != nil {
			return nil, err
		}
		return scripted(ctx)
	}
	walk.run(t, 1)
	want := [][2]time.Duration{{time.Minute, 0}, {30 * time.Second, 0}, {15 * time.Second, 0}}
	if !slices.Equal(leaf.windows, want) {
		t.Errorf("attempts counted %v, want %v", leaf.windows, want)
	}
	leaf.windows = nil
	if _, err := walk.reads[0].TracesCandidates.Process(&walk.reads[0]); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(leaf.windows, want[2:]) {
		t.Errorf("evaluated %v, want %v", leaf.windows, want[2:])
	}
}

// The stop rule uses the halved boundary: a rank between it and the planned
// boundary doesn't stop the walk.
func TestWalkStopsOnTheHalvedBoundary(t *testing.T) {
	walk := newScriptedWalk(
		[]model.TraceInfo{ranked("aa", nanosBefore(45*time.Second), 1)},
		[]model.TraceInfo{ranked("bb", nanosBefore(40*time.Second), 2)},
	)
	walk.withExtents(10, []clickhouse_transpiler.ShardExtent{row(11, 2)})
	got, ctx := walk.run(t, 1)
	if !slices.Equal(got, []string{"bb"}) || ctx.TotalJobs() != 2 {
		t.Errorf("got %v in %d slices, want bb in 2", got, ctx.TotalJobs())
	}
}

// A halved attempt without candidates ends the slice with no read.
func TestWalkHalvedSliceWithoutCandidates(t *testing.T) {
	walk := newScriptedWalk()
	walk.withExtents(10, []clickhouse_transpiler.ShardExtent{row(11, 2)}, nil)
	_, ctx := walk.run(t, 1)
	if ctx.TotalJobs() < 2 {
		t.Fatalf("TotalJobs %d", ctx.TotalJobs())
	}
	if !walk.reads[0].To.Equal(walkTo.Add(-30 * time.Second)) {
		t.Errorf("first read ends %v before the end, want 30s", walkTo.Sub(walk.reads[0].To))
	}
	if int64(len(walk.reads)) != ctx.TotalJobs()-1 {
		t.Errorf("%d reads, TotalJobs %d", len(walk.reads), ctx.TotalJobs())
	}
}

// A lone {} has no budget; every other search has the setting.
func TestPlanSetsTheBudget(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  int64
	}{
		{`{}`, 0},
		{`{} | select(.a)`, 0},
		{`{.a = "1"}`, traceqlconfig.PortionSpans()},
		{`{} | count() > 2`, traceqlconfig.PortionSpans()},
		{`{.a = "1"} >> {.b = "1"}`, traceqlconfig.PortionSpans()},
	} {
		if walk, _ := planWalk(t, tc.query); walk.budget != tc.want {
			t.Errorf("%s: budget %d, want %d", tc.query, walk.budget, tc.want)
		}
	}
}

// A slice over budget halves down to 1 s, or to a shard's one trace, then runs.
func TestWalkFloors(t *testing.T) {
	type rows = []clickhouse_transpiler.ShardExtent
	over := func(n int, r ...clickhouse_transpiler.ShardExtent) []rows {
		res := make([]rows, n)
		for i := range res {
			res[i] = r
		}
		return res
	}
	for _, tc := range []struct {
		name     string
		span     time.Duration
		attempts []rows
		calls    int
		want     time.Duration
	}{
		{"stops at 1 s still over budget", time.Hour, over(100, row(11, 2)), 7, time.Second},
		{"one trace runs at once", time.Hour, over(100, row(11, 1)), 1, time.Minute},
		{"two traces keep halving", time.Hour, append(over(1, row(11, 2)), over(1, row(11, 1))...), 2, 30 * time.Second},
		{"a many-trace shard within budget doesn't halve", time.Hour, over(100, row(11, 1), row(9, 5)), 1, time.Minute},
		{"halves the clipped slice", 3 * time.Second, over(1, row(11, 2)), 2, 1500 * time.Millisecond},
		{"a clipped slice stops at 1 s", 3 * time.Second, over(100, row(11, 2)), 3, time.Second},
		{"a range at 1 s isn't halved", time.Second, over(100, row(11, 2)), 1, time.Second},
		{"a range under 1 s isn't halved", 500 * time.Millisecond, over(100, row(11, 2)), 1, 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walk := newScriptedWalk([]model.TraceInfo{ranked("aa", walkTo.UnixNano(), 1)})
			calls := walk.withExtents(10, tc.attempts...)
			ctx := &shared.PlannerContext{From: walkTo.Add(-tc.span), To: walkTo, Limit: 1}
			if _, err := walk.Process(ctx); err != nil {
				t.Fatal(err)
			}
			if *calls != tc.calls {
				t.Errorf("%d extent reads, want %d", *calls, tc.calls)
			}
			checkReads(t, walk.reads, [2]time.Duration{tc.want, 0})
			if ctx.TotalJobs() != 1 {
				t.Errorf("TotalJobs %d", ctx.TotalJobs())
			}
		})
	}
}

func TestNextSlice(t *testing.T) {
	const longest = time.Duration(math.MaxInt64)
	for _, tc := range []struct {
		name         string
		d            time.Duration
		limit, found int64
		want         time.Duration
	}{
		{"the formula above double", time.Minute, 100, 10, 9 * time.Minute},
		{"the formula keeps a fraction", time.Minute, 7, 2, 150 * time.Second},
		{"the formula at double", time.Minute, 3, 1, 2 * time.Minute},
		{"the formula below double doubles", time.Minute, 10, 4, 2 * time.Minute},
		{"one short of the limit doubles", time.Minute, 10, 9, 2 * time.Minute},
		{"none found is x4", time.Minute, 5, 0, 4 * time.Minute},
		{"limit 0 doubles", time.Minute, 0, 5, 2 * time.Minute},
		{"limit 0 with none found doubles", time.Minute, 0, 0, 2 * time.Minute},
		{"the formula clamps", 1000 * time.Hour, math.MaxInt64, 1, longest},
		{"x4 clamps", longest / 2, 5, 0, longest},
		{"doubling clamps", longest, 0, 0, longest},
		{"the longest slice below double stays the longest", longest, 10, 9, longest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextSlice(tc.d, tc.limit, tc.found); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// Growth starts from the slice that ran and counts distinct traces above its
// boundary, across every slice so far.
func TestWalkGrowth(t *testing.T) {
	type parts = [][]model.TraceInfo
	type reads = [][2]time.Duration
	aa := ranked("aa", walkTo.UnixNano(), 1)
	bb := ranked("bb", walkTo.UnixNano()-1, 2)
	over := []clickhouse_transpiler.ShardExtent{row(11, 2)}
	for _, tc := range []struct {
		name     string
		limit    int64
		parts    parts
		attempts [][]clickhouse_transpiler.ShardExtent
		want     reads
	}{
		{
			name: "by the formula", limit: 10, parts: parts{{aa, bb}},
			want: reads{{time.Minute, 0}, {5 * time.Minute, time.Minute}},
		},
		{
			name: "x4 with none found", limit: 10,
			want: reads{{time.Minute, 0}, {5 * time.Minute, time.Minute}, {21 * time.Minute, 5 * time.Minute}},
		},
		{
			name: "from the halved slice", limit: 10, parts: parts{{aa, bb}},
			attempts: [][]clickhouse_transpiler.ShardExtent{over},
			want:     reads{{30 * time.Second, 0}, {150 * time.Second, 30 * time.Second}},
		},
		{
			name: "a slice adding nothing grows by the traces already found", limit: 3, parts: parts{{aa}, nil},
			want: reads{{time.Minute, 0}, {3 * time.Minute, time.Minute}, {7 * time.Minute, 3 * time.Minute}},
		},
		{
			name: "a trace in two slices counts once", limit: 5, parts: parts{{aa}, {aa}},
			want: reads{{time.Minute, 0}, {5 * time.Minute, time.Minute}, {21 * time.Minute, 5 * time.Minute}},
		},
		{
			name: "limit 0 doubles", limit: 0, parts: parts{{aa, bb}},
			want: reads{{time.Minute, 0}, {3 * time.Minute, time.Minute}, {7 * time.Minute, 3 * time.Minute}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walk := newScriptedWalk(tc.parts...)
			walk.next = nextSlice
			walk.withExtents(10, tc.attempts...)
			ctx := &shared.PlannerContext{From: walkTo.Add(-time.Hour), To: walkTo, Limit: tc.limit}
			if _, err := walk.Process(ctx); err != nil {
				t.Fatal(err)
			}
			if len(walk.reads) < len(tc.want) {
				t.Fatalf("%d reads, want at least %d", len(walk.reads), len(tc.want))
			}
			checkReads(t, walk.reads[:len(tc.want)], tc.want...)
		})
	}
}

// A slice length clamped at the longest duration still ends at the range's start.
func TestWalkClampsALongSlice(t *testing.T) {
	walk := newScriptedWalk([]model.TraceInfo{ranked("aa", walkTo.UnixNano(), 1)})
	walk.next = nextSlice
	walk.first = time.Duration(math.MaxInt64) / 3
	ctx := &shared.PlannerContext{From: walkTo.AddDate(-200, 0, 0), To: walkTo, Limit: math.MaxInt64}
	if _, err := walk.Process(ctx); err != nil {
		t.Fatal(err)
	}
	last := walk.reads[len(walk.reads)-1]
	for i, read := range walk.reads {
		if !read.From.Before(read.To) {
			t.Errorf("read %d: (%v, %v]", i, read.From, read.To)
		}
	}
	if !last.From.Equal(ctx.From) {
		t.Errorf("walk ends at %v, want %v", last.From, ctx.From)
	}
}
