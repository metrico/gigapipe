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
		main: &clickhouse_transpiler.SearchPlan{}, first: time.Minute, next: nextSlice,
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
