package traceql_transpiler

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// returnedTraceIDs runs query: the trace ids it returns, failing on an error
// or a repeated id.
func returnedTraceIDs(t *testing.T, s *searchStore, query string, run searchRun) []string {
	t.Helper()
	return traceIDLines(t, query, run, s.result(t, query, run))
}

func traceIDLines(t *testing.T, query string, run searchRun, lines []string) []string {
	t.Helper()
	ids := []string{}
	seen := map[string]bool{}
	for _, line := range lines {
		if strings.HasPrefix(line, "error: ") {
			t.Fatalf("%s at boundary %d: %s", query, run.boundary, line)
		}
		if rest, ok := strings.CutPrefix(line, "trace "); ok {
			id := strings.Fields(rest)[0]
			if seen[id] {
				t.Errorf("%s at boundary %d: trace %s returned twice", query, run.boundary, id)
			}
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

// A trace with spans either side of the fillers gets the same result wherever
// a slice ends.
func testSearchSameAtAnySliceBoundary(t *testing.T, cluster bool) {
	for _, x := range []int{1000, 1001, 1002} {
		t.Run(strconv.Itoa(x), func(t *testing.T) {
			s := openSearchStore(t, cluster, false)
			spans := []span{{x, 1, 0, []string{"a", "c"}}}
			for i := range 30 {
				spans = append(spans,
					span{2000 + i, 1, 0, []string{"a", "c"}}, span{2000 + i, 2, 1, []string{"b", "c"}},
					span{2000 + i, 3, 1, []string{"c"}}, span{3000 + i, 1, 0, []string{"b"}})
			}
			spans = append(spans, span{x, 2, 1, []string{"b", "c"}}, span{x, 3, 1, []string{"c"}})
			s.seed(t, spans)
			hexX := fmt.Sprintf("%x", x)
			for _, tc := range []struct {
				query string
				hasX  bool
			}{
				{`{.a = "1"} >> {.b = "1"}`, true},
				{`{.a = "1"} && {.b = "1"}`, true},
				{`{.a = "1"} !>> {.b = "1"}`, false},
				{`{.c = "1"} | count() > 2`, true},
				{`{.a != nil} >> {.b != nil}`, true},
				{`{.a = "1"} || {.b = "1"}`, true},
				{`{.c = "1"} | by(.a)`, true},
				{`{.c = "1"} | select(.a)`, true},
				{`{} >> {.b = "1"}`, true},
				{`{} | count() > 2`, true},
			} {
				base := searchRun{limit: 5}
				want := s.result(t, tc.query, base)
				ids := traceIDLines(t, tc.query, base, want)
				if len(ids) != 5 || slices.Contains(ids, hexX) != tc.hasX {
					t.Errorf("%s: got %q, want 5 traces, trace %s %v", tc.query, ids, hexX, tc.hasX)
				}
				for _, boundary := range []int{1, 3, 60} {
					run := searchRun{limit: 5, boundary: boundary}
					got := s.result(t, tc.query, run)
					traceIDLines(t, tc.query, run, got)
					if !slices.Equal(got, want) {
						t.Errorf("%s at boundary %d:\n%q\nwant\n%q", tc.query, boundary, got, want)
					}
				}
			}
		})
	}
}

func TestSearchSameAtAnySliceBoundary(t *testing.T) { testSearchSameAtAnySliceBoundary(t, false) }

func TestSearchSameAtAnySliceBoundaryOnCluster(t *testing.T) {
	testSearchSameAtAnySliceBoundary(t, true)
}

// A lone {} returns the same top wherever a slice ends.
func testSearchAllSpansAcrossSlices(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	var spans []span
	for i := 1; i <= 12; i++ {
		spans = append(spans, span{i, 1, 0, []string{"a"}})
	}
	s.seed(t, spans)
	want := []string{"c", "b", "a", "9", "8"}
	for _, query := range []string{`{}`, `{} | select(.a)`} {
		for _, boundary := range []int{0, 1, 3, 9} {
			run := searchRun{limit: 5, boundary: boundary}
			if got := returnedTraceIDs(t, s, query, run); !slices.Equal(got, want) {
				t.Errorf("%s at boundary %d: got %q, want %q", query, boundary, got, want)
			}
		}
	}
}

func TestSearchAllSpansAcrossSlices(t *testing.T) { testSearchAllSpansAcrossSlices(t, false) }

func TestSearchAllSpansAcrossSlicesOnCluster(t *testing.T) { testSearchAllSpansAcrossSlices(t, true) }

// A large limit returns every match wherever a slice ends.
func testSearchLargeLimitAcrossSlices(t *testing.T, cluster bool) {
	const traces = 12000
	s := openSearchStore(t, cluster, false)
	spans := make([]span, 0, 2*traces)
	for i := 1; i <= traces; i++ {
		spans = append(spans, span{i, 1, 0, []string{"a"}}, span{i, 2, 1, []string{"b"}})
	}
	s.seed(t, spans)
	for _, query := range []string{`{.a = "1"} >> {.b = "1"}`, `{.a = "1"}`} {
		for _, boundary := range []int{0, 3} {
			run := searchRun{limit: 20000, boundary: boundary}
			if got := returnedTraceIDs(t, s, query, run); len(got) != traces {
				t.Errorf("%s at boundary %d: got %d traces, want %d", query, boundary, len(got), traces)
			}
		}
	}
}

func TestSearchLargeLimitAcrossSlices(t *testing.T) { testSearchLargeLimitAcrossSlices(t, false) }

func TestSearchLargeLimitAcrossSlicesOnCluster(t *testing.T) {
	testSearchLargeLimitAcrossSlices(t, true)
}

// At limit 1 the newer matched span wins over the later start.
func testSearchLimitOneRanksByNewestMatch(t *testing.T, cluster bool) {
	for _, tc := range []struct {
		newest, laterStart int
		want               string
	}{{1001, 1002, "3e9"}, {1002, 1001, "3ea"}} {
		t.Run(tc.want, func(t *testing.T) {
			s := openSearchStore(t, cluster, false)
			s.seed(t, []span{
				{tc.newest, 1, 0, []string{"a"}},
				{tc.laterStart, 1, 0, []string{"a"}},
				{tc.newest, 2, 1, []string{"a"}},
			})
			for _, boundary := range []int{0, 1, 3} {
				run := searchRun{limit: 1, boundary: boundary}
				got := returnedTraceIDs(t, s, `{.a = "1"}`, run)
				if !slices.Equal(got, []string{tc.want}) {
					t.Errorf("boundary %d: got %q, want %s", boundary, got, tc.want)
				}
			}
		})
	}
}

func TestSearchLimitOneRanksByNewestMatch(t *testing.T) {
	testSearchLimitOneRanksByNewestMatch(t, false)
}

func TestSearchLimitOneRanksByNewestMatchOnCluster(t *testing.T) {
	testSearchLimitOneRanksByNewestMatch(t, true)
}

// Traces with one start come by trace id, descending, wherever a slice ends.
func testSearchEqualStartsSortByTraceID(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	const traces = 30
	var spans []span
	want := make([]string, traces)
	for i := range traces {
		trace := 1000 + i
		spans = append(spans, span{trace, 1, 0, []string{"a"}}, span{trace, 2, 1, []string{"b"}})
		want[traces-1-i] = fmt.Sprintf("%x", trace)
	}
	s.seedStarts(t, spans, func(int) int64 { return spanStart.UnixNano() })
	for _, query := range []string{`{.a = "1"}`, `{.a = "1"} >> {.b = "1"}`} {
		for _, limit := range []int{searchLimit, 5} {
			for _, boundary := range []int{0, 3} {
				run := searchRun{limit: limit, boundary: boundary}
				got := returnedTraceIDs(t, s, query, run)
				if wantTop := want[:min(limit, traces)]; !slices.Equal(got, wantTop) {
					t.Errorf("%s limit %d at boundary %d: got %q, want %q", query, limit, boundary, got, wantTop)
				}
			}
		}
	}
}

func TestSearchEqualStartsSortByTraceID(t *testing.T) { testSearchEqualStartsSortByTraceID(t, false) }

func TestSearchEqualStartsSortByTraceIDOnCluster(t *testing.T) {
	testSearchEqualStartsSortByTraceID(t, true)
}

// tagValuesAt runs a tag values request for key in the given portions: its
// sorted values.
func tagValuesAt(t *testing.T, s *searchStore, query, key string, portions int) []string {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := PlanValuesV2(q, key)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &shared.PlannerContext{
		Ctx: context.Background(), CHDb: s.db, IsCluster: s.cluster != "",
		// To is an hour past the seeded spans, inside tempo_traces_kv's date bounds.
		From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Hour),
		Limit:       searchLimit * 2,
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	distconfig.Init()
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
		Name: s.name, ClusterName: s.cluster,
	}})
	portioned := planner.(interface {
		ProcessComplexReq(*shared.PlannerContext, int64) (chan []string, error)
	})
	ch, err := portioned.ProcessComplexReq(ctx, int64(portions)*traceqlconfig.PortionSpans())
	if err != nil {
		t.Fatal(err)
	}
	res := []string{}
	for part := range ch {
		res = append(res, part...)
	}
	slices.Sort(res)
	return res
}

// Tag values under a structural operator come from every portion, including
// traces with spans either side of the fillers.
func testTagValuesSameAtAnyPortionCount(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	stragglers := []int{1000, 1001, 1002}
	var spans []span
	want := []string{}
	for _, x := range stragglers {
		spans = append(spans, span{x, 1, 0, []string{"a"}})
	}
	for i := 1; i <= 40; i++ {
		spans = append(spans, span{i, 1, 0, []string{"a"}}, span{i, 100 + i, 1, []string{"b"}})
		want = append(want, fmt.Sprintf("s%d", 100+i))
	}
	for _, x := range stragglers {
		spans = append(spans, span{x, x, 1, []string{"b"}})
		want = append(want, fmt.Sprintf("s%d", x))
	}
	slices.Sort(want)
	s.seed(t, spans)
	for _, portions := range []int{1, 3} {
		got := tagValuesAt(t, s, `{.a = "1"} >> {.b = "1"}`, "name", portions)
		if !slices.Equal(got, want) {
			t.Errorf("%d portions: got %q, want %q", portions, got, want)
		}
	}
}

func TestTagValuesSameAtAnyPortionCount(t *testing.T) { testTagValuesSameAtAnyPortionCount(t, false) }

func TestTagValuesSameAtAnyPortionCountOnCluster(t *testing.T) {
	testTagValuesSameAtAnyPortionCount(t, true)
}
