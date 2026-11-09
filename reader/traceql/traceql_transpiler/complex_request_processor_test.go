package traceql_transpiler

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type portionRead struct {
	from   time.Time
	filter shared.RandomFilter
}

// fakeTracesRead records each portion's context and answers the portion with
// traceCount traces.
func fakeTracesRead(traceCount int, reads *[]portionRead) func(*shared.PlannerContext, *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
	return func(ctx *shared.PlannerContext, _ *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
		i := len(*reads)
		*reads = append(*reads, portionRead{ctx.From, ctx.RandomFilter})
		res := make([]model.TraceInfo, traceCount)
		for j := range res {
			res[j] = model.TraceInfo{TraceID: fmt.Sprintf("p%dt%d", i, j), StartTimeUnixNano: "1"}
		}
		return res, nil
	}
}

func portionedTraces(read func(*shared.PlannerContext, *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error)) *TraceQLComplexityEvaluator[model.TraceInfo, *clickhouse_transpiler.SearchPlan] {
	return &TraceQLComplexityEvaluator[model.TraceInfo, *clickhouse_transpiler.SearchPlan]{
		threshold:               10,
		complexRequestProcessor: &ComplexRequestProcessor{read: read},
	}
}

func TestPortionsQueryTheRequestedFrom(t *testing.T) {
	const limit = 2
	requested := time.Unix(86390, 0).UTC()
	for _, traceCount := range []int{limit - 1, limit} {
		t.Run(fmt.Sprintf("%d of limit %d traces", traceCount, limit), func(t *testing.T) {
			var reads []portionRead
			ctx := &shared.PlannerContext{From: requested, Limit: limit}
			ch, err := portionedTraces(fakeTracesRead(traceCount, &reads)).ProcessComplexReq(ctx, 25)
			if err != nil {
				t.Fatal(err)
			}
			for range ch {
			}

			if len(reads) != 3 {
				t.Fatalf("got %d portions, want 3", len(reads))
			}
			for i, read := range reads {
				if !read.from.Equal(requested) {
					t.Errorf("portion %d: from %v, want %v", i, read.from, requested)
				}
				if want := (shared.RandomFilter{Max: 3, I: i}); read.filter != want {
					t.Errorf("portion %d: filter %+v, want %+v", i, read.filter, want)
				}
			}
			if !ctx.From.Equal(requested) {
				t.Errorf("context from %v, want %v", ctx.From, requested)
			}
		})
	}
}

// ranked is a trace with a search rank and a start.
func ranked(id string, newest, start int64) model.TraceInfo {
	return model.TraceInfo{TraceID: id, Newest: newest, StartTimeUnixNano: strconv.FormatInt(start, 10)}
}

// mergedPortions runs a request whose portions answer with parts: its trace ids.
func mergedPortions(t *testing.T, limit int64, parts ...[]model.TraceInfo) []string {
	t.Helper()
	read := func(ctx *shared.PlannerContext, _ *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
		return parts[ctx.RandomFilter.I], nil
	}
	ctx := &shared.PlannerContext{Limit: limit}
	ch, err := portionedTraces(read).ProcessComplexReq(ctx, int64(len(parts))*10)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.TotalJobs() != int64(len(parts)) {
		t.Errorf("TotalJobs %d, want %d", ctx.TotalJobs(), len(parts))
	}
	ids := []string{}
	for traces := range ch {
		for _, trace := range traces {
			ids = append(ids, trace.TraceID)
		}
	}
	return ids
}

func TestPortionsMergeByRank(t *testing.T) {
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
			name:  "a later portion displaces a lower rank",
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
			name:  "returns the top limit across portions",
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
			name:  "an empty portion adds nothing",
			limit: 2,
			parts: [][]model.TraceInfo{{}, {ranked("aa", 1, 1)}, nil},
			want:  []string{"aa"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mergedPortions(t, tc.limit, tc.parts...); !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
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

// Traces with one start sort by trace id, whatever their rank and portion.
func TestPortionsWithEqualStartsSortByTraceID(t *testing.T) {
	const traces = 200
	var parts [3][]model.TraceInfo
	want := make([]string, traces)
	for i := range traces {
		id := fmt.Sprintf("t%03d", i)
		want[traces-1-i] = id
		parts[i%3] = append(parts[i%3], ranked(id, int64(traces-i), 1))
	}
	if got := mergedPortions(t, traces, parts[:]...); !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPortionReadErrorFailsTheRequest(t *testing.T) {
	boom := errors.New("boom")
	read := func(*shared.PlannerContext, *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
		return nil, boom
	}
	_, err := portionedTraces(read).ProcessComplexReq(&shared.PlannerContext{}, 25)
	if !errors.Is(err, boom) {
		t.Fatalf("got error %v, want %v", err, boom)
	}
}

type portionCountRecorder struct {
	portions int64
}

func (p *portionCountRecorder) Process(_ *shared.PlannerContext, portions int64) (chan []string, error) {
	p.portions = portions
	return nil, nil
}

func (p *portionCountRecorder) SetMain(*clickhouse_transpiler.SearchPlan) {}

func TestPortionCountRoundsComplexityUpToThreshold(t *testing.T) {
	for _, tc := range []struct{ complexity, want int64 }{{10, 1}, {11, 2}, {25, 3}} {
		processor := &portionCountRecorder{}
		evaluator := &TraceQLComplexityEvaluator[string, *clickhouse_transpiler.SearchPlan]{threshold: 10, complexRequestProcessor: processor}
		if _, err := evaluator.ProcessComplexReq(&shared.PlannerContext{}, tc.complexity); err != nil {
			t.Fatal(err)
		}
		if processor.portions != tc.want {
			t.Errorf("complexity %d: got %d portions, want %d", tc.complexity, processor.portions, tc.want)
		}
	}
}

func TestComplexRequestRecordsPortions(t *testing.T) {
	var reads []portionRead
	ctx := &shared.PlannerContext{Limit: 2}
	ch, err := portionedTraces(fakeTracesRead(1, &reads)).ProcessComplexReq(ctx, 25)
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	if ctx.Portions != 3 || len(reads) != 3 {
		t.Errorf("Portions = %d, reads = %d, want 3", ctx.Portions, len(reads))
	}
}

type simpleRequestRecorder struct {
	calls int
}

func (s *simpleRequestRecorder) Process(*shared.PlannerContext) (chan []string, error) {
	s.calls++
	return nil, nil
}

func (s *simpleRequestRecorder) SetMain(*clickhouse_transpiler.SearchPlan) {}

func TestRequestPortionsOnlyOverTheBudget(t *testing.T) {
	for _, tc := range []struct {
		estimate, portions, totalJobs int64
		simpleCalls                   int
	}{{9, 0, 1, 1}, {10, 0, 1, 1}, {11, 2, 2, 0}, {20, 2, 2, 0}, {21, 3, 3, 0}} {
		simple, portioned := &simpleRequestRecorder{}, &portionCountRecorder{}
		evaluator := &TraceQLComplexityEvaluator[string, *clickhouse_transpiler.SearchPlan]{
			threshold: 10, simpleRequestProcessor: simple, complexRequestProcessor: portioned,
		}
		ctx := &shared.PlannerContext{}
		if _, err := evaluator.processEstimated(ctx, tc.estimate); err != nil {
			t.Fatal(err)
		}
		if simple.calls != tc.simpleCalls || portioned.portions != tc.portions || ctx.Portions != tc.portions {
			t.Errorf("estimate %d: simple calls %d, read in %d portions, ctx.Portions %d; want %d, %d, %d",
				tc.estimate, simple.calls, portioned.portions, ctx.Portions, tc.simpleCalls, tc.portions, tc.portions)
		}
		if ctx.TotalJobs() != tc.totalJobs {
			t.Errorf("estimate %d: TotalJobs %d, want %d", tc.estimate, ctx.TotalJobs(), tc.totalJobs)
		}
	}
}

// estimateRows returns one row per count, then ends with err, or cleanly without one.
type estimateRows struct {
	counts []int64
	err    error
}

func (r *estimateRows) Columns() []string { return []string{"_count"} }
func (r *estimateRows) Close() error      { return nil }
func (r *estimateRows) Next(dest []driver.Value) error {
	if len(r.counts) == 0 {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	dest[0], r.counts = r.counts[0], r.counts[1:]
	return nil
}

type estimateConn struct {
	driver.Conn
	counts []int64
	err    error
}

func (c estimateConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &estimateRows{counts: slices.Clone(c.counts), err: c.err}, nil
}

func (c estimateConn) Close() error { return nil }

type estimateConnector struct {
	counts []int64
	err    error
}

func (c estimateConnector) Connect(context.Context) (driver.Conn, error) {
	return estimateConn{counts: c.counts, err: c.err}, nil
}

func (c estimateConnector) Driver() driver.Driver { return nil }

type estimateDB struct {
	model.ISqlxDB
	db *sql.DB
}

func (d estimateDB) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}

type selectOne struct{}

func (selectOne) Process(*shared.PlannerContext) (sql_select.ISelect, error) {
	return sql_select.NewSelect().Select(sql_select.NewRawObject("1")), nil
}

func TestEstimateIsTheFullestShard(t *testing.T) {
	for _, tc := range []struct {
		name        string
		counts      []int64
		portions    int64
		simpleCalls int
	}{
		{name: "the fullest shard is neither first nor last", counts: []int64{0, 7, 3}, portions: 4},
		{name: "the fullest shard is first", counts: []int64{7, 0}, portions: 4},
		{name: "the fullest shard follows a smaller one", counts: []int64{3, 7, 0}, portions: 4},
		{name: "no rows read at once", simpleCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(estimateConnector{counts: tc.counts})
			t.Cleanup(func() { _ = db.Close() })
			simple, portioned := &simpleRequestRecorder{}, &portionCountRecorder{}
			evaluator := &TraceQLComplexityEvaluator[string, *clickhouse_transpiler.SearchPlan]{
				threshold: 2, simpleRequestProcessor: simple, complexRequestProcessor: portioned,
				evaluateComplexityPlanner: selectOne{},
			}
			ctx := &shared.PlannerContext{Ctx: context.Background(), CHDb: estimateDB{db: db}}
			if _, err := evaluator.Process(ctx); err != nil {
				t.Fatal(err)
			}
			if portioned.portions != tc.portions || simple.calls != tc.simpleCalls {
				t.Errorf("read in %d portions, %d simple calls; want %d, %d",
					portioned.portions, simple.calls, tc.portions, tc.simpleCalls)
			}
		})
	}
}

func TestEstimateReadFailureFailsTheRequest(t *testing.T) {
	failure := errors.New("shard failed")
	db := sql.OpenDB(estimateConnector{counts: []int64{1}, err: failure})
	t.Cleanup(func() { _ = db.Close() })
	simple, portioned := &simpleRequestRecorder{}, &portionCountRecorder{}
	evaluator := &TraceQLComplexityEvaluator[string, *clickhouse_transpiler.SearchPlan]{
		threshold: 10, simpleRequestProcessor: simple, complexRequestProcessor: portioned,
		evaluateComplexityPlanner: selectOne{},
	}
	ctx := &shared.PlannerContext{Ctx: context.Background(), CHDb: estimateDB{db: db}}
	if _, err := evaluator.Process(ctx); !errors.Is(err, failure) {
		t.Errorf("Process returned %v, want %v", err, failure)
	}
	if simple.calls != 0 || portioned.portions != 0 {
		t.Errorf("read the request after the estimate failed: %d simple calls, %d portions", simple.calls, portioned.portions)
	}
}

func recordPortionedLines(t *testing.T) *[]string {
	t.Helper()
	var lines []string
	original := logPortioned
	logPortioned = func(line string) { lines = append(lines, line) }
	t.Cleanup(func() { logPortioned = original })
	return &lines
}

func TestPortionedRequestLogsOneLine(t *testing.T) {
	lines := recordPortionedLines(t)
	var reads []portionRead
	ctx := &shared.PlannerContext{
		Limit: 2, TraceQL: `{.a = "1"} >> {.b}`,
		From: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), To: time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC),
	}
	ch, err := portionedTraces(fakeTracesRead(0, &reads)).processEstimated(ctx, 25)
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	want := `traceql portioned query="{.a = \"1\"} >> {.b}" from=2026-01-02T03:04:05Z to=2026-01-02T04:04:05Z ` +
		`estimate=25 portion_spans=10 portions=3`
	if len(*lines) != 1 || (*lines)[0] != want {
		t.Errorf("got %q, want one line %q", *lines, want)
	}
}

func TestPortionedAutocompleteLogsOneLine(t *testing.T) {
	lines := recordPortionedLines(t)
	evaluator := &TraceQLComplexityEvaluator[string, *clickhouse_transpiler.SearchPlan]{threshold: 10, complexRequestProcessor: &portionCountRecorder{}}
	if _, err := evaluator.processEstimated(&shared.PlannerContext{}, 11); err != nil {
		t.Fatal(err)
	}
	if len(*lines) != 1 || !strings.Contains((*lines)[0], "portions=2") {
		t.Errorf("got %q, want one line with 2 portions", *lines)
	}
}

func TestRequestWithinTheBudgetLogsNothing(t *testing.T) {
	lines := recordPortionedLines(t)
	evaluator := &TraceQLComplexityEvaluator[string, *clickhouse_transpiler.SearchPlan]{threshold: 10, simpleRequestProcessor: &simpleRequestRecorder{}}
	for _, estimate := range []int64{9, 10} {
		if _, err := evaluator.processEstimated(&shared.PlannerContext{}, estimate); err != nil {
			t.Fatal(err)
		}
	}
	if len(*lines) != 0 {
		t.Errorf("got %q, want no line", *lines)
	}
}
