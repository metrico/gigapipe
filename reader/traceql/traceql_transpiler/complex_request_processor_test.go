package traceql_transpiler

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
)

type portionRead struct {
	from   time.Time
	filter shared.RandomFilter
	ids    []string
}

// fakeTracesRead records each portion's context and answers the portion with
// traceCount traces that start an hour before the portion's from.
func fakeTracesRead(traceCount int, reads *[]portionRead) func(*shared.PlannerContext, shared.SQLRequestPlanner) ([]model.TraceInfo, error) {
	return func(ctx *shared.PlannerContext, _ shared.SQLRequestPlanner) ([]model.TraceInfo, error) {
		i := len(*reads)
		*reads = append(*reads, portionRead{ctx.From, ctx.RandomFilter, slices.Clone(ctx.CachedTraceIds)})
		res := make([]model.TraceInfo, traceCount)
		for j := range res {
			res[j] = model.TraceInfo{
				TraceID:           fmt.Sprintf("p%dt%d", i, j),
				StartTimeUnixNano: strconv.FormatInt(ctx.From.Add(-time.Hour).UnixNano(), 10),
			}
		}
		return res, nil
	}
}

func portionedTraces(read func(*shared.PlannerContext, shared.SQLRequestPlanner) ([]model.TraceInfo, error)) *TraceQLComplexityEvaluator[model.TraceInfo] {
	return &TraceQLComplexityEvaluator[model.TraceInfo]{
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
			var got []model.TraceInfo
			for traces := range ch {
				got = append(got, traces...)
			}

			if len(reads) != 3 {
				t.Fatalf("got %d portions, want 3", len(reads))
			}
			wantIDs := func(portion int) []string {
				var ids []string
				for j := range traceCount {
					ids = append(ids, fmt.Sprintf("p%dt%d", portion, j))
				}
				return ids
			}
			for i, read := range reads {
				if !read.from.Equal(requested) {
					t.Errorf("portion %d: from %v, want %v", i, read.from, requested)
				}
				if want := (shared.RandomFilter{Max: 3, I: i}); read.filter != want {
					t.Errorf("portion %d: filter %+v, want %+v", i, read.filter, want)
				}
				var want []string
				if i > 0 {
					want = wantIDs(i - 1)
				}
				if !slices.Equal(read.ids, want) {
					t.Errorf("portion %d: carried ids %v, want %v", i, read.ids, want)
				}
			}
			var gotIDs []string
			for _, trace := range got {
				gotIDs = append(gotIDs, trace.TraceID)
			}
			if !slices.Equal(gotIDs, wantIDs(2)) {
				t.Errorf("result %v, want the last portion's %v", gotIDs, wantIDs(2))
			}
			if !ctx.From.Equal(requested) {
				t.Errorf("context from %v, want %v", ctx.From, requested)
			}
		})
	}
}

func TestPortionReadErrorFailsTheRequest(t *testing.T) {
	boom := errors.New("boom")
	read := func(*shared.PlannerContext, shared.SQLRequestPlanner) ([]model.TraceInfo, error) {
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

func (p *portionCountRecorder) SetMain(shared.SQLRequestPlanner) {}

func TestPortionCountRoundsComplexityUpToThreshold(t *testing.T) {
	for _, tc := range []struct{ complexity, want int64 }{{10, 1}, {11, 2}, {25, 3}} {
		processor := &portionCountRecorder{}
		evaluator := &TraceQLComplexityEvaluator[string]{threshold: 10, complexRequestProcessor: processor}
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

func (s *simpleRequestRecorder) SetMain(shared.SQLRequestPlanner) {}

func TestRequestPortionsOnlyOverTheBudget(t *testing.T) {
	for _, tc := range []struct {
		estimate, portions int64
		simpleCalls        int
	}{{9, 0, 1}, {10, 0, 1}, {11, 2, 0}, {20, 2, 0}, {21, 3, 0}} {
		simple, portioned := &simpleRequestRecorder{}, &portionCountRecorder{}
		evaluator := &TraceQLComplexityEvaluator[string]{
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
	evaluator := &TraceQLComplexityEvaluator[string]{threshold: 10, complexRequestProcessor: &portionCountRecorder{}}
	if _, err := evaluator.processEstimated(&shared.PlannerContext{}, 11); err != nil {
		t.Fatal(err)
	}
	if len(*lines) != 1 || !strings.Contains((*lines)[0], "portions=2") {
		t.Errorf("got %q, want one line with 2 portions", *lines)
	}
}

func TestRequestWithinTheBudgetLogsNothing(t *testing.T) {
	lines := recordPortionedLines(t)
	evaluator := &TraceQLComplexityEvaluator[string]{threshold: 10, simpleRequestProcessor: &simpleRequestRecorder{}}
	for _, estimate := range []int64{9, 10} {
		if _, err := evaluator.processEstimated(&shared.PlannerContext{}, estimate); err != nil {
			t.Fatal(err)
		}
	}
	if len(*lines) != 0 {
		t.Errorf("got %q, want no line", *lines)
	}
}
