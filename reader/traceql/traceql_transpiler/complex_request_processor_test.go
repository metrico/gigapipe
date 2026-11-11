package traceql_transpiler

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
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
