package traceql_transpiler

import (
	"cmp"
	"slices"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

const (
	// firstSlice is the length of a walk's newest slice.
	firstSlice = time.Minute
	// minSlice is the shortest length a slice halves to.
	minSlice = time.Second
)

// nextSlice is the length of the slice after one of length d, with found traces
// ranking above its lower boundary at limit.
func nextSlice(d time.Duration, _, _ int64) time.Duration { return 2 * d }

// walkProcessor searches adjacent slices of (ctx.From, ctx.To], newest first,
// until the result is the exact top ctx.Limit by rank.
type walkProcessor struct {
	main  *clickhouse_transpiler.SearchPlan
	first time.Duration
	next  func(d time.Duration, limit, found int64) time.Duration
	// budget is the most candidate spans a slice loads on one shard, 0 for none.
	budget int64
	// extent runs the extent read over ctx's candidates.
	extent func(*shared.PlannerContext) ([]clickhouse_transpiler.ShardExtent, error)
	// read runs the evaluation and Data for one slice.
	read func(*shared.PlannerContext, *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error)
}

func newWalkProcessor(main *clickhouse_transpiler.SearchPlan, budget int64) *walkProcessor {
	return &walkProcessor{
		main: main, first: firstSlice, next: nextSlice, budget: budget,
		extent: clickhouse_transpiler.ReadExtents, read: readTraces,
	}
}

func (w *walkProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	if err := w.main.Check(ctx); err != nil {
		return nil, err
	}
	res, err := w.walk(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(res, func(a, b model.TraceInfo) int {
		s1, _ := strconv.ParseInt(a.StartTimeUnixNano, 10, 64)
		s2, _ := strconv.ParseInt(b.StartTimeUnixNano, 10, 64)
		return cmp.Or(cmp.Compare(s2, s1), cmp.Compare(b.TraceID, a.TraceID))
	})
	ch := make(chan []model.TraceInfo, 1)
	ch <- res
	close(ch)
	return ch, nil
}

// walk reads slices until at least ctx.Limit found traces rank above the lower
// boundary of the slice just read: any trace not found ranks at or below it.
// Limit 0 reads every slice.
func (w *walkProcessor) walk(ctx *shared.PlannerContext) ([]model.TraceInfo, error) {
	var found []model.TraceInfo
	ctx.TracesSlices = []shared.TracesSlice{}
	for to, size := ctx.To, w.first; to.After(ctx.From); {
		from := to.Add(-size)
		if from.Before(ctx.From) {
			from = ctx.From
		}
		ctx.Portions++
		part, from, err := w.readSlice(ctx, from, to)
		if err != nil {
			return nil, err
		}
		found = mergeSlice(found, part, ctx.Limit)
		above := ranksAbove(found, from)
		if ctx.Limit > 0 && above >= ctx.Limit {
			break
		}
		size = w.next(to.Sub(from), ctx.Limit, above)
		to = from
	}
	return found, nil
}

// readSlice evaluates the traces with a leaf match in (from, to] over every
// span they have in the request's range, and over the slice itself, halving
// the slice to fit the budget. It returns the lower boundary of the slice that
// ran, and reads nothing when that slice has no candidates.
func (w *walkProcessor) readSlice(ctx *shared.PlannerContext, from, to time.Time) ([]model.TraceInfo, time.Time, error) {
	sliceCtx := *ctx
	var extents []clickhouse_transpiler.ShardExtent
	for {
		sliceCtx.TracesCandidates = w.main.Candidates(from, to)
		var err error
		if extents, err = w.extent(&sliceCtx); err != nil {
			return nil, from, err
		}
		d := to.Sub(from)
		if d <= minSlice || !w.overBudget(extents) {
			break
		}
		from = to.Add(-max(d/2, minSlice))
	}
	if len(extents) == 0 {
		return nil, from, nil
	}
	sliceCtx.From, sliceCtx.To = evaluatedRange(extents, from, to)
	ctx.TracesSlices = append(ctx.TracesSlices, shared.TracesSlice{
		From: sliceCtx.From, To: sliceCtx.To, Candidates: sliceCtx.TracesCandidates,
	})
	part, err := w.read(&sliceCtx, w.main)
	return part, from, err
}

// overBudget reports whether a shard loads more candidate spans than the budget
// across more than one trace: halving doesn't split a trace.
func (w *walkProcessor) overBudget(extents []clickhouse_transpiler.ShardExtent) bool {
	if w.budget == 0 {
		return false
	}
	return slices.ContainsFunc(extents, func(e clickhouse_transpiler.ShardExtent) bool {
		return e.Spans > w.budget && e.Traces > 1
	})
}

// evaluatedRange is the range holding (from, to] and every extent. A range
// excludes its lower bound, so it starts 1ns before the earliest span.
func evaluatedRange(extents []clickhouse_transpiler.ShardExtent, from, to time.Time) (time.Time, time.Time) {
	lo, hi := extents[0].Lo, extents[0].Hi
	for _, e := range extents[1:] {
		lo, hi = min(lo, e.Lo), max(hi, e.Hi)
	}
	if t := time.Unix(0, lo-1).In(from.Location()); t.Before(from) {
		from = t
	}
	if t := time.Unix(0, hi).In(to.Location()); t.After(to) {
		to = t
	}
	return from, to
}

// mergeSlice adds part to found, part's copy replacing a trace found before,
// and keeps the top limit by rank.
func mergeSlice(found, part []model.TraceInfo, limit int64) []model.TraceInfo {
	at := make(map[string]int, len(found)+len(part))
	for i, trace := range found {
		at[trace.TraceID] = i
	}
	for _, trace := range part {
		if i, ok := at[trace.TraceID]; ok {
			found[i] = trace
			continue
		}
		at[trace.TraceID] = len(found)
		found = append(found, trace)
	}
	return keepTop(found, limit)
}

// ranksAbove counts the traces with a rank strictly above boundary: a span
// starting exactly at boundary belongs to the next slice.
func ranksAbove(traces []model.TraceInfo, boundary time.Time) int64 {
	var res int64
	for _, trace := range traces {
		if trace.Newest > boundary.UnixNano() {
			res++
		}
	}
	return res
}

// keepTop keeps the limit highest ranked traces, by newest matched span then
// trace id, or all of them at limit 0.
func keepTop(traces []model.TraceInfo, limit int64) []model.TraceInfo {
	slices.SortFunc(traces, func(a, b model.TraceInfo) int {
		return cmp.Or(cmp.Compare(b.Newest, a.Newest), cmp.Compare(b.TraceID, a.TraceID))
	})
	if limit > 0 && int64(len(traces)) > limit {
		return traces[:limit]
	}
	return traces
}

// readTraces runs main and collects its traces.
func readTraces(ctx *shared.PlannerContext, main *clickhouse_transpiler.SearchPlan) ([]model.TraceInfo, error) {
	planner := &TraceQLRequestProcessor{main}
	ch, err := planner.Process(ctx)
	if err != nil {
		return nil, err
	}
	var res []model.TraceInfo
	for traces := range ch {
		res = append(res, traces...)
	}
	return res, nil
}
