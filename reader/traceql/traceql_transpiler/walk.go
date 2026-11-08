package traceql_transpiler

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
)

const (
	// firstSlice is the length of a walk's newest slice.
	firstSlice = time.Minute
	// minSlice is the shortest length a slice halves to.
	minSlice = time.Second
)

// nextSlice is the length after a slice of length d with found traces ranked above
// its lower boundary: scaled to reach limit at that rate, x4 with none, at least double.
func nextSlice(d time.Duration, limit, found int64) time.Duration {
	factor := 2.0
	if limit > 0 {
		factor = 4
		if found > 0 {
			factor = max(2, float64(limit-found)/float64(found))
		}
	}
	if scaled := float64(d) * factor; scaled < math.MaxInt64 {
		return time.Duration(scaled)
	}
	return math.MaxInt64
}

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
	// newest returns the start of the newest candidate leaf match in (from, to],
	// false when there is none.
	newest func(ctx *shared.PlannerContext, from, to time.Time) (time.Time, bool, error)
}

func newWalkProcessor(main *clickhouse_transpiler.SearchPlan, budget int64) *walkProcessor {
	return &walkProcessor{
		main: main, first: firstSlice, next: nextSlice, budget: budget,
		extent: clickhouse_transpiler.ReadExtents, read: readTraces,
		newest: func(ctx *shared.PlannerContext, from, to time.Time) (time.Time, bool, error) {
			ns, ok, err := clickhouse_transpiler.ReadNewestCandidate(ctx, main.NewestCandidate(from, to))
			return time.Unix(0, ns).In(to.Location()), ok, err
		},
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
// Limit 0 reads every slice. After the first slice without candidates, the next
// slice ends at the newest candidate left, or the walk ends; after any later one,
// the next slice is the rest of the range.
func (w *walkProcessor) walk(ctx *shared.PlannerContext) ([]model.TraceInfo, error) {
	var found []model.TraceInfo
	var largest int64
	probed := false
	ctx.TracesSlices = []shared.TracesSlice{}
	for to, size := ctx.To, w.first; to.After(ctx.From); {
		// A slice leaving less than its own length takes the rest of the range.
		from := to.Add(-size)
		if from.Sub(ctx.From) < size {
			from = ctx.From
		}
		ctx.Portions++
		part, from, spans, err := w.readSlice(ctx, from, to)
		if err != nil {
			return nil, err
		}
		largest = max(largest, spans)
		found = mergeSlice(found, part, ctx.Limit)
		above := ranksAbove(found, from)
		if ctx.Limit > 0 && above >= ctx.Limit {
			break
		}
		size = w.next(to.Sub(from), ctx.Limit, above)
		to = from
		if spans == 0 && probed {
			size = to.Sub(ctx.From)
		}
		// After the first slice without candidates, jump to the newest candidate left.
		if spans == 0 && !probed && to.After(ctx.From) {
			probed = true
			ctx.Portions++
			newest, ok, err := w.newest(ctx, ctx.From, to)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			to = newest
		}
	}
	if ctx.Portions > 1 {
		logPortioned(portionedLine(ctx, largest, traceqlconfig.PortionSpans(), ctx.Portions))
	}
	return found, nil
}

// readSlice halves (from, to] to fit the budget, then evaluates its candidates, if
// any. It returns their traces, the slice's lower boundary and its largest shard load.
func (w *walkProcessor) readSlice(ctx *shared.PlannerContext, from, to time.Time) ([]model.TraceInfo, time.Time, int64, error) {
	sliceCtx := *ctx
	var extents []clickhouse_transpiler.ShardExtent
	for {
		sliceCtx.TracesCandidates = w.main.Candidates(from, to)
		var err error
		if extents, err = w.extent(&sliceCtx); err != nil {
			return nil, from, 0, err
		}
		d := to.Sub(from)
		if d <= minSlice || !w.overBudget(extents) {
			break
		}
		from = to.Add(-max(d/2, minSlice))
	}
	if len(extents) == 0 {
		return nil, from, 0, nil
	}
	sliceCtx.From, sliceCtx.To = evaluatedRange(extents, from, to)
	ctx.TracesSlices = append(ctx.TracesSlices, shared.TracesSlice{
		From: sliceCtx.From, To: sliceCtx.To, Candidates: sliceCtx.TracesCandidates,
	})
	part, err := w.read(&sliceCtx, w.main)
	return part, from, largestLoad(extents), err
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

// largestLoad is the most candidate spans of any shard.
func largestLoad(extents []clickhouse_transpiler.ShardExtent) int64 {
	var res int64
	for _, e := range extents {
		res = max(res, e.Spans)
	}
	return res
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
