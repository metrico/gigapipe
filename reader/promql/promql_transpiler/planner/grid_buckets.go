package planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// gridCol is one bucket aggregate: its parts over the interior cells and the
// raw edge samples, selected as p_<alias>, and merge, combining them.
type gridCol struct {
	alias            string
	cell, raw, merge string
}

// gridWindow is a range function's window on its grid: buckets (K-w, K], K =
// phase + k*w, with w = GridEdgeMs(Grid, Range).
type gridWindow struct {
	Grid  Grid
	Range time.Duration
}

func (w gridWindow) width() int64 {
	return GridEdgeMs(w.Grid, w.Range.Milliseconds())
}

// parts returns gridParts over (ctx.From - Range, ctx.To], keyed on the
// window's buckets.
func (w gridWindow) parts(ctx *shared.PlannerContext, fp shared.SQLRequestPlanner) (cells, edges sql.ISelect,
	err error) {
	readCtx := *ctx
	readCtx.From = ctx.From.Add(-w.Range)
	return gridParts(&readCtx, fp, w.Grid.PhaseMs, w.width())
}

// filled densifies main's buckets onto the window's keys, Range past each.
func (w gridWindow) filled(ctx *shared.PlannerContext, main shared.SQLRequestPlanner,
	cols []string) (sql.ISelect, error) {
	return (&FillGapsPlanner{
		Main:       main,
		Duration:   w.Range,
		Resolution: time.Duration(w.width()) * time.Millisecond,
		ValueCols:  cols,
	}).Process(ctx)
}

// gridBuckets reads one row per fingerprint and non-empty bucket of the
// window, with the merged Cols.
type gridBuckets struct {
	Fp     shared.SQLRequestPlanner
	Window gridWindow
	Cols   []gridCol
}

func (g *gridBuckets) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	cells, edges, err := g.Window.parts(ctx, g.Fp)
	if err != nil {
		return nil, err
	}
	key := sql.NewSimpleCol(gridKey(gridMsCol, g.Window.Grid.PhaseMs, g.Window.width()), "timestamp_ms")
	cellCols := []sql.SQLObject{sql.NewSimpleCol("samples.fingerprint", "fingerprint"), key}
	rawCols := []sql.SQLObject{sql.NewSimpleCol("samples.fingerprint", "fingerprint"), key}
	outCols := []sql.SQLObject{sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")}
	for _, c := range g.Cols {
		cellCols = append(cellCols, sql.NewSimpleCol(c.cell, "p_"+c.alias))
		rawCols = append(rawCols, sql.NewSimpleCol(c.raw, "p_"+c.alias))
		outCols = append(outCols, sql.NewSimpleCol(c.merge, c.alias))
	}
	byBucket := []sql.SQLObject{sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")}
	cells.Select(cellCols...).GroupBy(byBucket...).OrderBy()
	edges.Select(rawCols...).GroupBy(byBucket...).OrderBy()
	return sql.NewSelect().With(fpWith(edges)...).Select(outCols...).
		From(&unionAll{cells, []sql.ISelect{edges}}).
		GroupBy(byBucket...), nil
}

// gridBucketedValues is bucketedValues on a grid window.
func gridBucketedValues(ctx *shared.PlannerContext, fp shared.SQLRequestPlanner, w gridWindow,
	cols ...gridCol) (sql.ISelect, error) {
	aliases := make([]string, len(cols))
	for i, c := range cols {
		aliases[i] = c.alias
	}
	return w.filled(ctx, &gridBuckets{Fp: fp, Window: w, Cols: cols}, aliases)
}

// cellLastMsCol is the timestamp of a metrics_15s cell's newest sample: bytes
// 11..18 of its argMax(value, timestamp_ns) state.
const cellLastMsCol = "intDiv(reinterpretAsInt64(substring(CAST(argMaxMergeState(samples.last) AS String), 11, 8)), 1000000)"

// gridSeqStep renders what one pair of consecutive sample values, prev then
// cur, adds to a bucket's d.
type gridSeqStep func(prev, cur string) string

// gridSequence reads a selector as samples, the newest of each cell and every
// raw edge sample, one row per non-empty bucket of the window: its first and
// last sample, its sample count cnt, and d, the Step sum over the pairs ending
// in it, of which first_d is the pair ending in its first sample.
type gridSequence struct {
	Fp     shared.SQLRequestPlanner
	Window gridWindow
	Step   gridSeqStep
}

func (g *gridSequence) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	cells, edges, err := g.Window.parts(ctx, g.Fp)
	if err != nil {
		return nil, err
	}
	cells.Select(
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		sql.NewSimpleCol(cellLastMsCol, "ts_ms"),
		sql.NewSimpleCol("argMaxMerge(samples.last)", "v"),
		sql.NewSimpleCol("toFloat64(countMerge(samples.count))", "n"),
	).GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("samples.timestamp_ns")).OrderBy()
	edges.Select(
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		sql.NewSimpleCol(gridMsCol, "ts_ms"),
		sql.NewSimpleCol("samples.value", "v"),
		sql.NewSimpleCol("toFloat64(1)", "n"),
	).OrderBy()

	buckets := sql.NewSelect().With(fpWith(edges)...).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol(gridKey("ts_ms", g.Window.Grid.PhaseMs, g.Window.width()), "timestamp_ms"),
		sql.NewSimpleCol("arraySort(groupArray((ts_ms, v)))", "pts"),
		sql.NewSimpleCol("pts[1].2", "first_v"),
		sql.NewSimpleCol("pts[1].1", "first_ts"),
		sql.NewSimpleCol("pts[-1].2", "last_v"),
		sql.NewSimpleCol("pts[-1].1", "last_ts"),
		sql.NewSimpleCol("sum(n)", "cnt"),
		sql.NewSimpleCol(fmt.Sprintf("arraySum(arrayMap((p, c) -> %s, arrayPopBack(pts), arrayPopFront(pts)))",
			g.Step("p.2", "c.2")), "d_in"),
	).From(&unionAll{cells, []sql.ISelect{edges}}).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms"))
	withBuckets := sql.NewWith(buckets, "seq_buckets")

	prevWnd := &sql.WindowFunction{
		Alias:       "seq_prev_wnd",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Rows:        true,
		Start:       sql.WindowPoint{Offset: 1},
		End:         sql.WindowPoint{},
	}
	return sql.NewSelect().With(withBuckets).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol("first_v", "first_v"),
		sql.NewSimpleCol("first_ts", "first_ts"),
		sql.NewSimpleCol("last_v", "last_v"),
		sql.NewSimpleCol("last_ts", "last_ts"),
		sql.NewSimpleCol("cnt", "cnt"),
		sql.NewCol(overWnd(sql.NewRawObject("lagInFrame(last_v, 1, first_v)"), prevWnd), "prev_v"),
		sql.NewSimpleCol(g.Step("prev_v", "first_v"), "first_d"),
		sql.NewSimpleCol("d_in + first_d", "d"),
	).From(sql.NewWithRef(withBuckets)).AddWindows(prevWnd), nil
}

// gridSequenceWindows evaluates, at every key of w, the window (T-range, T] of
// the selector's gridSequence: first and last sample (f_first_v, f_first_ts,
// f_last_v, f_last_ts), sample count f_cnt, and f_d, the step sum inside it.
func gridSequenceWindows(ctx *shared.PlannerContext, fp shared.SQLRequestPlanner, w gridWindow,
	step gridSeqStep, prefix string) (*sql.With, error) {
	vals, err := w.filled(ctx, &gridSequence{Fp: fp, Window: w, Step: step},
		[]string{"first_v", "first_ts", "last_v", "last_ts", "cnt", "first_d", "d"})
	if err != nil {
		return nil, err
	}
	withVals := sql.NewWith(vals, prefix+"_vals")
	wnd, err := rangeFrame(prefix+"_wnd", w.Range)
	if err != nil {
		return nil, err
	}
	cols := [][2]string{
		{"argMinIf(first_v, timestamp_ms, source = 1)", "f_first_v"},
		{"minIf(first_ts, source = 1)", "f_first_ts"},
		{"argMaxIf(last_v, timestamp_ms, source = 1)", "f_last_v"},
		{"maxIf(last_ts, source = 1)", "f_last_ts"},
		{"sumIf(cnt, source = 1)", "f_cnt"},
		{"sumIf(d, source = 1)", "f_d_all"},
		// The first pair ends in the window but starts before it.
		{"argMinIf(first_d, timestamp_ms, source = 1)", "f_d_first"},
	}
	sel := []sql.SQLObject{
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
	}
	for _, col := range cols {
		sel = append(sel, sql.NewCol(overWnd(sql.NewRawObject(col[0]), wnd), col[1]))
	}
	sel = append(sel, sql.NewSimpleCol("f_d_all - f_d_first", "f_d"))
	return sql.NewWith(
		sql.NewSelect().With(withVals).Select(sel...).From(sql.NewWithRef(withVals)).AddWindows(wnd),
		prefix+"_ranges"), nil
}

// onGrid keeps the rows keyed on an evaluation point of g.
func onGrid(g Grid) sql.SQLCondition {
	return sql.Eq(gridMod("timestamp_ms", g.PhaseMs, g.StepMs), sql.NewIntVal(0))
}

// gridResult selects (fingerprint, timestamp_ms, val) from with, keeping the
// rows on g's evaluation points that satisfy where.
func gridResult(with *sql.With, g Grid, val string, where ...sql.SQLCondition) sql.ISelect {
	return sql.NewSelect().With(with).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol(val, "val")).
		From(sql.NewWithRef(with)).
		AndWhere(append(where, onGrid(g))...)
}
