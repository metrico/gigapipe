package planner

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/storage"
)

// DownsampleGridPlanner reads a tagged selector from metrics_15s, keyed on its
// grid. A bucket keyed K of width w holds (K-w, K], with K = phase + k*w.
//
// A metrics_15s cell holds [c, c+15s). Cells whose start is congruent to the
// phase modulo the edge width straddle a bucket edge, so their samples are read
// raw and keyed one by one; every other cell lies inside one bucket.
type DownsampleGridPlanner struct {
	Fp    shared.SQLRequestPlanner
	Hints *storage.SelectHints
	Grid  Grid
}

// gridShape is how a selector's read is bucketed: keys at Phase + k*Width,
// edge cells at Phase + k*Edge, and, when FilterStep is set, only the samples
// in (T-FilterWindow, T] of every T = Phase + k*FilterStep.
type gridShape struct {
	Phase, Width, Edge       int64
	FilterStep, FilterWindow int64
}

// shape picks the bucketing for the selector's function and window. A selector
// without a range reads the newest sample of (T-LD, T].
func (d *DownsampleGridPlanner) shape() gridShape {
	step, r := d.Grid.StepMs, d.Hints.Range
	window := SelectorWindowMs(r)
	s := gridShape{Phase: d.Grid.PhaseMs, Width: GridEdgeMs(d.Grid, r)}
	s.Edge = s.Width
	switch {
	case r > 0 && hintsRangeVectors[d.Hints.Func] && step > r:
		s.Width = step
		s.FilterStep, s.FilterWindow = step, r
	case r == 0 && step > window:
		s.FilterStep, s.FilterWindow = step, window
	}
	return s
}

// gridMerge combines a bucket's interior cells and its raw edge samples: val
// and aux per part, then the bucket's value from both. aux is the newest
// timestamp for last, the sample count for avg, and unused otherwise.
type gridMerge struct {
	cellVal, cellAux string
	rawVal, rawAux   string
	val              string
}

// gridLast keeps the newest sample; aux orders a part by its newest timestamp.
var gridLast = gridMerge{
	cellVal: "argMaxMerge(samples.last)", cellAux: "max(samples.timestamp_ns)",
	rawVal: "argMax(samples.value, samples.timestamp_ns)", rawAux: "max(samples.timestamp_ns)",
	val: "argMax(val, aux)",
}

var gridMerges = map[string]gridMerge{
	"min_over_time": {cellVal: "min(samples.min)", cellAux: "0", rawVal: "min(samples.value)", rawAux: "0",
		val: "min(val)"},
	"max_over_time": {cellVal: "max(samples.max)", cellAux: "0", rawVal: "max(samples.value)", rawAux: "0",
		val: "max(val)"},
	"sum_over_time": {cellVal: "sum(samples.sum)", cellAux: "0", rawVal: "sum(samples.value)", rawAux: "0",
		val: "sum(val)"},
	"count_over_time": {cellVal: "toFloat64(countMerge(samples.count))", cellAux: "0",
		rawVal: "toFloat64(count())", rawAux: "0", val: "sum(val)"},
	"avg_over_time": {cellVal: "sum(samples.sum)", cellAux: "countMerge(samples.count)",
		rawVal: "sum(samples.value)", rawAux: "count()", val: "sum(val) / sum(aux)"},
	"absent_over_time": {cellVal: "toFloat64(1)", cellAux: "0", rawVal: "toFloat64(1)", rawAux: "0",
		val: "max(val)"},
	"present_over_time": {cellVal: "toFloat64(1)", cellAux: "0", rawVal: "toFloat64(1)", rawAux: "0",
		val: "max(val)"},
	"last_over_time": gridLast,
}

// merge picks the bucket merge for the selector's function.
func (d *DownsampleGridPlanner) merge() gridMerge {
	if m, ok := gridMerges[d.Hints.Func]; ok && d.Hints.Range > 0 {
		return m
	}
	return gridLast
}

const gridMsCol = "intDiv(samples.timestamp_ns, 1000000)"

// GridEdgeMs is the width of the buckets a selector with window rangeMs reads
// on g; cells starting on a multiple of it are read raw.
func GridEdgeMs(g Grid, rangeMs int64) int64 {
	return gcd(g.StepMs, SelectorWindowMs(rangeMs))
}

func (d *DownsampleGridPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	shape := d.shape()
	merge := d.merge()
	cells, edges, err := gridParts(ctx, d.Fp, shape.Phase, shape.Edge)
	if err != nil {
		return nil, err
	}
	edges.Select(
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		sql.NewSimpleCol(gridKey(gridMsCol, shape.Phase, shape.Width), "key_ms"),
		sql.NewSimpleCol(merge.rawVal, "val"),
		sql.NewSimpleCol(merge.rawAux, "aux"),
	)
	cells.Select(
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		sql.NewSimpleCol(gridKey(gridMsCol, shape.Phase, shape.Width), "key_ms"),
		sql.NewSimpleCol(merge.cellVal, "val"),
		sql.NewSimpleCol(merge.cellAux, "aux"),
	)
	for _, p := range []sql.ISelect{cells, edges} {
		if shape.FilterStep != 0 {
			p.AndWhere(trailingWindowOn(gridMsCol, shape.Phase, shape.FilterStep, shape.FilterWindow, false))
		}
		// The outer select groups and orders the union.
		p.GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("key_ms")).OrderBy()
	}

	key := "key_ms"
	if config.Cloki.Setting.ClokiReader.Compat_4_0_19 {
		key = "key_ms - 1"
	}
	res := sql.NewSelect().Select(
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol(merge.val, "val"),
		sql.NewSimpleCol(key, "timestamp_ms"),
	).From(&unionAll{cells, []sql.ISelect{edges}}).GroupBy(
		sql.NewRawObject("fingerprint"),
		sql.NewRawObject("key_ms"),
	).OrderBy(
		sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
		sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC),
	)
	return res.With(fpWith(edges)...), nil
}

// gridParts returns the two reads of a grid-keyed read over (ctx.From,
// ctx.To]: the metrics_15s cells that do not start on an edge, phase + k*edge,
// and the raw samples of the cells that do. Both read from the alias samples.
func gridParts(ctx *shared.PlannerContext, fp shared.SQLRequestPlanner, phase, edge int64) (cells, edges sql.ISelect,
	err error) {
	edges, err = (&ValuesPlanner{Fp: fp}).Process(ctx)
	if err != nil {
		return nil, nil, err
	}
	edges.AndWhere(sql.Eq(gridMod(fmt.Sprintf("intDiv(samples.timestamp_ns, %d) * %d", LatticeMs*1000000, LatticeMs),
		phase, edge), sql.NewIntVal(0)))
	cells, err = (&DownsampleValuesPlanner{ValuesPlanner{Fp: fp}}).Process(ctx)
	if err != nil {
		return nil, nil, err
	}
	cells.AndWhere(sql.Neq(gridMod(gridMsCol, phase, edge), sql.NewIntVal(0)))
	return cells, edges, nil
}

// fpWith returns the fingerprint CTE of sel, for hoisting onto a select built
// over it.
func fpWith(sel sql.ISelect) []*sql.With {
	for _, w := range sel.GetWith() {
		if w.GetAlias() == "fp" {
			return []*sql.With{w}
		}
	}
	return nil
}

// gridKey keys the millisecond column col to the end of its (K-width, K]
// bucket, K = phase + k*width.
func gridKey(col string, phase, width int64) string {
	phase %= width
	if phase == 0 {
		return fmt.Sprintf("intDiv(%s + %d, %d) * %d", col, width-1, width, width)
	}
	return fmt.Sprintf("intDiv(%s - %d + %d, %d) * %d + %d", col, phase, width-1, width, width, phase)
}

// gridMod renders (col - phase) % mod.
func gridMod(col string, phase, mod int64) sql.SQLObject {
	phase %= mod
	if phase == 0 {
		return sql.NewRawObject(fmt.Sprintf("%s %% %d", col, mod))
	}
	return sql.NewRawObject(fmt.Sprintf("(%s - %d) %% %d", col, phase, mod))
}
