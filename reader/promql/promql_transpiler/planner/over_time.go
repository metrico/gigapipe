package planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type overTimeCol struct {
	alias string
	expr  string
}

// overTimeDef describes a range function that is a plain aggregate over the
// samples of (t-range, t]: no lookback outside the frame, no dependency on the
// order of the samples other than through an aggregate.
type overTimeDef struct {
	// bucket holds the partial aggregates read per step from metrics_15s.
	bucket []overTimeCol
	// window holds the aggregates evaluated over the range frame. They must all
	// be -If(..., source = 1) forms to skip the rows added by WITH FILL.
	window []overTimeCol
	// val is the final value, built from the window aliases.
	val string
	// grid holds the bucket aggregates of a grid-keyed read, under the
	// aliases of bucket.
	grid []gridCol
}

var overTimeDefs = map[string]overTimeDef{
	"sum_over_time": {
		bucket: []overTimeCol{{"b_sum", "sum(sum)"}},
		window: []overTimeCol{{"w_sum", "sumIf(b_sum, source = 1)"}},
		val:    "w_sum",
		grid:   []gridCol{gridSum},
	},
	"count_over_time": {
		// counts samples, not buckets, so the bucket counts are summed rather
		// than counted over the frame.
		bucket: []overTimeCol{{"b_cnt", "toFloat64(countMerge(count))"}},
		window: []overTimeCol{{"w_cnt", "sumIf(b_cnt, source = 1)"}},
		val:    "w_cnt",
		grid:   []gridCol{gridCount},
	},
	"min_over_time": {
		bucket: []overTimeCol{{"b_min", "min(min)"}},
		window: []overTimeCol{{"w_min", "minIf(b_min, source = 1)"}},
		val:    "w_min",
		grid:   []gridCol{gridPartial("b_min", gridMerges["min_over_time"], "min(p_b_min)")},
	},
	"max_over_time": {
		bucket: []overTimeCol{{"b_max", "max(max)"}},
		window: []overTimeCol{{"w_max", "maxIf(b_max, source = 1)"}},
		val:    "w_max",
		grid:   []gridCol{gridPartial("b_max", gridMerges["max_over_time"], "max(p_b_max)")},
	},
	"avg_over_time": {
		// sum/count rather than an average of the bucket averages, which would
		// weight every bucket equally regardless of how many samples it holds.
		bucket: []overTimeCol{{"b_sum", "sum(sum)"}, {"b_cnt", "toFloat64(countMerge(count))"}},
		window: []overTimeCol{{"w_sum", "sumIf(b_sum, source = 1)"}, {"w_cnt", "sumIf(b_cnt, source = 1)"}},
		val:    "w_sum / w_cnt",
		grid:   []gridCol{gridSum, gridCount},
	},
	"last_over_time": {
		bucket: []overTimeCol{{"b_last", "argMaxMerge(last)"}, {"b_ts", "max(timestamp_ns)"}},
		window: []overTimeCol{{"w_last", "argMaxIf(b_last, b_ts, source = 1)"}},
		val:    "w_last",
		grid: []gridCol{
			gridPartial("b_last", gridLast, "argMax(p_b_last, p_b_ts)"),
			{alias: "b_ts", cell: gridLast.cellAux, raw: gridLast.rawAux, merge: "max(p_b_ts)"},
		},
	},
	"present_over_time": {
		// carried entirely by the emptiness check below.
		val: "1",
	},
}

var (
	gridSum   = gridPartial("b_sum", gridMerges["sum_over_time"], "sum(p_b_sum)")
	gridCount = gridPartial("b_cnt", gridMerges["count_over_time"], "sum(p_b_cnt)")
)

// gridPartial is the bucket aggregate alias: the value parts of m, combined by
// merge.
func gridPartial(alias string, m gridMerge, merge string) gridCol {
	return gridCol{alias: alias, cell: m.cellVal, raw: m.rawVal, merge: merge}
}

// OverTimePlanner accelerates the range functions that reduce to one aggregate
// over one frame. See CounterPlanner for the ones that measure across the frame
// boundary instead.
//
// With a Grid it reads the grid's buckets and returns its evaluation points.
type OverTimePlanner struct {
	FpPlanner shared.SQLRequestPlanner
	Duration  time.Duration
	Fn        string
	Grid      *Grid
}

func (o *OverTimePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	def, ok := overTimeDefs[o.Fn]
	if !ok {
		return nil, fmt.Errorf("unsupported range function: %s", o.Fn)
	}

	frame, err := rangeFrame("ot_frame", o.Duration)
	if err != nil {
		return nil, err
	}

	bucketCols := make([]sql.SQLObject, len(def.bucket))
	for i, c := range def.bucket {
		bucketCols[i] = sql.NewSimpleCol(c.expr, c.alias)
	}
	// Buckets of whole fractions of the range, so the frame tiles (t-range, t].
	var vals sql.ISelect
	if o.Grid != nil {
		vals, err = gridBucketedValues(ctx, o.FpPlanner, gridWindow{Grid: *o.Grid, Range: o.Duration}, def.grid...)
	} else {
		vals, err = bucketedValues(ctx, o.FpPlanner, o.Duration, BucketResolution(ctx.Step, o.Duration), bucketCols...)
	}
	if err != nil {
		return nil, err
	}
	withVals := sql.NewWith(vals, "ot_vals")

	wndCols := []sql.SQLObject{
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
	}
	for _, c := range def.window {
		wndCols = append(wndCols, sql.NewCol(overWnd(sql.NewRawObject(c.expr), frame), c.alias))
	}
	// Number of real samples anywhere in the frame. A range with no samples at
	// all yields no point rather than a zero, which is what prometheus does.
	wndCols = append(wndCols, sql.NewCol(overWnd(sql.NewRawObject("sum(source)"), frame), "w_src"))

	withWnd := sql.NewWith(
		sql.NewSelect().With(withVals).Select(wndCols...).
			From(sql.NewWithRef(withVals)).
			AddWindows(frame),
		"ot_wnd")

	nonEmpty := sql.Gt(sql.NewRawObject("w_src"), sql.NewIntVal(0))
	if o.Grid != nil {
		return gridResult(withWnd, *o.Grid, def.val, nonEmpty), nil
	}
	return sql.NewSelect().With(withWnd).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol(def.val, "val")).
		From(sql.NewWithRef(withWnd)).
		AndWhere(nonEmpty), nil
}
