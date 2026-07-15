package planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// prevValues exposes, for every step, the value of the preceding real sample
// (prev) and how many preceding real samples were in reach (prev_cnt). Both the
// counter functions and the transition counting functions are built on it.
//
// prev_cnt = 0 means there is no preceding sample within staleness, in which
// case prev is a zero default and must not be compared against val.
func prevValues(ctx *shared.PlannerContext, fpPlanner shared.SQLRequestPlanner,
	duration time.Duration) (*sql.With, error) {
	vals, err := bucketedValues(ctx, fpPlanner, duration+staleness,
		sql.NewSimpleCol("argMaxMerge(last)", "val"))
	if err != nil {
		return nil, err
	}
	withVals := sql.NewWith(vals, "cnt_vals")

	lookback := ctx.Step
	if lookback < staleness {
		lookback = staleness
	}
	start, err := windowOffset(lookback)
	if err != nil {
		return nil, err
	}
	prevWnd := &sql.WindowFunction{
		Alias:       "cnt_prev_wnd",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Start:       sql.WindowPoint{Offset: start},
		End:         sql.WindowPoint{Offset: 1},
	}

	return sql.NewWith(
		sql.NewSelect().With(withVals).Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("val", "val"),
			sql.NewSimpleCol("source", "source"),
			sql.NewCol(overWnd(sql.NewRawObject("argMaxIf(val, timestamp_ms, source = 1)"), prevWnd), "prev"),
			sql.NewCol(overWnd(sql.NewRawObject("countIf(source = 1)"), prevWnd), "prev_cnt")).
			From(sql.NewWithRef(withVals)).
			AddWindows(prevWnd),
		"cnt_prev"), nil
}

// CounterPlanner accelerates the range functions that measure the change of a
// series across the frame: rate, increase and delta.
//
// They cannot be expressed as one aggregate over (t-range, t], because the
// value at the start of the range is an actual sample whose position varies
// with the data. The frame therefore has to be probed on both sides: openWnd
// looks for the last sample before the range, closeWnd covers the range itself.
type CounterPlanner struct {
	FpPlanner shared.SQLRequestPlanner
	Duration  time.Duration
	Fn        string
}

func (c *CounterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	// isCounter: add back the value lost at every counter reset. delta operates
	// on gauges, where a decrease is a real decrease.
	var isCounter bool
	var val string
	switch c.Fn {
	case "rate":
		isCounter = true
		val = fmt.Sprintf("(end - start + resets) / %f", c.Duration.Seconds())
	case "increase":
		isCounter = true
		val = "end - start + resets"
	case "delta":
		val = "end - start"
	default:
		return nil, fmt.Errorf("unsupported counter function: %s", c.Fn)
	}

	withPrev, err := prevValues(ctx, c.FpPlanner, c.Duration)
	if err != nil {
		return nil, err
	}

	resetCol := "0"
	if isCounter {
		// The value lost at a reset is the whole pre-reset counter value.
		resetCol = "prev * (prev_cnt > 0) * (prev > val) * (source = 1)"
	}
	withResets := sql.NewWith(
		sql.NewSelect().With(withPrev).Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("val", "val"),
			sql.NewSimpleCol("source", "source"),
			sql.NewSimpleCol(resetCol, "reset")).
			From(sql.NewWithRef(withPrev)),
		"cnt_resets")

	closeWnd, err := rangeFrame("cnt_close_wnd", c.Duration)
	if err != nil {
		return nil, err
	}
	openStart, err := windowOffset(c.Duration + staleness)
	if err != nil {
		return nil, err
	}
	openEnd, err := windowOffset(c.Duration - time.Millisecond)
	if err != nil {
		return nil, err
	}
	// The staleness wide strip immediately before the range.
	openWnd := &sql.WindowFunction{
		Alias:       "cnt_open_wnd",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Start:       sql.WindowPoint{Offset: openStart},
		End:         sql.WindowPoint{Offset: openEnd},
	}

	withRanges := sql.NewWith(
		sql.NewSelect().With(withResets).Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(overWnd(sql.NewRawObject("argMaxIf(val, timestamp_ms, source = 1)"), openWnd), "start_open"),
			sql.NewCol(overWnd(sql.NewRawObject("countIf(source = 1)"), openWnd), "open_cnt"),
			sql.NewCol(overWnd(sql.NewRawObject("argMinIf(val, timestamp_ms, source = 1)"), closeWnd), "start_close"),
			sql.NewCol(overWnd(sql.NewRawObject("argMaxIf(val, timestamp_ms, source = 1)"), closeWnd), "end"),
			sql.NewCol(overWnd(sql.NewRawObject("sum(source)"), closeWnd), "close_cnt"),
			sql.NewCol(overWnd(sql.NewRawObject("sum(reset)"), closeWnd), "resets")).
			From(sql.NewWithRef(withResets)).
			AddWindows(openWnd, closeWnd),
		"cnt_ranges")

	// Measure from the last sample before the range when there is one, so that
	// the growth that happened between it and the first in-range sample is not
	// dropped. open_cnt, not start_open > 0: a counter legitimately sitting at
	// zero, or any gauge, would defeat a value based existence test.
	withStart := sql.NewWith(
		sql.NewSelect().With(withRanges).Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("end", "end"),
			sql.NewSimpleCol("resets", "resets"),
			sql.NewSimpleCol("close_cnt", "close_cnt"),
			sql.NewSimpleCol("if(open_cnt > 0, start_open, start_close)", "start")).
			From(sql.NewWithRef(withRanges)),
		"cnt_start")

	return sql.NewSelect().With(withStart).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol(val, "val")).
		From(sql.NewWithRef(withStart)).
		AndWhere(sql.Gt(sql.NewRawObject("close_cnt"), sql.NewIntVal(0))), nil
}

// CounterFlagsPlanner accelerates the range functions that count transitions
// between consecutive samples: resets and changes.
type CounterFlagsPlanner struct {
	FpPlanner shared.SQLRequestPlanner
	Duration  time.Duration
	Fn        string
}

func (c *CounterFlagsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var flag string
	switch c.Fn {
	case "resets":
		flag = "(prev_cnt > 0) * (prev > val) * (source = 1)"
	case "changes":
		// prev_cnt guards the first sample of the series: prev defaults to zero
		// there, which would read as a change for any non zero value.
		flag = "(prev_cnt > 0) * (prev != val) * (source = 1)"
	default:
		return nil, fmt.Errorf("unsupported transition function: %s", c.Fn)
	}

	withPrev, err := prevValues(ctx, c.FpPlanner, c.Duration)
	if err != nil {
		return nil, err
	}
	withFlags := sql.NewWith(
		sql.NewSelect().With(withPrev).Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol("source", "source"),
			sql.NewSimpleCol(flag, "flag")).
			From(sql.NewWithRef(withPrev)),
		"cnt_flags")

	closeWnd, err := rangeFrame("cnt_close_wnd", c.Duration)
	if err != nil {
		return nil, err
	}
	withWnd := sql.NewWith(
		sql.NewSelect().With(withFlags).Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(overWnd(sql.NewRawObject("sumIf(flag, source = 1)"), closeWnd), "flags"),
			// The earliest in range sample compares against a sample outside the
			// range, a transition prometheus does not count. Every later sample
			// compares against one inside it, so subtracting this one flag is
			// the whole correction.
			sql.NewCol(overWnd(sql.NewRawObject("argMinIf(flag, timestamp_ms, source = 1)"), closeWnd), "first_flag"),
			sql.NewCol(overWnd(sql.NewRawObject("sum(source)"), closeWnd), "close_cnt")).
			From(sql.NewWithRef(withFlags)).
			AddWindows(closeWnd),
		"cnt_flags_wnd")

	return sql.NewSelect().With(withWnd).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol("toFloat64(flags - first_flag)", "val")).
		From(sql.NewWithRef(withWnd)).
		AndWhere(sql.Gt(sql.NewRawObject("close_cnt"), sql.NewIntVal(0))), nil
}
