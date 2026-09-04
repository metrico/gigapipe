package planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// prevValues exposes, for every step, the value of the preceding real sample
// (prev) and how many preceding real samples were in reach (prev_cnt). Both the
// counter functions and the transition counting functions are built on it.
//
// prev_cnt = 0 means there is no preceding sample within staleness, in which
// case prev is a zero default and must not be compared against val.
func prevValues(ctx *shared.PlannerContext, fpPlanner shared.SQLRequestPlanner,
	duration time.Duration) (*sql.With, error) {
	// val is the last sample of the bucket, so val_ts -- that sample's own
	// timestamp -- is where it actually sits, which is not the bucket key. The
	// two differ by however far into the bucket the last sample fell, and the
	// final bucket of a query is truncated by the read bound, so its last sample
	// can be anywhere in it. Anything measuring an interval between values has to
	// use val_ts; measuring between bucket keys would overstate it.
	vals, err := bucketedValues(ctx, fpPlanner, duration+staleness,
		sql.NewSimpleCol("argMaxMerge(last)", "val"),
		sql.NewSimpleCol("intDiv(max(timestamp_ns), 1000000)", "val_ts"))
	if err != nil {
		return nil, err
	}
	withVals := sql.NewWith(vals, "cnt_vals")

	lookback := max(ctx.Step, staleness)
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
			sql.NewSimpleCol("val_ts", "val_ts"),
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
// They cannot be expressed as one aggregate over (t-range, t], because what is
// measured is the difference between two particular samples, not a reduction
// over all of them. The frame is probed for its first and last real sample; the
// change between them, divided by the time between them, is the slope reported.
type CounterPlanner struct {
	FpPlanner shared.SQLRequestPlanner
	Duration  time.Duration
	Fn        string
}

func (c *CounterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	// isCounter: add back the value lost at every counter reset. delta operates
	// on gauges, where a decrease is a real decrease.
	//
	// isRate: report the change per second. increase is the same measurement
	// over the whole range instead, which is why it is a counter function but
	// not a rate one.
	var isCounter, isRate bool
	switch c.Fn {
	case "rate":
		isCounter, isRate = true, true
	case "increase":
		isCounter, isRate = true, false
	case "delta":
		isCounter, isRate = false, false
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
			sql.NewSimpleCol("val_ts", "val_ts"),
			sql.NewSimpleCol("source", "source"),
			sql.NewSimpleCol(resetCol, "reset")).
			From(sql.NewWithRef(withPrev)),
		"cnt_resets")

	closeWnd, err := rangeFrame("cnt_close_wnd", c.Duration)
	if err != nil {
		return nil, err
	}

	// The two samples the change is measured between, and where they sit. Both
	// _ts columns are picked by bucket order but carry val_ts, so they are the
	// positions of those same two samples rather than of their buckets -- the
	// change and the interval it happened over are then on the same clock.
	//
	// first_reset is the reset flagged on the earliest in-range sample. That one
	// compares against a sample outside the range, so the drop it records
	// happened before the range began and is not part of the change measured
	// inside it; c_change subtracts it back out. Every later sample compares
	// against one inside the range. CounterFlagsPlanner applies the same
	// correction to its transition counts via first_flag.
	withRanges := sql.NewWith(
		sql.NewSelect().With(withResets).Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(overWnd(sql.NewRawObject("argMinIf(val, timestamp_ms, source = 1)"), closeWnd), "first_v"),
			sql.NewCol(overWnd(sql.NewRawObject("argMinIf(val_ts, timestamp_ms, source = 1)"), closeWnd), "first_ts"),
			sql.NewCol(overWnd(sql.NewRawObject("argMaxIf(val, timestamp_ms, source = 1)"), closeWnd), "last_v"),
			sql.NewCol(overWnd(sql.NewRawObject("argMaxIf(val_ts, timestamp_ms, source = 1)"), closeWnd), "last_ts"),
			sql.NewCol(overWnd(sql.NewRawObject("sum(reset)"), closeWnd), "resets"),
			sql.NewCol(overWnd(sql.NewRawObject("argMinIf(reset, timestamp_ms, source = 1)"), closeWnd), "first_reset")).
			From(sql.NewWithRef(withResets)).
			AddWindows(closeWnd),
		"cnt_ranges")

	withSlope := sql.NewWith(
		sql.NewSelect().With(withRanges).Select(
			append([]sql.SQLObject{
				sql.NewSimpleCol("fingerprint", "fingerprint"),
				sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
				sql.NewSimpleCol("first_ts", "first_ts"),
				sql.NewSimpleCol("last_ts", "last_ts"),
				sql.NewSimpleCol("first_v", "first_v"),
				sql.NewSimpleCol(c.change(isCounter), "c_change"),
				// Milliseconds to seconds: rate is per second, so the span it is
				// divided by has to be too. Doing it once here keeps the unit in
				// one place rather than at each of the call sites.
				//
				// The subtraction is positive by construction -- both timestamps
				// are those of real samples inside (t-range, t] -- and clickhouse
				// division yields Float64 whatever the operands are.
				sql.NewSimpleCol("(last_ts - first_ts) / 1000", "c_span_sec"),
			}, c.reach(isCounter)...)...).
			From(sql.NewWithRef(withRanges)),
		"cnt_slope")

	// The change carried out to the edges of the range, then reported per second
	// or over the range. c_reach is 1 whenever the samples already span the whole
	// range, so the common case is unaffected by any of it.
	val := "c_change * c_reach"
	if isRate {
		val = fmt.Sprintf("c_change * c_reach / %f", c.Duration.Seconds())
	}

	return sql.NewSelect().With(withSlope).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol(val, "val")).
		From(sql.NewWithRef(withSlope)).
		// Two distinct samples inside the range, or there is no interval to
		// measure a change over. A lone sample yields no point, as in
		// prometheus, and this is also what keeps c_span_sec off zero.
		AndWhere(sql.Gt(sql.NewRawObject("last_ts"), sql.NewRawObject("first_ts"))), nil
}

// change is the value change actually observed between the first and last real
// sample of the range, before it is scaled. For counters the value lost at every
// reset inside the range is added back, so a wrap does not read as a decrease.
func (c *CounterPlanner) change(isCounter bool) string {
	if isCounter {
		return "last_v - first_v + (resets - first_reset)"
	}
	return "last_v - first_v"
}

// reach builds c_reach, the multiplier that carries the observed change out to
// the edges of the range.
//
// The samples only cover c_span_sec of the range. Reporting the change as if it
// were the whole range's understates it; assuming the observed slope held across
// every second of the range overstates it whenever the series was not reporting
// for part of that time. c_reach is how much of the uncovered time the change is
// carried across, as a multiple of the span the samples do cover, so it is 1
// exactly when the samples already reach both edges.
//
// Forward, there is nothing to decide: the series is still live at t, so the
// slope carries to the edge. Backward is bounded twice over -- by the edge
// itself, and for counters by the counter's own first value, since a counter
// cannot have been negative. At the observed slope, growing from zero up to
// first_v takes c_span_sec * first_v / c_change seconds, and no more growth than
// that can be attributed to the time before the first sample. A counter that
// started at zero inside the range therefore gets no backward reach at all,
// which is what stops a series from appearing to have been running before it
// existed.
func (c *CounterPlanner) reach(isCounter bool) []sql.SQLObject {
	back := "c_back_edge"
	if isCounter {
		back = "least(c_back_edge, if(c_change > 0 AND first_v >= 0, " +
			"c_span_sec * first_v / c_change, c_back_edge))"
	}
	cols := [][2]string{
		{fmt.Sprintf("(first_ts - (timestamp_ms - %d)) / 1000", c.Duration.Milliseconds()), "c_back_edge"},
		{"(timestamp_ms - last_ts) / 1000", "c_fwd_edge"},
		{back, "c_back"},
		{"(c_span_sec + c_back + c_fwd_edge) / c_span_sec", "c_reach"},
	}
	res := make([]sql.SQLObject, len(cols))
	for i, col := range cols {
		res[i] = sql.NewSimpleCol(col[0], col[1])
	}
	return res
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
