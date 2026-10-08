package planner

import (
	"fmt"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/storage"
)

type HintsPlanner struct {
	Main  shared.SQLRequestPlanner
	Hints *storage.SelectHints
	// Grid, when set, is the selector's evaluation grid; bucket keys and the
	// trailing filter are anchored on it.
	Grid *Grid
}

// hintsInstantVectors are the instant functions a raw read buckets for.
var hintsInstantVectors = map[string]bool{
	"abs": true, "absent": true, "ceil": true, "exp": true, "floor": true,
	"ln": true, "log2": true, "log10": true, "round": true, "scalar": true, "sgn": true, "sort": true, "sqrt": true,
	"timestamp": true, "atan": true, "cos": true, "cosh": true, "sin": true, "sinh": true, "tan": true, "tanh": true,
	"deg": true, "rad": true,
}

// hintsRangeVectors are the range functions the Step > Range filter applies to.
var hintsRangeVectors = map[string]bool{
	"absent_over_time": true /*"changes": true,*/, "deriv": true, "idelta": true, "irate": true,
	"rate": true, "resets": true, "min_over_time": true, "max_over_time": true, "sum_over_time": true,
	"count_over_time": true, "stddev_over_time": true, "stdvar_over_time": true, "last_over_time": true,
	"present_over_time": true, "delta": true, "increase": true, "avg_over_time": true,
}

func (h *HintsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	query, err := h.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if h.Grid != nil {
		return h.processOnGrid(query, *h.Grid), nil
	}
	hints := h.Hints
	if keepsLastPerBucket(hints.Func) {
		query = lastPerBucket(query, hints.Start, hints.Step)
	}
	if hintsRangeVectors[hints.Func] && hints.Step > hints.Range {
		query.AndWhere(trailingWindow(0, hints.Step, hints.Range, true))
	}
	return query, nil
}

// processOnGrid reads, for a selector without a range (bare or inside a
// subquery), the newest sample of (T-LD, T] at every T on g; a range function
// whose step outruns its range reads only the trailing range of each step.
func (h *HintsPlanner) processOnGrid(query sql.ISelect, g Grid) sql.ISelect {
	hints := h.Hints
	if hints.Range == 0 {
		if !keepsLastPerBucket(hints.Func) && !hintsRangeVectors[hints.Func] {
			return query
		}
		lookback := SelectorWindowMs(0)
		width := gcd(gcd(hints.Step, g.StepMs), lookback)
		if g.StepMs > lookback {
			query.AndWhere(trailingWindow(g.PhaseMs, g.StepMs, lookback, false))
			width = g.StepMs
		}
		return lastPerBucket(query, gridAnchor(hints.Start, g.PhaseMs, width), width)
	}
	if hintsRangeVectors[hints.Func] && hints.Step > hints.Range && g.StepMs > hints.Range {
		query.AndWhere(trailingWindow(g.PhaseMs, g.StepMs, hints.Range, true))
	}
	return query
}

// keepsLastPerBucket reports whether a selector read for fn needs only the
// newest sample of each bucket.
func keepsLastPerBucket(fn string) bool {
	return fn == "" || hintsInstantVectors[fn]
}

// gridAnchor is the latest time at or before notAfter that is congruent to
// phase modulo width.
func gridAnchor(notAfter, phase, width int64) int64 {
	return notAfter - ((notAfter-phase)%width+width)%width
}

// lastPerBucket keeps the newest sample of every (key-width, key] bucket,
// with keys at anchor + k*width.
func lastPerBucket(query sql.ISelect, anchor, width int64) sql.ISelect {
	withQuery := sql.NewWith(query, "spls")
	return sql.NewSelect().With(withQuery).Select(
		sql.NewRawObject("fingerprint"),
		//sql.NewSimpleCol("spls.labels", "labels"),
		sql.NewSimpleCol("argMax(spls.val, spls.timestamp_ms)", "val"),
		sql.NewSimpleCol(fmt.Sprintf("intDiv(spls.timestamp_ms - %d + %d - 1, %d) * %d + %d",
			anchor, width, width, width, anchor), "timestamp_ms"),
	).From(
		sql.NewWithRef(withQuery),
	).GroupBy(
		sql.NewRawObject("timestamp_ms"),
		sql.NewRawObject("fingerprint"),
	).OrderBy(
		sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
		sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC),
	)
}

// trailingWindow keeps the samples within window before each evaluation point
// phase + k*step: [T-window, T] when closed, (T-window, T] otherwise.
func trailingWindow(phase, step, window int64, closed bool) sql.SQLCondition {
	ts := "timestamp_ms"
	if phase != 0 {
		ts = fmt.Sprintf("(timestamp_ms - %d)", phase)
	}
	msInStep := sql.NewRawObject(fmt.Sprintf("%s %% %d", ts, step))
	edge := sql.Gt(msInStep, sql.NewIntVal(step-window))
	if closed {
		edge = sql.Ge(msInStep, sql.NewIntVal(step-window))
	}
	return sql.Or(sql.Eq(msInStep, sql.NewIntVal(0)), edge)
}
