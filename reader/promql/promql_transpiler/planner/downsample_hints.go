package planner

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/storage"
)

type DownsampleHintsPlanner struct {
	Main    shared.SQLRequestPlanner
	Partial bool
	Hints   *storage.SelectHints
}

func (d *DownsampleHintsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	query, err := d.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if d.Hints.Step == 0 {
		return query, nil
	}
	hints := d.Hints
	rangeVectors := map[string]bool{
		"absent_over_time": true /*"changes": true,*/, "deriv": true, "idelta": true, "irate": true,
		"rate": true, "resets": true, "min_over_time": true, "max_over_time": true, "sum_over_time": true,
		"count_over_time": true, "stddev_over_time": true, "stdvar_over_time": true, "last_over_time": true,
		"present_over_time": true, "delta": true, "increase": true, "avg_over_time": true,
	}

	patchField(query, "val",
		sql.NewSimpleCol(d.getValueMerge(hints.Func), "val").(sql.Aliased))

	step := hints.Step
	if changeFunctions[hints.Func] {
		// This bucket is handed to the engine's own implementation of the
		// function as if it were the raw series, one point per bucket picked by
		// argMax(last, ts) -- so a change-across-samples function only ever sees
		// as many samples as there are buckets inside the (t-range, t] window it
		// evaluates. Buckets are aligned to epoch, not to the query's own
		// evaluation timestamps, so how many of them a given window happens to
		// catch depends on an alignment the caller never controls: at step close
		// to (or above) range this could come back empty, come back with one
		// point, or work, essentially at random, and irate/idelta -- which read
		// only the last two samples of whatever the window catches -- are
		// exactly as exposed to it as rate/deriv/delta, which need the window's
		// full span. Capping the bucket to at most range/2 guarantees at least
		// two land inside any (t-range, t] window regardless of that alignment.
		if half := hints.Range / 2; half > 0 && half < step {
			step = half
		}
	}

	if rangeVectors[hints.Func] && step > hints.Range {
		timeField := fmt.Sprintf("intDiv(samples.timestamp_ns + %d * 1000000, %d * 1000000) * %d",
			hints.Range, step, step)
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))
		msInStep := sql.NewRawObject(fmt.Sprintf("timestamp_ns %% %d000000", step))
		query.AndWhere(sql.Or(
			sql.Eq(msInStep, sql.NewIntVal(0)),
			sql.Gt(msInStep, sql.NewIntVal(step*1000000-hints.Range*1000000)),
		))
	} else {
		compat4019 := ""
		if config.Cloki.Setting.ClokiReader.Compat_4_0_19 {
			compat4019 = " - 1 "
		}
		timeField := fmt.Sprintf("intDiv(samples.timestamp_ns, %d * 1000000) * %d%s",
			step, step, compat4019)
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))
	}

	return query, nil
}

// changeFunctions are the rangeVectors entries that measure a change across
// samples rather than reducing over every sample in the window: rate, irate
// and deriv compute a slope, delta/idelta a difference, resets a count of
// decreases. increase and changes are included defensively even though the
// ClickHouse pushdown (CounterPlanner/CounterFlagsPlanner) accelerates them
// before a query reaches this planner in the common case.
var changeFunctions = map[string]bool{
	"rate": true, "irate": true, "deriv": true, "delta": true, "idelta": true,
	"resets": true, "increase": true, "changes": true,
}

func (d *DownsampleHintsPlanner) getValueMerge(fn string) string {
	supportedRangeVectors := map[string]string{
		"absent_over_time":  "1",
		"min_over_time":     "min(min)",
		"max_over_time":     "max(max)",
		"sum_over_time":     "sum(sum)",
		"count_over_time":   "countMerge(count)::Float64",
		"last_over_time":    "argMaxMerge(samples.last)",
		"present_over_time": "1",
		"avg_over_time":     "sum(sum) / countMerge(count)",
	}
	if d.Partial {
		supportedRangeVectors = map[string]string{
			"absent_over_time":  "1",
			"min_over_time":     "min(min)",
			"max_over_time":     "max(max)",
			"sum_over_time":     "sum(sum)",
			"count_over_time":   "countMergeState(count)",
			"last_over_time":    "argMaxMergeState(samples.last)",
			"present_over_time": "1",
			"avg_over_time":     "(sum(sum), countMerge(count))",
		}
	}
	if col, ok := supportedRangeVectors[fn]; ok {
		return col
	} else if d.Partial {
		return "argMaxMergeState(samples.last)"
	}
	return "argMaxMerge(samples.last)"
}

func (d *DownsampleHintsPlanner) getValueFinalize(fn string) string {
	supportedRangeVectors := map[string]string{
		"absent_over_time":  "toFloat64(1)",
		"min_over_time":     "min(val)",
		"max_over_time":     "max(val)",
		"sum_over_time":     "sum(val)",
		"count_over_time":   "countMerge(val)",
		"last_over_time":    "argMaxMerge(val)",
		"present_over_time": "toFloat64(1)",
		"avg_over_time":     "sum(val.1) / sum(val.2)",
	}
	if col, ok := supportedRangeVectors[fn]; ok {
		return col
	}
	return "argMaxMerge(val)"
}
