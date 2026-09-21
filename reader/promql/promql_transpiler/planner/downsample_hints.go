package planner

import (
	"fmt"
	"time"

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

	compat4019 := ""
	if config.Cloki.Setting.ClokiReader.Compat_4_0_19 {
		compat4019 = " - 1 "
	}

	// Three shapes, and which one applies is decided by what the function needs
	// from its window -- not by the step alone.
	switch {
	case NeedsDistinctSamples(hints.Func):
		// The bucket is handed to the engine's own implementation of the function
		// as if it were the raw series, one point per bucket picked by
		// argMax(last, ts), so the function sees only as many samples as there are
		// buckets inside the (t-range, t] window it evaluates. Buckets are aligned
		// to epoch, not to the query's evaluation timestamps, so how many a window
		// catches depends on an alignment the caller never controls: near or above
		// step == range this came back empty, or with one point, or worked, at
		// random. BucketResolution is what keeps two in reach.
		//
		// The trailing-window shape below is not an option here, however coarse the
		// step: it collapses every sample of the window onto a single bucket key,
		// which is precisely the one input these functions cannot work from.
		width := BucketResolution(
			time.Duration(hints.Step)*time.Millisecond,
			time.Duration(hints.Range)*time.Millisecond)
		timeField := bucketTimestampCol("samples.timestamp_ns", width) + compat4019
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))

	case rangeVectors[hints.Func] && hints.Step > hints.Range:
		// A reducer whose step outruns its range: only the trailing range of each
		// step can contribute, so read just that and snap it forward onto the step
		// it belongs to. One bucket is all a reducer needs, and the WHERE keeps the
		// scan proportional to the range rather than to the whole span.
		timeField := fmt.Sprintf("intDiv(samples.timestamp_ns + %d * 1000000, %d * 1000000) * %d",
			hints.Range, hints.Step, hints.Step)
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))
		msInStep := sql.NewRawObject(fmt.Sprintf("timestamp_ns %% %d000000", hints.Step))
		query.AndWhere(sql.Or(
			sql.Eq(msInStep, sql.NewIntVal(0)),
			sql.Gt(msInStep, sql.NewIntVal(hints.Step*1000000-hints.Range*1000000)),
		))

	default:
		// A bare selector, or a function the engine evaluates itself over the
		// bucketed series. The value reported at t must be the newest sample at
		// or before t, so the bucket has to hold (key-Step, key]. Keyed by the
		// floor it held [key, key+Step) and argMaxMerge(last) then returned the
		// newest sample inside it -- the value belonging to t+Step. Measured
		// against Prometheus over the same data, every sample came back one step
		// early: value[i] was Prometheus's value[i+1] for the whole series.
		timeField := bucketTimestampCol("samples.timestamp_ns",
			time.Duration(hints.Step)*time.Millisecond) + compat4019
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))
	}

	return query, nil
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
