package transpiler

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
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

	patchField(query, "value",
		sql.NewSimpleCol(d.getValueMerge(hints.Func), "value").(sql.Aliased))
	if rangeVectors[hints.Func] && hints.Step > hints.Range {
		timeField := fmt.Sprintf("intDiv(samples.timestamp_ns + %d000000, %d * 1000000) * %d - 1",
			hints.Range, hints.Step, hints.Step)
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))

		msInStep := sql.NewRawObject(fmt.Sprintf("timestamp_ns %% %d000000", hints.Step))
		query.AndWhere(sql.Or(
			sql.Eq(msInStep, sql.NewIntVal(0)),
			sql.Gt(msInStep, sql.NewIntVal(hints.Step*1000000-hints.Range*1000000)),
		))
	} else {
		timeField := fmt.Sprintf("intDiv(samples.timestamp_ns, %d * 1000000) * %d - 1",
			hints.Step, hints.Step)
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
		"count_over_time":   "countMerge(count)",
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
		"min_over_time":     "min(value)",
		"max_over_time":     "max(value)",
		"sum_over_time":     "sum(value)",
		"count_over_time":   "countMerge(value)",
		"last_over_time":    "argMaxMerge(value)",
		"present_over_time": "toFloat64(1)",
		"avg_over_time":     "sum(value.1) / sum(value.2)",
	}
	if col, ok := supportedRangeVectors[fn]; ok {
		return col
	}
	return "argMaxMerge(value)"
}
