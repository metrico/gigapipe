package planner

import (
	"fmt"
	"github.com/metrico/qryn/v4/reader/config"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
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
		sql.NewSimpleCol(d.getValueMerge(hints.Func), "val").(sql.Aliased))
	if rangeVectors[hints.Func] && hints.Step > hints.Range {
		timeField := fmt.Sprintf("intDiv(samples.timestamp_ns + %d * 1000000, %d * 1000000) * %d",
			hints.Range, hints.Step, hints.Step)
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))
		msInStep := sql.NewRawObject(fmt.Sprintf("timestamp_ns %% %d000000", hints.Step))
		query.AndWhere(sql.Or(
			sql.Eq(msInStep, sql.NewIntVal(0)),
			sql.Gt(msInStep, sql.NewIntVal(hints.Step*1000000-hints.Range*1000000)),
		))
	} else {
		compat4019 := ""
		if config.Cloki.Setting.ClokiReader.Compat_4_0_19 {
			compat4019 = " - 1 "
		}
		timeField := fmt.Sprintf("intDiv(samples.timestamp_ns, %d * 1000000) * %d%s",
			hints.Step, hints.Step, compat4019)
		patchField(query, "timestamp_ms",
			sql.NewSimpleCol(timeField, "timestamp_ms").(sql.Aliased))
	}
	if d.Hints.Func == "count_over_time" {
		query = d.countOverTime(query)
	}

	return query, nil
}

func (d *DownsampleHintsPlanner) countOverTime(query sql.ISelect) sql.ISelect {
	query.Select(append(query.GetSelect(), sql.NewSimpleCol("range(toInt64(val))", "arr"))...)
	withQuery := sql.NewWith(query, "pre_count_over_time")
	res := sql.NewSelect().
		With(withQuery).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("1", "val"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms")).
		From(sql.NewWithRef(withQuery)).
		Join(sql.NewJoin("array", sql.NewSimpleCol("arr", "arr"), nil))
	return res
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
