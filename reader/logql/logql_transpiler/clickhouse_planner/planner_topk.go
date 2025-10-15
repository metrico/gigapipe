package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type TopKPlanner struct {
	Main  shared.SQLRequestPlanner
	Len   int
	IsTop bool
}

func (t *TopKPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := t.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	hasLabels := hasColumn(main.GetSelect(), "labels")

	withParA := sql.NewWith(main, "par_a")
	lambda := ""
	if t.IsTop {
		lambda = "x -> (-x.1, x.2"
		if hasLabels {
			lambda += ", x.3"
		}
		lambda += "),"
	}

	tuple := "par_a.value, par_a.fingerprint"
	if hasLabels {
		tuple += ", par_a.labels"
	}

	q := sql.NewSelect().
		With(withParA).
		Select(
			sql.NewSimpleCol("par_a.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol(
				fmt.Sprintf(
					"arraySlice(arraySort(%sgroupArray((%s))), 1, %d)",
					lambda,
					tuple,
					t.Len),
				"slice")).
		From(sql.NewWithRef(withParA)).
		GroupBy(sql.NewRawObject("timestamp_ns"))

	withParB := sql.NewWith(q, "par_b")

	q = sql.NewSelect().
		With(withParB).
		Select(
			sql.NewSimpleCol("arr_b.2", "fingerprint"),
			sql.NewSimpleCol("par_b.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("arr_b.1", "value"),
			sql.NewSimpleCol("''", "string")).
		From(sql.NewWithRef(withParB)).
		Join(sql.NewJoin("array", sql.NewSimpleCol("par_b.slice", "arr_b"), nil))

	if hasLabels {
		q.Select(append(q.GetSelect(), sql.NewSimpleCol("arr_b.3", "labels"))...)
	}
	return q, nil
}
