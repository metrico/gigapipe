package prof_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MergeJoinedPlanner struct {
	Main shared.SQLRequestPlanner
}

func (j *MergeJoinedPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := j.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(main, "raw")
	preJoined := sql.NewSelect().
		With(withMain).
		Select(sql.NewRawObject("rtree")).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin("array", sql.NewSimpleCol("raw.tree", "rtree"), nil))
	withPreJoined := sql.NewWith(preJoined, "pre_joined")
	res := sql.NewSelect().
		With(withPreJoined).
		Select(
			sql.NewSimpleCol(
				"(rtree.1, rtree.2, rtree.3, sum(rtree.4), sum(rtree.5))",
				"tree")).
		From(sql.NewWithRef(withPreJoined)).
		GroupBy(
			sql.NewRawObject("rtree.1"),
			sql.NewRawObject("rtree.2"),
			sql.NewRawObject("rtree.3")).
		OrderBy(sql.NewRawObject("rtree.1")).
		Limit(sql.NewIntVal(2000000))
	return res, nil
}
