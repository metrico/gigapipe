package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// globalInCondition renders `left GLOBAL IN (subquery)`. Used in cluster mode
// where a plain IN between two distributed tables is denied by ClickHouse.
type globalInCondition struct {
	left  sql.SQLObject
	right sql.ISelect
}

func (g *globalInCondition) GetFunction() string { return "GLOBAL IN" }

func (g *globalInCondition) GetEntity() []sql.SQLObject { return []sql.SQLObject{g.left} }

func (g *globalInCondition) String(ctx *sql.Ctx, options ...int) (string, error) {
	l, err := g.left.String(ctx, options...)
	if err != nil {
		return "", err
	}
	r, err := g.right.String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s GLOBAL IN (%s)", l, r), nil
}

type LabelsJoinPlanner struct {
	NoStreamSelect bool
	Main           shared.SQLRequestPlanner
	Fingerprints   shared.SQLRequestPlanner
	TimeSeries     shared.SQLRequestPlanner
	FpCache        **sql.With
	LabelsCache    **sql.With
}

func (l *LabelsJoinPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var (
		tsReq sql.ISelect
		err   error
	)
	if !l.NoStreamSelect {
		tsReq, err = (&WithConnectorPlanner{
			Main:      l.TimeSeries,
			With:      l.Fingerprints,
			Alias:     "fp_sel",
			WithCache: l.FpCache,

			ProcessFn: func(q sql.ISelect, w *sql.With) (sql.ISelect, error) {
				return q.AndPreWhere(sql.NewIn(sql.NewRawObject("time_series.fingerprint"), sql.NewWithRef(w))), nil
			},
		}).Process(ctx)
	} else {
		tsReq, err = l.TimeSeries.Process(ctx)
	}
	if err != nil {
		return nil, err
	}

	mainReq, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(mainReq, "main")

	// Bound label resolution to fingerprints that actually appear in the
	// time-windowed `main` subquery, avoiding a huge map build for
	// high-cardinality selectors. See issue #702.
	boundTimeSeriesToWindow(ctx, tsReq, withMain)

	withTS := sql.NewWith(tsReq, "_time_series")
	if l.LabelsCache != nil {
		*l.LabelsCache = withTS
	}

	joinType := "ANY LEFT "
	if ctx.IsCluster {
		withTSRef := sql.NewWithRef(withTS)
		withTSSelect := sql.NewSelect().
			Select(sql.NewSimpleCol("mapFromArrays(groupArray(fingerprint), groupArray(labels))", "map")).
			From(sql.NewCol(withTSRef, "_time_series"))

		labelsCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strRes, err := withTSSelect.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s)[main.fingerprint]", strRes), nil
		})
		return sql.NewSelect().
			With(withMain, withTS).
			Select(
				sql.NewSimpleCol("main.fingerprint", "fingerprint"),
				sql.NewSimpleCol("main.timestamp_ns", "timestamp_ns"),
				sql.NewCol(labelsCol, "labels"),
				sql.NewSimpleCol("main.string", "string"),
				sql.NewSimpleCol("main.value", "value")).
			From(sql.NewWithRef(withMain)), nil

	}
	return sql.NewSelect().
		With(withMain, withTS).
		Select(
			sql.NewSimpleCol("main.fingerprint", "fingerprint"),
			sql.NewSimpleCol("main.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("_time_series.labels", "labels"),
			sql.NewSimpleCol("main.string", "string"),
			sql.NewSimpleCol("main.value", "value")).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin(
			joinType,
			sql.NewWithRef(withTS),
			sql.Eq(sql.NewRawObject("main.fingerprint"), sql.NewRawObject("_time_series.fingerprint")))), nil
}
