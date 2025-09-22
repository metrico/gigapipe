package clickhouse_planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"time"
)

type RatePlanner struct {
	FpPlanner shared.SQLRequestPlanner
	Duration  time.Duration
}

func (r *RatePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	resetsReq, err := r.resets(ctx)
	if err != nil {
		return nil, err
	}
	withResets := sql.NewWith(resetsReq, "rate_resets")
	openWindow := sql.WindowFunction{
		Alias:       "rate_open",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Start:       sql.WindowPoint{Offset: int32(r.Duration.Milliseconds() + 300000)},
		End:         sql.WindowPoint{Offset: int32(r.Duration.Milliseconds())},
	}
	closeWindow := sql.WindowFunction{
		Alias:       "rate_close",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Start:       sql.WindowPoint{Offset: int32(r.Duration.Milliseconds())},
		End:         sql.WindowPoint{},
	}
	valCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		strOpenWnd, err := (&sql.WindowFunctionRef{&openWindow}).String(ctx, options...)
		if err != nil {
			return "", err
		}
		strCloseWnd, err := (&sql.WindowFunctionRef{&closeWindow}).String(ctx, options...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(
			"(val - argMax(val, timestamp_ms) OVER %s + sum(reset) OVER %s) / %f",
			strOpenWnd,
			strCloseWnd,
			r.Duration.Seconds()), nil
	})
	return sql.NewSelect().
		With(withResets).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(valCol, "val"),
		).
		From(sql.NewWithRef(withResets)).
		AddWindows(&openWindow, &closeWindow).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)), nil
}

func (r *RatePlanner) resets(ctx *shared.PlannerContext) (sql.ISelect, error) {
	valReq, err := r.values(ctx)
	if err != nil {
		return nil, err
	}

	withValReq := sql.NewWith(valReq, "rate_val")

	resetOffset := max(ctx.Step.Milliseconds(), 300000)

	resetWindow := &sql.WindowFunction{
		Alias:       "rate_reset",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy: []sql.SQLObject{
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC),
		},
		Start: sql.WindowPoint{Offset: int32(resetOffset)},
		End:   sql.WindowPoint{Offset: int32(1)},
	}

	resetCol := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		strWnd, err := (&sql.WindowFunctionRef{resetWindow}).String(ctx, options...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(
			"(argMax(val,timestamp_ms) OVER %s as rate_past_max) * (rate_past_max > val)",
			strWnd), nil
	})

	return sql.NewSelect().With(withValReq).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol("val", "val"),
		sql.NewCol(resetCol, "reset")).
		From(sql.NewWithRef(withValReq)).
		AddWindows(resetWindow), nil
}

func (r *RatePlanner) values(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := r.FpPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	withFp := sql.NewWith(fp, "fp")

	timestampCol := fmt.Sprintf("intDiv(timestamp_ns, 1000000 * %d) * %d",
		ctx.Step.Milliseconds(), ctx.Step.Milliseconds())

	valReq := sql.NewSelect().With(withFp).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol(timestampCol, "timestamp_ms"),
		sql.NewSimpleCol("argMaxMerge(last)", "val")).
		From(sql.NewRawObject(ctx.Metrics15sTableName)). //TODO: DIST SUPPORT
		AndWhere(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.Add(time.Minute*5).UnixNano())),
			sql.Le(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp))).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms"))
	return valReq, nil

}
