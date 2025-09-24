package planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
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
		End:         sql.WindowPoint{Offset: int32(r.Duration.Milliseconds() - 1)},
	}
	closeWindow := sql.WindowFunction{
		Alias:       "rate_close",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Start:       sql.WindowPoint{Offset: int32(r.Duration.Milliseconds() - 1)},
		End:         sql.WindowPoint{},
	}

	overWnd := func(col sql.SQLObject, wnd *sql.WindowFunction) sql.SQLObject {
		return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strCol, err := col.String(ctx, options...)
			if err != nil {
				return "", err
			}
			strOver, err := (&sql.WindowFunctionRef{wnd}).String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s OVER %s", strCol, strOver), nil
		})
	}
	ranges_req := sql.NewSelect().
		With(withResets).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(overWnd(
				sql.NewRawObject("argMaxIf(val, timestamp_ms, source = 1)"), &openWindow), "start_open"),
			sql.NewCol(overWnd(
				sql.NewRawObject("argMinIf(val, timestamp_ms, source = 1)"), &closeWindow), "start_close"),
			sql.NewCol(overWnd(
				sql.NewRawObject("argMaxIf(val, timestamp_ms, source = 1)"), &closeWindow), "end"),
			sql.NewCol(overWnd(
				sql.NewRawObject("sum(source)"), &closeWindow), "source_end"),
			sql.NewCol(overWnd(
				sql.NewRawObject("maxIf(timestamp_ms, source = 1)"), &openWindow), "end_ms"),
			sql.NewCol(overWnd(
				sql.NewRawObject("sum(reset)"), &closeWindow), "resets"),
		).
		From(sql.NewWithRef(withResets)).
		AddWindows(&openWindow, &closeWindow)
	withRangesReq := sql.NewWith(ranges_req, "rate_ranges")
	return sql.NewSelect().
		With(withRangesReq).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewSimpleCol(fmt.Sprintf(
				"if(source_end != 0, (end - if (start_open > 0, start_open, start_close) + resets) / %f, 0)",
				r.Duration.Seconds()),
				"val")).
		From(sql.NewWithRef(withRangesReq)).
		OrWhere(sql.Gt(sql.NewRawObject("val"), sql.NewIntVal(0)),
			sql.Lt(sql.NewRawObject("timestamp_ms - end_ms"), sql.NewIntVal(r.Duration.Milliseconds()+ctx.Step.Milliseconds()))), nil

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
			"(argMaxIf(val,timestamp_ms, source=1) OVER %s as rate_past_max) * (rate_past_max > val) * (source = 1)",
			strWnd), nil
	})

	return sql.NewSelect().With(withValReq).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
		sql.NewSimpleCol("val", "val"),
		sql.NewCol(resetCol, "reset"),
		sql.NewSimpleCol("source", "source")).
		From(sql.NewWithRef(withValReq)).
		AddWindows(resetWindow), nil
}

func (r *RatePlanner) values(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := r.FpPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	withFp := sql.NewWith(fp, "fp")

	timestampCol := fmt.Sprintf("intDiv(timestamp_ns, %d) * %d",
		ctx.Step.Nanoseconds(), ctx.Step.Milliseconds())

	valReq := sql.NewSelect().With(withFp).Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol(timestampCol, "timestamp_ms"),
		sql.NewSimpleCol("argMaxMerge(last)", "val"),
		sql.NewSimpleCol("1", "source")).
		From(sql.NewRawObject(ctx.Metrics15sDistTableName)).
		AndWhere(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.Add(time.Minute*-5).UnixNano())),
			sql.Le(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp))).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC).
				WithFill(ctx.From.UnixMilli(), ctx.To.UnixMilli(), ctx.Step.Milliseconds()))
	return valReq, nil

}
