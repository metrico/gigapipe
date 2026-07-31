package planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	dbversion "github.com/metrico/qryn/v4/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// FillGapsPlanner densifies its Main producer onto the step grid. Main must yield
// rows of (fingerprint, timestamp_ms, <value cols>) at real sample buckets, with
// no source column and no fill; FillGapsPlanner adds source = 0 evaluation rows so
// that a downstream window aggregate is evaluated at every step, out to Duration
// past each real sample.
//
// The filled rows are evaluation points, never data: their stored values are
// irrelevant, and every consumer must recompute its value with an
// -If(..., source = 1) form. Duration is the gap the fill is sized to -- the range
// R for an over-time aggregator, the 5m lookback delta before a cross-series
// aggregation. See the fill semantics in fillGaps.
type FillGapsPlanner struct {
	Main      shared.SQLRequestPlanner
	Duration  time.Duration
	ValueCols []string
}

func (f *FillGapsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	grouped, err := f.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	return fillGaps(grouped, ctx, f.Duration, f.ValueCols), nil
}

// fillGaps densifies grouped onto the step grid, choosing the native STALENESS
// clause when the server supports it (clickhouse >= 24.11) and the arrayJoin
// range expansion otherwise. Both produce identical rows: real buckets carry
// source = 1, generated evaluation points carry source = 0, out to duration past
// each real bucket.
func fillGaps(grouped sql.ISelect, ctx *shared.PlannerContext, duration time.Duration,
	valueCols []string) sql.ISelect {
	if ctx.VersionInfo.HasCapability(dbversion.CapStaleness) {
		return fillStaleness(grouped, ctx, duration)
	}
	return fillArrayJoin(grouped, ctx, duration, valueCols)
}

// fillStaleness densifies grouped with ORDER BY ... WITH FILL STALENESS, the
// native path on clickhouse >= 24.11. Filled rows carry source = 0 by default.
func fillStaleness(grouped sql.ISelect, ctx *shared.PlannerContext, duration time.Duration) sql.ISelect {
	grouped.Select(append(grouped.GetSelect(), sql.NewSimpleCol("1", "source"))...)
	return grouped.OrderBy(
		sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
		sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC).
			WithFillStaleness(ctx.To.UnixMilli(), ctx.Step.Milliseconds(), duration.Milliseconds()))
}

// fillArrayJoin densifies grouped without WITH FILL STALENESS, for clickhouse <
// 24.11 where that clause is a parse error. Each real bucket is expanded to the
// steps it covers -- from itself up to the earlier of the next bucket or a
// duration later -- so the output has the exact same cardinality as the staleness
// path: dense data expands 1:1, only real gaps generate rows.
//
// Filled rows carry the source-bucket's values verbatim rather than a zero
// default; that is harmless because every consumer masks them with an
// -If(..., source = 1) form, exactly as it does the staleness path's zero rows.
func fillArrayJoin(grouped sql.ISelect, ctx *shared.PlannerContext, duration time.Duration,
	valueCols []string) sql.ISelect {
	withGrouped := sql.NewWith(grouped, "bv_grouped")

	durationMs := duration.Milliseconds()
	// bucket_ms + duration as the leadInFrame default keeps the last bucket of a
	// series alive for a full duration forward, matching STALENESS filling past
	// the final real row.
	leadWnd := &sql.WindowFunction{
		Alias:       "bv_lead_wnd",
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Rows:        true,
		Start:       sql.WindowPoint{},
		End:         sql.WindowPoint{Offset: 1, IsFollowing: true},
	}
	leadExpr := fmt.Sprintf("leadInFrame(toInt64(timestamp_ms), 1, toInt64(timestamp_ms) + toInt64(%d))", durationMs)

	leadSel := []sql.SQLObject{
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("toInt64(timestamp_ms)", "bucket_ms"),
	}
	for _, alias := range valueCols {
		leadSel = append(leadSel, sql.NewSimpleCol(alias, alias))
	}
	leadSel = append(leadSel, sql.NewCol(overWnd(sql.NewRawObject(leadExpr), leadWnd), "next_ms"))

	withLead := sql.NewWith(
		sql.NewSelect().With(withGrouped).Select(leadSel...).
			From(sql.NewWithRef(withGrouped)).
			AddWindows(leadWnd),
		"bv_lead")

	// range(start, end, step) is [start, end): every step from the bucket up to,
	// but excluding, whichever comes first of the next bucket, a duration later,
	// or the query end. next_ms is another bucket and so already step aligned.
	tsExpr := fmt.Sprintf(
		"arrayJoin(range(bucket_ms, least(bucket_ms + toInt64(%d), next_ms, toInt64(%d)), toInt64(%d)))",
		durationMs, ctx.To.UnixMilli(), ctx.Step.Milliseconds())

	finalSel := []sql.SQLObject{
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol(tsExpr, "timestamp_ms"),
	}
	for _, alias := range valueCols {
		finalSel = append(finalSel, sql.NewSimpleCol(alias, alias))
	}
	// Only the row landing on the bucket itself is real; the rest are carried.
	finalSel = append(finalSel, sql.NewSimpleCol("toUInt8(timestamp_ms = bucket_ms)", "source"))

	return sql.NewSelect().With(withGrouped, withLead).Select(finalSel...).
		From(sql.NewWithRef(withLead)).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC))
}
