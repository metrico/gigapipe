package planner

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// BucketProducer reads the 15s downsampled table and produces one row per
// (fingerprint, bucket) with the requested partial aggregates. It is the raw
// per-bucket value extractor: no source column, no grid fill, just the real
// buckets. FillGapsPlanner is layered on top to densify it onto the same grid.
//
// Lookback extends the read window before ctx.From so the earliest steps see the
// buckets their frame or fill reaches back into. It is the same quantity the fill
// is sized to: the furthest a sample can influence a step.
//
// Resolution is the bucket width. It is usually ctx.Step, but callers that need
// to tell two samples apart within a window narrower than ctx.Step -- the range
// functions in CounterPlanner and CounterFlagsPlanner -- pass a finer one; see
// BucketResolution.
type BucketProducer struct {
	Fp         shared.SQLRequestPlanner
	Lookback   time.Duration
	Resolution time.Duration
	Cols       []sql.SQLObject
}

// ColAliases returns the aliases of the value columns, for handing to
// FillGapsPlanner so it can carry them through the arrayJoin fallback.
func (b *BucketProducer) ColAliases() []string {
	aliases := make([]string, len(b.Cols))
	for i, c := range b.Cols {
		aliases[i] = c.(sql.Aliased).GetAlias()
	}
	return aliases
}

func (b *BucketProducer) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := b.Fp.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFp := sql.NewWith(fp, "fp")

	// The window frame layered on top reaches the keys inside (t-range, t], so
	// with this keying those buckets tile backwards from exactly t.
	timestampCol := bucketTimestampCol("timestamp_ns", b.Resolution)

	sel := []sql.SQLObject{
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol(timestampCol, "timestamp_ms"),
	}
	sel = append(sel, b.Cols...)

	return sql.NewSelect().With(withFp).Select(sel...).
		From(sql.NewRawObject(ctx.Metrics15sDistTableName)).
		AndWhere(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.Add(-b.Lookback).UnixNano())),
			sql.Le(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp))).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")), nil
}
