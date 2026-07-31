package planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// BucketProducer reads the 15s downsampled table and produces one row per
// (fingerprint, step bucket) with the requested partial aggregates. It is the
// raw per-step value extractor: no source column, no grid fill, just the real
// buckets. FillGapsPlanner is layered on top to densify it onto the step grid.
//
// Lookback extends the read window before ctx.From so the earliest steps see the
// buckets their frame or fill reaches back into. It is the same quantity the fill
// is sized to: the furthest a sample can influence a step.
type BucketProducer struct {
	Fp       shared.SQLRequestPlanner
	Lookback time.Duration
	Cols     []sql.SQLObject
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

	timestampCol := fmt.Sprintf("intDiv(timestamp_ns, %d) * %d",
		ctx.Step.Nanoseconds(), ctx.Step.Milliseconds())

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
