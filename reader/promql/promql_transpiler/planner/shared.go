package planner

import (
	"fmt"
	"math"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// staleness is how far back a preceding sample is still considered valid,
// mirroring the prometheus staleness delta.
const staleness = time.Minute * 5

func patchField(query sql.ISelect, alias string, newField sql.Aliased) sql.ISelect {
	_select := make([]sql.SQLObject, len(query.GetSelect()))
	for i, f := range query.GetSelect() {
		if f.(sql.Aliased).GetAlias() != alias {
			_select[i] = f
			continue
		}
		_select[i] = newField
	}
	query.Select(_select...)
	return query
}

// overWnd renders `col OVER wnd`, referencing wnd by its alias from the
// WINDOW clause of the enclosing select.
func overWnd(col sql.SQLObject, wnd *sql.WindowFunction) sql.SQLObject {
	return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		strCol, err := col.String(ctx, options...)
		if err != nil {
			return "", err
		}
		strOver, err := (&sql.WindowFunctionRef{Fn: wnd}).String(ctx, options...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s OVER %s", strCol, strOver), nil
	})
}

// windowOffset converts a duration to a RANGE frame offset. WindowPoint.Offset
// is an int32 of milliseconds, so ranges beyond ~24.8 days cannot be expressed
// and must be rejected rather than silently wrapped into a bogus frame.
func windowOffset(d time.Duration) (int32, error) {
	ms := d.Milliseconds()
	if ms > math.MaxInt32 || ms < math.MinInt32 {
		return 0, fmt.Errorf("range %s is too large to accelerate: window offsets are limited to %s",
			d, time.Duration(math.MaxInt32)*time.Millisecond)
	}
	return int32(ms), nil
}

// bucketedValues builds the per-step value CTE over the 15s downsampled table:
// a BucketProducer read densified by FillGapsPlanner. cols are the bucket level
// partial aggregates to expose.
//
// The fill materializes a row at every step so the window aggregate layered on
// top is evaluated everywhere, not only where a real bucket lands; those filled
// rows carry source = 0, so every such aggregate must use an -If(..., source = 1)
// form to keep them from contributing as data.
//
// lookback is both how far back the read window extends before ctx.From and how
// far forward a real row is filled. That is one quantity, not two: it is the
// furthest a sample can influence a step, so it is exactly what the first steps
// must be able to reach back to and exactly how long a sample stays relevant.
//
// resolution is the width real samples are bucketed to -- see bucketResolution.
func bucketedValues(ctx *shared.PlannerContext, fpPlanner shared.SQLRequestPlanner,
	lookback, resolution time.Duration, cols ...sql.SQLObject) (sql.ISelect, error) {
	producer := &BucketProducer{Fp: fpPlanner, Lookback: lookback, Resolution: resolution, Cols: cols}
	return (&FillGapsPlanner{
		Main:       producer,
		Duration:   lookback,
		Resolution: resolution,
		ValueCols:  producer.ColAliases(),
	}).Process(ctx)
}

// bucketResolution returns the width real samples are grouped to before a
// range function evaluates them.
//
// ctx.Step is used whenever it already leaves room for at least two buckets
// inside any (t-duration, t] window -- the normal case, where the query's step
// is finer than the function's own range. Once step reaches or exceeds
// duration, every bucket lands exactly on the query's step grid and a whole
// (t-duration, t] window can hold at most one of them: a function that
// measures a change between two samples (rate, increase, delta, resets,
// changes) then never sees more than a single point and always yields
// nothing, silently, however healthy the underlying data is. Falling back to
// duration/2 keeps at least two buckets in reach regardless of how coarse the
// query's step is.
func bucketResolution(step, duration time.Duration) time.Duration {
	if step < duration {
		return step
	}
	if half := duration / 2; half > 0 {
		return half
	}
	return time.Millisecond
}

// rangeFrame is the frame covering (t-duration, t], the sample set a prometheus
// range selector [duration] sees at each step.
func rangeFrame(alias string, duration time.Duration) (*sql.WindowFunction, error) {
	start, err := windowOffset(duration - time.Millisecond)
	if err != nil {
		return nil, err
	}
	return &sql.WindowFunction{
		Alias:       alias,
		PartitionBy: []sql.SQLObject{sql.NewRawObject("fingerprint")},
		OrderBy:     []sql.SQLObject{sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC)},
		Start:       sql.WindowPoint{Offset: start},
		End:         sql.WindowPoint{},
	}, nil
}
