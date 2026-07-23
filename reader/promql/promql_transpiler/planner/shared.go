package planner

import (
	"fmt"
	"math"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
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
func bucketedValues(ctx *shared.PlannerContext, fpPlanner shared.SQLRequestPlanner,
	lookback time.Duration, cols ...sql.SQLObject) (sql.ISelect, error) {
	producer := &BucketProducer{Fp: fpPlanner, Lookback: lookback, Cols: cols}
	return (&FillGapsPlanner{
		Main:      producer,
		Duration:  lookback,
		ValueCols: producer.ColAliases(),
	}).Process(ctx)
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
