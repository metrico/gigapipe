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

// bucketedValues builds the per-bucket value CTE over the 15s downsampled table:
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
// resolution is the width real samples are bucketed to -- see BucketResolution.
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

// changeFunctions are the range functions that measure a change across samples
// rather than reducing over every sample of the window: rate, irate and deriv
// compute a slope, delta and idelta a difference, resets and changes a count of
// transitions, increase a counter's growth.
//
// They are exactly the functions that cannot answer from a single sample, which
// is the whole reason a bucket has to be sized against the range rather than
// against the query's step. The *_over_time reducers are not here: they reduce
// over whatever the window holds, so one sample is already an answer.
//
// This is the single list. Both the request layer (which caps the step so the
// routing in transpileLabelMatchers can see it) and the planners below consult
// it; they must agree, or the SQL buckets at a width the request never chose.
var changeFunctions = map[string]bool{
	"rate": true, "irate": true, "deriv": true, "delta": true, "idelta": true,
	"resets": true, "increase": true, "changes": true,
}

// NeedsDistinctSamples reports whether fn needs two distinct samples inside its
// window to produce a value at all. See changeFunctions.
func NeedsDistinctSamples(fn string) bool {
	return changeFunctions[fn]
}

// BucketResolution returns the width real samples must be grouped to before a
// function that NeedsDistinctSamples evaluates them over a window of duration.
//
// Buckets are keyed by their right edge, so a bucket keyed T covers (T-b, T],
// and the frame reaches the keys T, T-b, ... down to the smallest at or above
// t-duration+1ms. Their union is the window the function actually sees, so the
// width is chosen to make those buckets tile (t-duration, t]: a whole number of
// them across the range, rather than whatever the query's step happens to be.
//
// Two constraints, in order. At most half the duration, so at least two buckets
// are always in reach -- a function measuring a change between samples needs
// two, and at one bucket per window it silently returns nothing however healthy
// the data is. No coarser than the query's own step, so a step finer than the
// range still gets the detail it asked for.
//
// Then the range is divided into a whole number of buckets of at most that
// width. Where the division is exact -- the ordinary case, a round range with a
// round step -- the union is exactly (t-duration, t]. Where it cannot be, the
// bucket is the next size down and the union over-includes less than one bucket
// of extra history: the floor of bucketing at all, since the alternative is a
// partial bucket at the back edge.
//
// It is idempotent -- applying it to its own result changes nothing -- so the
// request layer and the planners can both call it without fighting.
func BucketResolution(step, duration time.Duration) time.Duration {
	if duration <= 0 {
		return time.Millisecond
	}
	widest := duration / 2
	if widest < time.Millisecond {
		// A duration too small to halve on the millisecond grid the SQL is
		// expressed on. Nothing finer can be asked for.
		return time.Millisecond
	}
	if step > 0 && step < widest {
		widest = step
	}

	// The fewest buckets that fit the range without exceeding widest, then the
	// first count at or above it that divides the range evenly. Dividing evenly
	// is what makes the buckets tile the window exactly, and it is also what
	// makes this idempotent: re-applying it finds the same count and returns the
	// same width, so the request layer and the planners cannot disagree.
	//
	// A range given in whole seconds is a multiple of 1000ms and so has a divisor
	// within easy reach; the search is bounded for the ranges that are not, and
	// falls back to the unrounded width, which still tiles to within one bucket.
	first := (duration + widest - 1) / widest
	for n, limit := first, 2*first+64; n <= limit; n++ {
		if duration%n == 0 {
			if b := duration / n; b >= time.Millisecond {
				return b
			}
			break
		}
	}
	if b := duration / first; b >= time.Millisecond {
		return b
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
