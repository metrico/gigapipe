package router

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// emptyQueryable is a stub storage.Queryable that returns no series. It lets us
// drive the PromQL engine through query planning/execution without a backend.
type emptyQueryable struct{}

func (emptyQueryable) Querier(int64, int64) (storage.Querier, error) {
	return emptyQuerier{}, nil
}

type emptyQuerier struct{}

func (emptyQuerier) Select(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}

func (emptyQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (emptyQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (emptyQuerier) Close() error { return nil }

// TestSubqueryWithoutStep guards against a regression where a subquery that
// omits its resolution step (e.g. `up[1h:]`) panicked with a nil pointer
// dereference because the engine's NoStepSubqueryIntervalFn was nil.
func TestSubqueryWithoutStep(t *testing.T) {
	eng := NewPromEngine(5000000)
	ctx := context.Background()

	q, err := eng.NewInstantQuery(ctx, emptyQueryable{}, nil, "avg_over_time(up[1h:])", time.Unix(0, 0))
	if err != nil {
		t.Fatalf("NewInstantQuery: %v", err)
	}
	defer q.Close()

	res := q.Exec(ctx)
	if res.Err != nil {
		t.Fatalf("Exec: %v", res.Err)
	}
}
