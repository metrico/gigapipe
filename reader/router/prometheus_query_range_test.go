package router

import (
	"context"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
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

// staleLabelsGetter is a minimal model.ILabelsGetter for a single series.
type staleLabelsGetter struct {
	lbls model.Labels
}

func (g staleLabelsGetter) Get(uint64) model.Labels { return g.lbls }
func (g staleLabelsGetter) GetNative(uint64) labels.Labels {
	return labels.New(g.lbls...)
}

// oneSampleQueryable returns a single prolonging series carrying exactly one
// real sample at sampleTsMs. Prolong=true routes it through prolongSeriesIt,
// the iterator under test, which emits the real sample plus one stale marker.
type oneSampleQueryable struct {
	sampleTsMs int64
	value      float64
	stepMs     int64
}

func (q oneSampleQueryable) Querier(int64, int64) (storage.Querier, error) {
	return oneSampleQuerier(q), nil
}

type oneSampleQuerier oneSampleQueryable

func (q oneSampleQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	step := q.stepMs
	if hints.Step != 0 {
		step = hints.Step
	}
	getter := staleLabelsGetter{lbls: model.Labels{{Name: "__name__", Value: "up"}}}
	ss := &model.SeriesSet{
		Series: []*model.SeriesV2{{
			LabelsGetter: getter,
			Fp:           1,
			Samples:      []model.Sample{{TimestampMs: q.sampleTsMs, Value: q.value}},
			Prolong:      true,
			StepMs:       step,
		}},
	}
	ss.Reset()
	return ss
}

func (oneSampleQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (oneSampleQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (oneSampleQuerier) Close() error { return nil }

// instantValueAt runs an instant query for `up` at evalT and reports whether the
// series is present (and its value). This is how the Prometheus engine surfaces
// carry-forward: at each instant it looks back up to LookbackDelta for the most
// recent sample, stopping at a stale marker.
func instantValueAt(t *testing.T, eng *promql.Engine, q storage.Queryable, evalT time.Time) (present bool, val float64) {
	t.Helper()
	query, err := eng.NewInstantQuery(context.Background(), q, nil, "up", evalT)
	if err != nil {
		t.Fatalf("NewInstantQuery @%s: %v", evalT.UTC(), err)
	}
	defer query.Close()
	res := query.Exec(context.Background())
	if res.Err != nil {
		t.Fatalf("Exec @%s: %v", evalT.UTC(), res.Err)
	}
	v, err := res.Vector()
	if err != nil {
		t.Fatalf("Vector @%s: %v", evalT.UTC(), err)
	}
	if len(v) == 0 {
		return false, 0
	}
	return true, v[0].F
}

// TestEngine_StaleMarkerStopsCarryForward drives the production PromQL engine
// (NewPromEngine, LookbackDelta=0 => 5m default) over the prolonging iterator to
// verify the fix for issue #931. A single real sample at T is queried as an
// instant vector selector at increasing evaluation times. The iterator yields
// the real sample and a stale marker at T + LookbackDeltaMs (5m), so the value
// is carried forward for the engine's full 5m lookback and then stops - never
// the ~10m the old fill + engine-lookback stacking produced.
//
// The assertions therefore check BOTH ends: the value is still present within
// the 5m window (Prometheus-faithful carry-forward) and gone well after it
// (issue #931 - no stacking). The T+8m check is what fails under the old
// behavior.
func TestEngine_StaleMarkerStopsCarryForward(t *testing.T) {
	const stepMs = int64(15000)
	sampleT := time.Unix(600, 0) // T = 600s
	queryable := oneSampleQueryable{
		sampleTsMs: sampleT.UnixMilli(),
		value:      42,
		stepMs:     stepMs,
	}

	eng := NewPromEngine(50_000_000)

	// At T: the real sample is present.
	if present, v := instantValueAt(t, eng, queryable, sampleT); !present || v != 42 {
		t.Fatalf("at T: expected value 42 present, got present=%v val=%v", present, v)
	}

	// Within the 5m lookback window the value is carried forward, exactly as
	// upstream Prometheus does - the marker sits at the boundary, not before it.
	if present, v := instantValueAt(t, eng, queryable, sampleT.Add(4*time.Minute)); !present || v != 42 {
		t.Errorf("at T+4m: expected value 42 still carried forward within the 5m lookback, "+
			"got present=%v val=%v", present, v)
	}

	// The critical assertion for #931: by T+8m the value MUST be gone. Under the
	// old stacked behavior (5m forward-fill + 5m engine lookback) it would still
	// be present at ~T+8m.
	if present, v := instantValueAt(t, eng, queryable, sampleT.Add(8*time.Minute)); present {
		t.Errorf("at T+8m: value still present (val=%v) - carry-forward is stacking; "+
			"the stale marker should have stopped it by ~T+5m", v)
	}

	// And well past any single lookback window it must certainly be gone.
	if present, v := instantValueAt(t, eng, queryable, sampleT.Add(11*time.Minute)); present {
		t.Errorf("at T+11m: value still present (val=%v) - series was never terminated", v)
	}
}
