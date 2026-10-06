package service

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// barrier holds every query until n have been issued, recording each issue and completion; a
// query left waiting fails.
type barrier struct {
	n      int
	mtx    sync.Mutex
	events []string
	all    chan struct{}
}

func newBarrier(n int) *barrier { return &barrier{n: n, all: make(chan struct{})} }

func (b *barrier) record(e string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.events = append(b.events, e)
	if strings.HasPrefix(e, "issue ") && len(b.events) == b.n {
		close(b.all)
	}
}

func (b *barrier) wait(kind string, answer func() (fakeclickhouse.Result, error)) (fakeclickhouse.Result, error) {
	b.record("issue " + kind)
	select {
	case <-b.all:
	case <-time.After(2 * time.Second):
		return fakeclickhouse.Result{}, errors.New(kind + " was issued alone")
	}
	defer b.record("complete " + kind)
	return answer()
}

func queryKind(query string) string {
	if strings.Contains(query, "ARRAY JOIN") {
		return "points"
	}
	return "labels"
}

func pointsResult(rows ...[]driver.Value) func() (fakeclickhouse.Result, error) {
	return func() (fakeclickhouse.Result, error) {
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "t_ms", "value"}, Rows: rows}, nil
	}
}

func labelsResult(rows ...[]driver.Value) func() (fakeclickhouse.Result, error) {
	return func() (fakeclickhouse.Result, error) {
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "labels"}, Rows: rows}, nil
	}
}

func querierOf(t *testing.T, db *fakeclickhouse.DB, expr *promql_parser.Expr) storage.Querier {
	t.Helper()
	querier, err := (&CLokiQueriable{ServiceData: model.ServiceData{Session: db}}).
		SetOidAndDB(context.Background(), expr).Querier(0, 300000)
	if err != nil {
		t.Fatal(err)
	}
	return querier
}

var substituteHints = &storage.SelectHints{Start: -239999, End: 300000, Step: 60000}

func substituteMatcher(name string) *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchEqual, "__name__", name)
}

func TestSelectIssuesASubstitutesPointsAndLabelsTogether(t *testing.T) {
	b := newBarrier(2)
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		if queryKind(query) == "points" {
			return b.wait("points", pointsResult([]driver.Value{uint64(7), int64(60000), 1.0}))
		}
		return b.wait("labels", labelsResult([]driver.Value{uint64(7), map[string]string{"job": "a"}}))
	})
	set := querierOf(t, db, rateSubstitute(t)).Select(context.Background(), true, substituteHints,
		substituteMatcher("__metric_subst__1"))
	if !set.Next() || set.At().Labels().String() != `{job="a"}` || set.Next() {
		t.Fatalf("want the one series {job=\"a\"}, err = %v", set.Err())
	}
	if set.Err() != nil {
		t.Fatal(set.Err())
	}
	for i, e := range b.events[:2] {
		if !strings.HasPrefix(e, "issue ") {
			t.Fatalf("event %d is %q, want both reads issued before either completes: %q", i, e, b.events)
		}
	}
}

// twoSubstitutes is a ratio of two pushed-down instant selectors.
func twoSubstitutes(t *testing.T) *promql_parser.Expr {
	expr := parse(t, "__metric_subst__1 / __metric_subst__2")
	for name, metric := range map[string]string{"__metric_subst__1": "a", "__metric_subst__2": "b"} {
		expr.Substitutes[name] = &promql_parser.Substitute{MetricName: name,
			Pushdown: metricread.Pushdown{Grid: metricread.Grid{StartMs: 60000, EndMs: 300000, StepMs: 60000},
				RangeMs: 300000, Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", metric)}}}
	}
	return expr
}

func TestSelectReturnsBeforeItsReadsSoSelectorsOverlap(t *testing.T) {
	b := newBarrier(4)
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		metric := "a"
		if strings.Contains(query, "(name = 'b')") {
			metric = "b"
		}
		if queryKind(query) == "points" {
			return b.wait("points "+metric, pointsResult([]driver.Value{uint64(1), int64(60000), 1.0}))
		}
		return b.wait("labels "+metric, labelsResult([]driver.Value{uint64(1), map[string]string{"__name__": metric}}))
	})
	querier := querierOf(t, db, twoSubstitutes(t))
	first := querier.Select(context.Background(), false, substituteHints, substituteMatcher("__metric_subst__1"))
	second := querier.Select(context.Background(), false, substituteHints, substituteMatcher("__metric_subst__2"))
	for name, set := range map[string]storage.SeriesSet{"a": first, "b": second} {
		if !set.Next() || set.At().Labels().String() != `{__name__="`+name+`"}` {
			t.Fatalf("%s: want its one series, err = %v", name, set.Err())
		}
		if set.Next() || set.Err() != nil {
			t.Fatalf("%s: err = %v", name, set.Err())
		}
	}
}

func TestSelectReportsAFailedSubstituteRead(t *testing.T) {
	for _, failing := range []string{"points", "labels"} {
		t.Run(failing, func(t *testing.T) {
			db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
				kind := queryKind(query)
				if kind == failing {
					return fakeclickhouse.Result{}, errors.New(kind + " failed")
				}
				if kind == "points" {
					return pointsResult([]driver.Value{uint64(7), int64(60000), 1.0})()
				}
				return labelsResult([]driver.Value{uint64(7), map[string]string{"job": "a"}})()
			})
			set := querierOf(t, db, rateSubstitute(t)).Select(context.Background(), true, substituteHints,
				substituteMatcher("__metric_subst__1"))
			if set.Next() || set.Err() == nil || !strings.Contains(set.Err().Error(), failing+" failed") {
				t.Fatalf("err = %v, want the %s read's error", set.Err(), failing)
			}
		})
	}
}

// A series indexed between the two reads has points but no label row in the first labels read.
func TestSelectRereadsTheLabelsOnceForASeriesIndexedBetweenTheReads(t *testing.T) {
	var mtx sync.Mutex
	labelReads := 0
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		if queryKind(query) == "points" {
			return pointsResult([]driver.Value{uint64(7), int64(60000), 1.0}, []driver.Value{uint64(8), int64(60000), 2.0})()
		}
		mtx.Lock()
		labelReads++
		n := labelReads
		mtx.Unlock()
		if n == 1 {
			return labelsResult([]driver.Value{uint64(7), map[string]string{"job": "a"}})()
		}
		return labelsResult([]driver.Value{uint64(7), map[string]string{"job": "a"}},
			[]driver.Value{uint64(8), map[string]string{"job": "b"}})()
	})
	got := selectSeries(t, db, rateSubstitute(t), substituteHints, substituteMatcher("__metric_subst__1"))
	if len(got) != 2 || labelReads != 2 {
		t.Fatalf("got %v after %d label reads, want two series after two", got, labelReads)
	}
}

func TestCloseCancelsTheReadsOfASetNeverRead(t *testing.T) {
	issued, cancelled := make(chan struct{}, 2), make(chan struct{}, 2)
	db := fakeclickhouse.NewWithContext(func(ctx context.Context, query string) (fakeclickhouse.Result, error) {
		issued <- struct{}{}
		select {
		case <-ctx.Done():
			cancelled <- struct{}{}
			return fakeclickhouse.Result{}, ctx.Err()
		case <-time.After(5 * time.Second):
			return fakeclickhouse.Result{}, errors.New("not cancelled")
		}
	})
	querier := querierOf(t, db, rateSubstitute(t))
	querier.Select(context.Background(), true, substituteHints, substituteMatcher("__metric_subst__1"))
	<-issued
	if err := querier.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("Close left the read running")
	}
}
