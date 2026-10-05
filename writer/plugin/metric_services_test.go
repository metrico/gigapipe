package plugin

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/metrico/qryn/v5/writer/chwrapper"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
)

// insertLog is a ClickHouse stand-in that records the table of every insert it receives.
type insertLog struct {
	chwrapper.IChClient
	mtx    sync.Mutex
	tables []string
	sent   chan struct{}
}

func (l *insertLog) Do(_ context.Context, q ch.Query) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.tables = append(l.tables, strings.Fields(q.Body)[2])
	l.sent <- struct{}{}
	return nil
}

func (l *insertLog) Ping(context.Context) error { return nil }
func (l *insertLog) Close() error               { return nil }

func (l *insertLog) inserted() []string {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return append([]string{}, l.tables...)
}

// await waits for n inserts, failing the test after a second without one.
func (l *insertLog) await(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-l.sent:
		case <-time.After(time.Second):
			t.Fatalf("waited for %d inserts, got %v", n, l.inserted())
		}
	}
}

func startMetricServices(t *testing.T, ch *insertLog) metricServices {
	t.Helper()
	service.CreateColPools(0)
	svcs := newMetricServices(model.InsertServiceOpts{
		Session:      func() (chwrapper.IChClient, error) { return ch, nil },
		Node:         &model.DataDatabasesMap{},
		Interval:     time.Hour,
		ParallelNum:  1,
		MaxQueueSize: 10,
	})
	for _, svc := range []service.IInsertServiceV2{svcs.staging, svcs.series, svcs.metadata, svcs.exemplars} {
		svc.Init()
		go svc.Run()
		t.Cleanup(svc.Stop)
	}
	return svcs
}

func newSeries(fp uint64) *model.MetricSeriesData {
	return &model.MetricSeriesData{
		MName: []string{"up"}, MFingerprint: []uint64{fp},
		MLabels:      []map[string]string{{"__name__": "up"}},
		MFirstSeenMs: []int64{1000}, MLastSeenMs: []int64{1000}, Size: 1,
	}
}

// A request carrying a new series fills the staging service past its queue size while its
// series row stays below it: the staging insert has the series service flush its row too.
func TestMetricStagingInsertFlushesTheSeriesService(t *testing.T) {
	ch := &insertLog{sent: make(chan struct{}, 8)}
	svcs := startMetricServices(t, ch)
	svcs.series.Request(newSeries(7), service.INSERT_MODE_SYNC)
	svcs.staging.Request(&model.MetricSamplesData{
		MFingerprint: []uint64{7}, MTimestampMs: []int64{1000}, MValue: []float64{1},
		MPrevTimestampMs: []int64{0}, MPrevValue: []float64{0}, MAggregate: []uint8{1}, Size: 100,
	}, service.INSERT_MODE_SYNC)
	ch.await(t, 2)
	if got := ch.inserted(); !(slices.Contains(got, "metric_series") && slices.Contains(got, "metric_samples_in")) {
		t.Fatalf("inserts: got %v, want metric_series and metric_samples_in", got)
	}
}

// An exemplar names its series through the series index, so its insert flushes the series row too.
func TestMetricExemplarInsertFlushesTheSeriesService(t *testing.T) {
	ch := &insertLog{sent: make(chan struct{}, 8)}
	svcs := startMetricServices(t, ch)
	svcs.series.Request(newSeries(7), service.INSERT_MODE_SYNC)
	svcs.exemplars.Request(&model.MetricExemplarsData{
		MFingerprint: []uint64{7}, MTimestampMs: []int64{1000}, MValue: []float64{1},
		MTraceID: []string{"t"}, MLabels: []string{"{}"}, Size: 100,
	}, service.INSERT_MODE_SYNC)
	ch.await(t, 2)
	if got := ch.inserted(); !(slices.Contains(got, "metric_series") && slices.Contains(got, "metric_exemplars")) {
		t.Fatalf("inserts: got %v, want metric_series and metric_exemplars", got)
	}
}
