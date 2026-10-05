package service

import (
	"bytes"
	"cmp"
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/utils/cityhash102"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/shared/metricretention"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"
)

type StatsStore struct {
	Starts  map[string]time.Time
	Ends    map[string]time.Time
	Counter int32
	Mtx     sync.Mutex
}

func NewStatsStore() *StatsStore {
	return &StatsStore{
		Starts:  make(map[string]time.Time),
		Ends:    make(map[string]time.Time),
		Mtx:     sync.Mutex{},
		Counter: 1,
	}
}

func (s *StatsStore) StartTiming(key string) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.Starts[key] = time.Now()
}

func (s *StatsStore) EndTiming(key string) {
	s.Mtx.Lock()
	defer s.Mtx.Unlock()
	s.Ends[key] = time.Now()
}

func (s *StatsStore) Id() int32 {
	return atomic.AddInt32(&s.Counter, 1)
}

func (s *StatsStore) AsMap() map[string]float64 {
	res := make(map[string]float64)
	for k, start := range s.Starts {
		end := time.Now()
		if _, ok := s.Ends[k]; ok {
			end = s.Ends[k]
		}
		dist := end.Sub(start)
		res[k] = dist.Seconds()
	}
	return res
}

type CLokiQueriable struct {
	model.ServiceData
	Ctx   context.Context
	Stats *StatsStore
	Expr  *promql_parser.Expr
	// Tiers routes each query to one tier; without it every read is raw.
	Tiers *TierRouting
}

// TierRouting holds what tier selection reads besides the query: the tier lifetimes and the
// METRICS_READ_TIER knob.
type TierRouting struct {
	Lifetimes metricretention.Tiers
	Forced    string
}

func (c *CLokiQueriable) Querier(mint, maxt int64) (storage.Querier, error) {
	db, err := c.ServiceData.Session.GetDB(c.Ctx)
	if err != nil {
		return nil, err
	}
	res := &CLokiQuerier{
		db:   db,
		expr: c.Expr,
		tier: c.tier(),
	}
	if p := plugins.GetMetricLabelsGetterPlugin(); p != nil {
		res.labelsPlugin = *p
	}
	return res, nil
}

// tier is the one tier the query is served from. A query that was not transpiled reads raw
// unless a tier is forced.
func (c *CLokiQueriable) tier() metricread.Tier {
	if c.Tiers == nil {
		return metricread.RawTier
	}
	if c.Expr == nil {
		if t, ok := metricread.TierNamed(c.Tiers.Forced); ok {
			return t
		}
		return metricread.RawTier
	}
	return metricread.SelectTier(c.Expr.Read, c.Tiers.Lifetimes, time.Now(), c.Tiers.Forced)
}

func (c *CLokiQueriable) SetOidAndDB(ctx context.Context, expr *promql_parser.Expr) *CLokiQueriable {
	return &CLokiQueriable{
		ServiceData: c.ServiceData,
		Ctx:         ctx,
		Expr:        expr,
		Tiers:       c.Tiers,
	}
}

type CLokiQuerier struct {
	db   *model.DataDatabasesMap
	expr *promql_parser.Expr
	tier metricread.Tier
	// labelsPlugin, when set, selects the series labels in place of the series index.
	labelsPlugin plugins.MetricLabelsGetterPlugin

	mtx     sync.Mutex
	cancels []context.CancelFunc
}

// appendStaleMarker ends each run of a substitute series' points, taken one step apart, with a
// stale marker one step past its last point, unless that is past the query end or the step is
// unknown (0).
func appendStaleMarker(samples []model.Sample, stepMs int64, queryEndMs int64) []model.Sample {
	if len(samples) == 0 || stepMs <= 0 {
		return samples
	}
	res := make([]model.Sample, 0, len(samples)+1)
	for i, s := range samples {
		res = append(res, s)
		markerTs := s.TimestampMs + stepMs
		if markerTs > queryEndMs || i+1 < len(samples) && samples[i+1].TimestampMs <= markerTs {
			continue
		}
		res = append(res, model.Sample{TimestampMs: markerTs, Value: model.StaleMarkerValue})
	}
	return res
}

// Select starts the selector's reads and returns at once; the set's first Next or Err waits
// for them, so the engine's selectors read concurrently.
func (c *CLokiQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.SeriesSet {

	var _matchers []*labels.Matcher
	for _, m := range matchers {
		if m.Name == "__ignore_usage__" && m.Type == labels.MatchEqual && m.Value == "" {
			continue
		}
		_matchers = append(_matchers, m)
	}
	matchers = _matchers

	ctx, cancel := context.WithCancel(ctx)
	c.mtx.Lock()
	c.cancels = append(c.cancels, cancel)
	c.mtx.Unlock()
	set := &pendingSeriesSet{done: make(chan struct{})}
	go func() {
		defer close(set.done)
		var (
			series []*model.SeriesV2
			err    error
		)
		if sub := c.substitute(matchers); sub != nil {
			series, err = c.selectSubstitute(ctx, sub)
		} else {
			series, err = c.selectRaw(ctx, hints, matchers)
		}
		if err != nil {
			set.set = model.SeriesSet{Error: err}
			return
		}
		set.set = model.SeriesSet{Series: c.sortSeries(c.ReshuffleSeries(series))}
		set.set.Reset()
	}()
	return set
}

// pendingSeriesSet is a series set whose reads are in flight until done closes.
type pendingSeriesSet struct {
	done chan struct{}
	set  model.SeriesSet
}

func (s *pendingSeriesSet) Next() bool {
	<-s.done
	return s.set.Next()
}

func (s *pendingSeriesSet) At() storage.Series { return s.set.At() }

func (s *pendingSeriesSet) Err() error {
	<-s.done
	return s.set.Err()
}

func (s *pendingSeriesSet) Warnings() annotations.Annotations { return nil }

// substitute returns the substitute a matcher names, if any.
func (c *CLokiQuerier) substitute(matchers []*labels.Matcher) *promql_parser.Substitute {
	if c.expr == nil {
		return nil
	}
	for _, m := range matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			if sub, ok := c.expr.Substitutes[m.Value]; ok {
				return sub
			}
		}
	}
	return nil
}

// selectRaw reads the selected series from the series index and hands the engine their samples
// in the hinted interval [Start, End]: raw samples, or from a tier each bucket's last sample
// and the stale marker that follows it.
func (c *CLokiQuerier) selectRaw(ctx context.Context, hints *storage.SelectHints, matchers []*labels.Matcher) ([]*model.SeriesV2, error) {
	window := metricread.Window{FromMs: hints.Start - 1, ToMs: hints.End, Cluster: c.cluster(),
		Series: c.seriesSource(ctx)}
	lbls, err := c.readSeries(ctx, metricread.SeriesSQL(window, matchers))
	if err != nil || len(lbls) == 0 {
		return nil, err
	}
	samples := metricread.RawSamplesSQL(window, matchers)
	if c.tier.WidthMs > 0 {
		samples = metricread.TierSamplesSQL(window, c.tier, matchers)
	}
	rows, err := c.query(ctx, samples)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		series []*model.SeriesV2
		fp     uint64
		ts     time.Time
		val    float64
	)
	for rows.Next() {
		if err := rows.Scan(&fp, &ts, &val); err != nil {
			return nil, err
		}
		if _, ok := lbls[fp]; ok {
			series = appendSample(series, lbls, fp, ts.UnixMilli(), val)
		}
	}
	return series, rows.Err()
}

// selectSubstitute evaluates a substitute's pushdown at the query's own timestamps, from the
// query's tier, and names each of its series from one label row per fingerprint. The points and
// labels are read concurrently with one predicate and window; a series indexed between the two
// reads gets its labels from one more labels read.
func (c *CLokiQuerier) selectSubstitute(ctx context.Context, sub *promql_parser.Substitute) ([]*model.SeriesV2, error) {
	pushdown := sub.Pushdown
	pushdown.Tier = c.tier
	pushdown.Cluster = c.cluster()
	pushdown.Series = c.seriesSource(ctx)
	labelsSQL := metricread.PushdownLabelsSQL(pushdown)
	var (
		series []*model.SeriesV2
		lbls   seriesLabels
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		series, err = c.readPoints(gctx, metricread.PushdownSQL(pushdown))
		if err == nil && len(series) == 0 {
			return errNoPoints
		}
		return err
	})
	g.Go(func() error {
		var err error
		lbls, err = c.readSeries(gctx, labelsSQL)
		return err
	})
	if err := g.Wait(); err != nil {
		if errors.Is(err, errNoPoints) {
			return nil, nil
		}
		return nil, err
	}
	if slices.ContainsFunc(series, func(s *model.SeriesV2) bool { _, ok := lbls[s.Fp]; return !ok }) {
		var err error
		if lbls, err = c.readSeries(ctx, labelsSQL); err != nil {
			return nil, err
		}
	}
	for _, s := range series {
		if _, ok := lbls[s.Fp]; !ok {
			return nil, fmt.Errorf("pushdown series %d has no labels", s.Fp)
		}
		s.LabelsGetter = lbls
	}
	if err := uniqueLabelSets(series); err != nil {
		return nil, err
	}
	grid := sub.Pushdown.Grid
	for _, s := range series {
		s.Samples = appendStaleMarker(s.Samples, grid.StepMs, grid.EndMs)
	}
	return series, nil
}

// errNoPoints ends a substitute's labels read when the pushdown returns no points.
var errNoPoints = errors.New("no points")

// readPoints returns the series of the (fingerprint, t_ms, value) rows of query, unnamed.
func (c *CLokiQuerier) readPoints(ctx context.Context, query string) ([]*model.SeriesV2, error) {
	rows, err := c.query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		series []*model.SeriesV2
		fp     uint64
		tMs    int64
		val    float64
	)
	for rows.Next() {
		if err := rows.Scan(&fp, &tMs, &val); err != nil {
			return nil, err
		}
		series = appendSample(series, nil, fp, tMs, val)
	}
	return series, rows.Err()
}

// errSameLabelset is the engine's error for a function result holding two series with one
// label set.
var errSameLabelset = errors.New("vector cannot contain metrics with the same labelset")

// uniqueLabelSets fails when two series share a label set, which the engine rejects in a range
// function's result whatever their timestamps.
func uniqueLabelSets(series []*model.SeriesV2) error {
	seen := make(map[string]struct{}, len(series))
	for _, s := range series {
		key := labels.New(s.LabelsGetter.Get(s.Fp)...).String()
		if _, ok := seen[key]; ok {
			return errSameLabelset
		}
		seen[key] = struct{}{}
	}
	return nil
}

// readSeries returns the label set of every fingerprint the (fingerprint, labels) query selects.
func (c *CLokiQuerier) readSeries(ctx context.Context, query string) (seriesLabels, error) {
	rows, err := c.query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := seriesLabels{}
	var (
		fp  uint64
		set map[string]string
	)
	for rows.Next() {
		if err := rows.Scan(&fp, &set); err != nil {
			return nil, err
		}
		res[fp] = labelsFromMap(set)
	}
	return res, rows.Err()
}

// seriesSource is the labels plugin's series query, or nil to read the series index.
func (c *CLokiQuerier) seriesSource(ctx context.Context) metricread.SeriesSource {
	if c.labelsPlugin == nil {
		return nil
	}
	return func(w metricread.Window, matchers []*labels.Matcher) string {
		return c.labelsPlugin.GetMetricLabelsQuery(ctx, c.db, matchers,
			time.UnixMilli(w.FromMs), time.UnixMilli(w.ToMs))
	}
}

// cluster reports whether the database runs on a cluster, where reads go to the distributed tables.
func (c *CLokiQuerier) cluster() bool {
	return c.db.Config != nil && c.db.Config.ClusterName != ""
}

func (c *CLokiQuerier) query(ctx context.Context, query string) (*gosql.Rows, error) {
	logger.Debug("[ PromQuerier ] ", query)
	return c.db.Session.QueryCtx(ctx, query)
}

// appendSample adds a sample to the series of fp, starting a new series when fp changes.
// Rows arrive ordered by fingerprint and timestamp; a repeated instant keeps its first row.
func appendSample(series []*model.SeriesV2, lbls seriesLabels, fp uint64, ts int64, val float64) []*model.SeriesV2 {
	if len(series) == 0 || series[len(series)-1].Fp != fp {
		series = append(series, &model.SeriesV2{LabelsGetter: lbls, Fp: fp})
	}
	last := series[len(series)-1]
	if n := len(last.Samples); n > 0 && last.Samples[n-1].TimestampMs == ts {
		return series
	}
	last.Samples = append(last.Samples, model.Sample{TimestampMs: ts, Value: val})
	return series
}

// sortSeries orders series by label set, as the engine expects.
func (c *CLokiQuerier) sortSeries(series []*model.SeriesV2) []*model.SeriesV2 {
	slices.SortFunc(series, func(a, b *model.SeriesV2) int {
		return labels.Compare(a.Labels(), b.Labels())
	})
	return series
}

// ReshuffleSeries merges series that resolve to the same label set (this can
// happen when a single logical series is split across interleaved ClickHouse
// blocks and ends up in more than one *model.SeriesV2 entry). The duplicate's
// samples are merged into the first entry and the duplicate itself must be
// dropped from the returned slice - the prometheus engine errors out with
// "vector cannot contain metrics with the same labelset" if two series with
// identical labels are both kept.
func (c *CLokiQuerier) ReshuffleSeries(series []*model.SeriesV2) []*model.SeriesV2 {
	seriesMap := make(map[uint64]*model.SeriesV2, len(series)*2)
	out := series[:0]
	for _, ent := range series {
		lbls := ent.LabelsGetter.Get(ent.Fp)
		strLabels := make([][]byte, len(lbls))
		for i, lbl := range lbls {
			strLabels[i] = []byte(lbl.Name + "=" + lbl.Value)
		}
		str := bytes.Join(strLabels, []byte(" "))
		_fp := cityhash102.CityHash64(str, uint32(len(str)))
		if chunk, ok := seriesMap[_fp]; ok {
			logger.Error(fmt.Sprintf("Warning: double labels set found [%d - %d]: %s",
				chunk.Fp, ent.Fp, string(str)))
			chunk.Samples = append(chunk.Samples, ent.Samples...)
			slices.SortFunc(chunk.Samples, func(a, b model.Sample) int {
				return cmp.Compare(a.TimestampMs, b.TimestampMs)
			})
			// duplicate merged into chunk - drop it from the output
		} else {
			seriesMap[_fp] = ent
			out = append(out, ent)
		}
	}
	return out
}

func (c *CLokiQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (c *CLokiQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

// Close stops the reads of every set Select returned.
func (c *CLokiQuerier) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, cancel := range c.cancels {
		cancel()
	}
	c.cancels = nil
	return nil
}

// seriesLabels maps a fingerprint to its label set.
type seriesLabels map[uint64]model.Labels

func (s seriesLabels) Get(fp uint64) model.Labels { return s[fp] }

func (s seriesLabels) GetNative(fp uint64) labels.Labels { return labels.New(s[fp]...) }

func labelsFromMap(m map[string]string) model.Labels {
	res := make(model.Labels, 0, len(m))
	for k, v := range m {
		res = append(res, labels.Label{Name: k, Value: v})
	}
	slices.SortFunc(res, func(a, b labels.Label) int { return cmp.Compare(a.Name, b.Name) })
	return res
}
