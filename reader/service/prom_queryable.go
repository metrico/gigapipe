package service

import (
	"bytes"
	"cmp"
	"context"
	gosql "database/sql"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/utils/cityhash102"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
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
}

func (c *CLokiQueriable) Querier(mint, maxt int64) (storage.Querier, error) {
	db, err := c.ServiceData.Session.GetDB(c.Ctx)
	if err != nil {
		return nil, err
	}
	return &CLokiQuerier{
		db:   db,
		expr: c.Expr,
	}, nil
}

func (c *CLokiQueriable) SetOidAndDB(ctx context.Context, expr *promql_parser.Expr) *CLokiQueriable {
	return &CLokiQueriable{
		ServiceData: c.ServiceData,
		Ctx:         ctx,
		Expr:        expr,
	}
}

type CLokiQuerier struct {
	db   *model.DataDatabasesMap
	expr *promql_parser.Expr
}

// appendStaleMarker caps a substitute series with a stale marker one step past its last
// point, unless the series reaches the query end or the step is unknown (0).
func appendStaleMarker(samples []model.Sample, stepMs int64, queryEndMs int64) []model.Sample {
	if len(samples) == 0 || stepMs <= 0 {
		return samples
	}
	markerTs := samples[len(samples)-1].TimestampMs + stepMs
	if markerTs > queryEndMs {
		return samples
	}
	return append(samples, model.Sample{TimestampMs: markerTs, Value: model.StaleMarkerValue})
}

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

	var (
		series []*model.SeriesV2
		err    error
	)
	if sub := c.substitute(matchers); sub != nil {
		series, err = c.selectSubstitute(ctx, sub, hints)
	} else {
		series, err = c.selectRaw(ctx, hints, matchers)
	}
	if err != nil {
		return &model.SeriesSet{Error: err}
	}
	res := model.SeriesSet{Series: c.sortSeries(c.ReshuffleSeries(series))}
	res.Reset()
	return &res
}

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

// selectRaw reads the selected series from the series index and hands the engine their raw
// samples in the hinted interval [Start, End].
func (c *CLokiQuerier) selectRaw(ctx context.Context, hints *storage.SelectHints, matchers []*labels.Matcher) ([]*model.SeriesV2, error) {
	window := metricread.Window{FromMs: hints.Start - 1, ToMs: hints.End}
	lbls, err := c.readSeries(ctx, metricread.SeriesSQL(window, matchers))
	if err != nil || len(lbls) == 0 {
		return nil, err
	}
	rows, err := c.query(ctx, metricread.RawSamplesSQL(window, matchers))
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

// selectSubstitute runs a substitute's request, whose rows are
// (fingerprint UInt64, label_set Map(String, String), timestamp DateTime64(3), value Float64)
// ordered by fingerprint and timestamp.
func (c *CLokiQuerier) selectSubstitute(ctx context.Context, sub *promql_parser.Substitute, hints *storage.SelectHints) ([]*model.SeriesV2, error) {
	plannerCtx := shared.PlannerContext{
		IsCluster: c.db.Config.ClusterName != "",
		From:      time.UnixMilli(hints.Start),
		To:        time.UnixMilli(hints.End),
		Step:      time.Duration(hints.Step) * time.Millisecond,
		Ctx:       ctx,
		CHDb:      c.db.Session,
	}
	req, err := sub.Request.Process(&plannerCtx)
	if err != nil {
		return nil, err
	}
	var opts []int
	if plannerCtx.IsCluster {
		opts = []int{sql.STRING_OPT_INLINE_WITH}
	}
	str, err := req.String(&sql.Ctx{Params: map[string]sql.SQLObject{}}, opts...)
	if err != nil {
		return nil, err
	}
	rows, err := c.query(ctx, str)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		series []*model.SeriesV2
		lbls   = seriesLabels{}
		fp     uint64
		set    map[string]string
		ts     time.Time
		val    float64
	)
	for rows.Next() {
		if err := rows.Scan(&fp, &set, &ts, &val); err != nil {
			return nil, err
		}
		if _, ok := lbls[fp]; !ok {
			lbls[fp] = labelsFromMap(set)
		}
		series = appendSample(series, lbls, fp, ts.UnixMilli(), val)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, s := range series {
		s.Samples = appendStaleMarker(s.Samples, hints.Step, hints.End)
	}
	return series, nil
}

// readSeries returns the label set of every fingerprint the series query selects.
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

// Close releases the resources of the Querier.
func (c *CLokiQuerier) Close() error {
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
