package service

import (
	"bytes"
	"context"
	"fmt"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/plugins"
	"github.com/metrico/qryn/reader/utils/cityhash102"
	"github.com/metrico/qryn/reader/utils/dbVersion"
	"github.com/metrico/qryn/reader/utils/logger"
	"github.com/metrico/qryn/reader/utils/tables"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metrico/qryn/reader/promql/transpiler"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

var cache = fastcache.New(100 * 1024 * 1024)

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
	random *rand.Rand
	Ctx    context.Context
	Stats  *StatsStore
}

func (c *CLokiQueriable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if c.random == nil {
		c.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	db, err := c.ServiceData.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	return &CLokiQuerier{
		db:  db,
		ctx: c.Ctx,
	}, nil
}

func (c *CLokiQueriable) SetOidAndDB(ctx context.Context) *CLokiQueriable {
	return &CLokiQueriable{
		ServiceData: c.ServiceData,
		random:      c.random,
		Ctx:         ctx,
	}
}

type CLokiQuerier struct {
	db  *model.DataDatabasesMap
	ctx context.Context
}

var supportedFunctions = map[string]bool{
	// Over time
	"avg_over_time":      true,
	"min_over_time":      true,
	"max_over_time":      true,
	"sum_over_time":      true,
	"count_over_time":    true,
	"quantile_over_time": false,
	"stddev_over_time":   false,
	"stdvar_over_time":   false,
	"last_over_time":     true,
	"present_over_time":  true,
	"absent_over_time":   true,
	//instant
	"":    true,
	"abs": true, "absent": true, "ceil": true, "exp": true, "floor": true,
	"ln": true, "log2": true, "log10": true, "round": true, "scalar": true,
	"sgn": true, "sort": true, "sqrt": true, "timestamp": true, "atan": true,
	"cos": true, "cosh": true, "sin": true, "sinh": true, "tan": true,
	"tanh": true, "deg": true, "rad": true,
	//agg
	"sum":   true,
	"min":   true,
	"max":   true,
	"group": true,
	"avg":   true,
}

func (c *CLokiQuerier) transpileLabelMatchers(hints *storage.SelectHints,
	matchers []*labels.Matcher, versionInfo dbVersion.VersionInfo) (*transpiler.TranspileResponse, error) {
	isSupported, ok := supportedFunctions[hints.Func]

	c.adjustHintsForRate(hints)

	useRawData := hints.Start%15000 != 0 ||
		hints.Step < 15000 ||
		(hints.Range > 0 && hints.Range < 15000) ||
		!(isSupported || !ok)

	start := hints.Start - hints.Range

	ctx := shared.PlannerContext{
		IsCluster:   c.db.Config.ClusterName != "",
		From:        time.Unix(0, start*1000000),
		To:          time.Unix(0, hints.End*1000000),
		Ctx:         c.ctx,
		CHDb:        c.db.Session,
		CancelCtx:   nil,
		Step:        time.Millisecond * time.Duration(hints.Step),
		Type:        2,
		VersionInfo: versionInfo,
	}
	tables.PopulateTableNames(&ctx, c.db)
	if useRawData {
		return transpiler.TranspileLabelMatchers(hints, &ctx, matchers...)
	}
	return transpiler.TranspileLabelMatchersDownsample(hints, &ctx, matchers...)
}

var rateFunctions = []string{"deriv", "rate", "delta"}

func (c *CLokiQuerier) adjustHintsForRate(hints *storage.SelectHints) {
	step := hints.Step
	if slices.Contains(rateFunctions, hints.Func) && hints.Step > (hints.Range/2) || hints.Step == 0 {
		step = max(hints.Range/2, 15000)
	}
	hints.Step = step
}

func (c *CLokiQuerier) isProlong(hints *storage.SelectHints) bool {
	return (slices.Contains(rateFunctions, hints.Func) || hints.Func == "") && hints.Step != 0
}

func (c *CLokiQuerier) Select(sortSeries bool, hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.SeriesSet {

	var _matchers []*labels.Matcher
	for _, m := range matchers {
		if m.Name == "__ignore_usage__" && m.Type == labels.MatchEqual && m.Value == "" {
			continue
		}
		_matchers = append(_matchers, m)
	}
	matchers = _matchers

	versionInfo, err := dbVersion.GetVersionInfo(c.ctx, c.db.Config.ClusterName != "", c.db.Session)
	if err != nil {
		return &model.SeriesSet{Error: err}
	}

	q, err := c.transpileLabelMatchers(hints, matchers, versionInfo)
	if err != nil {
		return &model.SeriesSet{Error: err}
	}
	ctx := sql.Ctx{
		Params: map[string]sql.SQLObject{},
	}
	var opts []int
	if c.db.Config.ClusterName != "" {
		opts = []int{sql.STRING_OPT_INLINE_WITH}
	}
	str, err := q.Query.String(&ctx, opts...)
	if err != nil {
		return &model.SeriesSet{Error: err}
	}
	rows, err := c.db.Session.QueryCtx(c.ctx, str)
	if err != nil {
		fmt.Println(str)
		return &model.SeriesSet{Error: err}
	}
	var (
		fp         uint64  = 0
		val        float64 = 0
		ts         int64   = 0
		lastLabels uint64  = 0
	)
	res := model.SeriesSet{
		Error:  nil,
		Series: make([]*model.SeriesV2, 0, 1000),
	}
	res.Reset()
	cntRows := 0
	cntSeries := 0
	lblsGetter := newLabelsGetter(time.UnixMilli(hints.Start), time.UnixMilli(hints.End), c.db, c.ctx)
	isProlong := c.isProlong(hints)
	for rows.Next() {
		err = rows.Scan(&fp, &val, &ts)
		if err != nil {
			return &model.SeriesSet{Error: err}
		}
		if len(res.Series) == 0 || fp != lastLabels {
			lblsGetter.Plan(fp)
			lastLabels = fp
			if len(res.Series) > 0 && q.MapResult != nil {
				res.Series[len(res.Series)-1].Samples = q.MapResult(res.Series[len(res.Series)-1].Samples)
			}
			res.Series = append(res.Series, &model.SeriesV2{
				LabelsGetter: lblsGetter,
				Fp:           fp,
				Samples:      make([]model.Sample, 0, 500),
				StepMs:       hints.Step,
				Prolong:      isProlong,
			})
			cntSeries++
		}
		res.Series[len(res.Series)-1].Samples = append(res.Series[len(res.Series)-1].Samples,
			model.Sample{ts, val})
		cntRows++
	}
	if len(res.Series) > 0 && q.MapResult != nil {
		res.Series[len(res.Series)-1].Samples = q.MapResult(res.Series[len(res.Series)-1].Samples)
	}
	err = lblsGetter.Fetch()
	if err != nil {
		return &model.SeriesSet{Error: err}
	}
	c.ReshuffleSeries(res.Series)
	sort.Slice(res.Series, func(i, j int) bool {
		for k, l1 := range res.Series[i].Labels() {
			l2 := res.Series[j].Labels()
			if k >= len(l2) {
				return false
			}
			if l1.Name != l2[k].Name {
				return l1.Name < l2[k].Name
			}
			if l1.Value != l2[k].Value {
				return l1.Value < l2[k].Value
			}
		}
		return true
	})
	return &res
}

func (c *CLokiQuerier) ReshuffleSeries(series []*model.SeriesV2) {
	seriesMap := make(map[uint64]*model.SeriesV2, len(series)*2)
	for _, ent := range series {
		labels := ent.LabelsGetter.Get(ent.Fp)
		strLabels := make([][]byte, labels.Len())
		for i, lbl := range labels {
			strLabels[i] = []byte(lbl.Name + "=" + lbl.Value)
		}
		str := bytes.Join(strLabels, []byte(" "))
		_fp := cityhash102.CityHash64(str, uint32(len(str)))
		if chunk, ok := seriesMap[_fp]; ok {
			logger.Error(fmt.Printf("Warning: double labels set found [%d - %d]: %s",
				chunk.Fp, ent.Fp, string(str)))
			chunk.Samples = append(chunk.Samples, ent.Samples...)
			sort.Slice(chunk.Samples, func(i, j int) bool {
				return chunk.Samples[i].TimestampMs < chunk.Samples[j].TimestampMs
			})

		} else {
			seriesMap[_fp] = ent
		}
	}
}

func (c *CLokiQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (c *CLokiQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

// Close releases the resources of the Querier.
func (c *CLokiQuerier) Close() error {
	return nil
}

type labelsGetter struct {
	DateFrom           time.Time
	DateTo             time.Time
	Conn               *model.DataDatabasesMap
	Ctx                context.Context
	fingerprintsHas    map[uint64][][]string
	fingerprintToFetch map[uint64]bool
	Distributed        bool
	plugin             plugins.LabelsGetterPlugin
}

func newLabelsGetter(from time.Time, to time.Time, conn *model.DataDatabasesMap, ctx context.Context) *labelsGetter {
	res := &labelsGetter{
		DateFrom:           from,
		DateTo:             to,
		Conn:               conn,
		Ctx:                ctx,
		Distributed:        conn.Config.ClusterName != "",
		fingerprintsHas:    make(map[uint64][][]string),
		fingerprintToFetch: make(map[uint64]bool),
	}
	p := plugins.GetLabelsGetterPlugin()
	if p != nil {
		res.plugin = *p
	}
	return res
}

func (l *labelsGetter) Get(fingerprint uint64) labels.Labels {
	strLabels, ok := l.fingerprintsHas[fingerprint]
	if !ok {
		logger.Error(fmt.Sprintf("Warning: no fingerprint %d found", fingerprint))
		return labels.Labels{}
	}
	res := make(labels.Labels, len(strLabels))
	for i, label := range strLabels {
		res[i] = labels.Label{
			Name:  label[0],
			Value: label[1],
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}

func (l *labelsGetter) Plan(fingerprint uint64) {
	l.fingerprintToFetch[fingerprint] = true
}

func (l *labelsGetter) getFetchRequest(fingerprints map[uint64]bool) sql.ISelect {
	if l.plugin != nil {
		return l.plugin.GetLabelsQuery(l.Ctx, l.Conn, fingerprints, l.DateFrom, l.DateTo)
	}
	tableName := tables.GetTableName("time_series")
	if l.Distributed {
		tableName = tables.GetTableName("time_series_dist")
	}
	fps := make([]sql.SQLObject, 0, len(fingerprints))
	for fp, _ := range l.fingerprintToFetch {
		fps = append(fps, sql.NewRawObject(strconv.FormatUint(fp, 10)))
	}
	req := sql.NewSelect().
		Select(sql.NewRawObject("fingerprint"), sql.NewSimpleCol("JSONExtractKeysAndValues(labels, 'String')", "labels")).
		From(sql.NewRawObject(tableName)).
		AndWhere(
			sql.NewIn(sql.NewRawObject("fingerprint"), fps...),
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(FormatFromDate(l.DateFrom))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(l.DateTo.Format("2006-01-02"))))
	return req
}

func (l *labelsGetter) Fetch() error {
	if len(l.fingerprintToFetch) == 0 {
		return nil
	}
	req := l.getFetchRequest(l.fingerprintToFetch)
	strReq, err := req.String(&sql.Ctx{})
	if err != nil {
		return err
	}
	rows, err := l.Conn.Session.QueryCtx(l.Ctx, strReq)
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			fingerprint uint64
			labels      [][]interface{}
		)
		err := rows.Scan(&fingerprint, &labels)
		if err != nil {
			return err
		}
		strLabels := make([][]string, len(labels))
		for i, label := range labels {
			strLabels[i] = []string{label[0].(string), label[1].(string)}
		}
		sort.Slice(strLabels, func(i, j int) bool {
			return strings.Compare(strLabels[i][0], strLabels[j][0]) < 0
		})
		l.fingerprintsHas[fingerprint] = strLabels
		//cache.Set(l.getIdx(fingerprint), bLabels)
	}
	return nil
}
