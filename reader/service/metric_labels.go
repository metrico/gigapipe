package service

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/utils/logger"
)

// MetricLabelsService answers the Prometheus label, series, metadata and exemplar endpoints
// from the metric series index.
type MetricLabelsService struct {
	model.ServiceData
}

func NewMetricLabelsService(sd *model.ServiceData) *MetricLabelsService {
	return &MetricLabelsService{ServiceData: *sd}
}

// LabelNames returns the label names of the series q picks, and whether q.Limit cut them.
func (s *MetricLabelsService) LabelNames(ctx context.Context, q metricread.IndexQuery) ([]string, bool, error) {
	return s.strings(ctx, q, metricread.LabelNamesSQL)
}

// LabelValues returns the values of label name over the series q picks, and whether q.Limit
// cut them.
func (s *MetricLabelsService) LabelValues(ctx context.Context, name string,
	q metricread.IndexQuery) ([]string, bool, error) {
	return s.strings(ctx, q, func(q metricread.IndexQuery) string { return metricread.LabelValuesSQL(name, q) })
}

// Series returns the label set of every series q picks, and whether q.Limit cut them.
func (s *MetricLabelsService) Series(ctx context.Context, q metricread.IndexQuery) ([]map[string]string, bool, error) {
	rows, q, err := s.query(ctx, q, metricread.SeriesListSQL)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	res := []map[string]string{}
	for rows.Next() {
		var set map[string]string
		if err := rows.Scan(&set); err != nil {
			return nil, false, err
		}
		res = append(res, set)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	res, truncated := truncate(res, q.Limit)
	return res, truncated, nil
}

// MetricMetadata is a metric family's metadata entry.
type MetricMetadata struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

// Metadata returns the latest metadata of every family, or of metric alone when set, keyed
// by family, up to limit families; a negative limit reads every family.
func (s *MetricLabelsService) Metadata(ctx context.Context, metric string, limit int) (map[string][]MetricMetadata, error) {
	rows, _, err := s.query(ctx, metricread.IndexQuery{}, func(q metricread.IndexQuery) string {
		return metricread.MetadataSQL(metric, limit, q.Cluster)
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string][]MetricMetadata{}
	for rows.Next() {
		var name string
		var m MetricMetadata
		if err := rows.Scan(&name, &m.Type, &m.Help, &m.Unit); err != nil {
			return nil, err
		}
		res[name] = []MetricMetadata{m}
	}
	return res, rows.Err()
}

// ExemplarSeries is a series' label set and its exemplars, oldest first.
type ExemplarSeries struct {
	SeriesLabels map[string]string
	Exemplars    []Exemplar
}

type Exemplar struct {
	Labels      map[string]string
	Value       float64
	TimestampMs int64
}

// Exemplars returns the exemplars in [q.StartMs, q.EndMs] of the series q picks, per series.
func (s *MetricLabelsService) Exemplars(ctx context.Context, q metricread.IndexQuery) ([]ExemplarSeries, error) {
	rows, _, err := s.query(ctx, q, metricread.ExemplarsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := []ExemplarSeries{}
	var lastFp uint64
	for rows.Next() {
		var (
			fp     uint64
			series map[string]string
			ts     time.Time
			e      Exemplar
			lbls   string
		)
		if err := rows.Scan(&fp, &series, &ts, &e.Value, &lbls); err != nil {
			return nil, err
		}
		e.TimestampMs = ts.UnixMilli()
		e.Labels = map[string]string{}
		if lbls != "" {
			if err := json.Unmarshal([]byte(lbls), &e.Labels); err != nil {
				logger.Error("exemplar labels ", lbls, ": ", err)
			}
		}
		if len(res) == 0 || fp != lastFp {
			res = append(res, ExemplarSeries{SeriesLabels: series})
			lastFp = fp
		}
		last := &res[len(res)-1]
		last.Exemplars = append(last.Exemplars, e)
	}
	return res, rows.Err()
}

func (s *MetricLabelsService) strings(ctx context.Context, q metricread.IndexQuery,
	build func(metricread.IndexQuery) string) ([]string, bool, error) {
	rows, q, err := s.query(ctx, q, build)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	res := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, false, err
		}
		res = append(res, v)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	res, truncated := truncate(res, q.Limit)
	return res, truncated, nil
}

// query runs the SQL build makes from q, reading the distributed tables on a cluster.
func (s *MetricLabelsService) query(ctx context.Context, q metricread.IndexQuery,
	build func(metricread.IndexQuery) string) (*gosql.Rows, metricread.IndexQuery, error) {
	conn, err := s.Session.GetDB(ctx)
	if err != nil {
		return nil, q, err
	}
	q.Cluster = conn.Config.ClusterName != ""
	sql := build(q)
	logger.Debug("[ MetricLabels ] ", sql)
	rows, err := conn.Session.QueryCtx(ctx, sql)
	return rows, q, err
}

// truncate cuts res to limit when it holds more; 0 keeps everything.
func truncate[T any](res []T, limit int) ([]T, bool) {
	if limit > 0 && len(res) > limit {
		return res[:limit], true
	}
	return res, false
}
