package service

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"slices"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/prometheus/prometheus/model/labels"
)

// MetricLabelsService answers the Prometheus label, series, metadata and exemplar endpoints
// from the metric series index.
type MetricLabelsService struct {
	model.ServiceData
	plugin plugins.MetricLabelsServicePlugin
}

func NewMetricLabelsService(sd *model.ServiceData) *MetricLabelsService {
	p := plugins.GetMetricLabelsServicePlugin()
	res := &MetricLabelsService{ServiceData: *sd}
	if p != nil {
		(*p).SetServiceData(sd)
		res.plugin = *p
	}
	return res
}

// LabelNames returns the label names of the series q picks, and whether q.Limit cut them.
func (s *MetricLabelsService) LabelNames(ctx context.Context, q metricread.IndexQuery) ([]string, bool, error) {
	if s.plugin != nil {
		return s.plugin.LabelNames(ctx, q)
	}
	return s.strings(ctx, q, metricread.LabelNamesSQL)
}

// LabelValues returns the values of label name over the series q picks, and whether q.Limit
// cut them.
func (s *MetricLabelsService) LabelValues(ctx context.Context, name string,
	q metricread.IndexQuery) ([]string, bool, error) {
	if s.plugin != nil {
		return s.plugin.LabelValues(ctx, name, q)
	}
	return s.strings(ctx, q, func(q metricread.IndexQuery) string { return metricread.LabelValuesSQL(name, q) })
}

// Series returns the label set of every series q picks, and whether q.Limit cut them.
func (s *MetricLabelsService) Series(ctx context.Context, q metricread.IndexQuery) ([]map[string]string, bool, error) {
	if s.plugin != nil {
		return s.plugin.Series(ctx, q)
	}
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

// Metadata returns the latest metadata of every family, or of metric alone when set, keyed
// by family, up to limit families; a negative limit reads every family.
func (s *MetricLabelsService) Metadata(ctx context.Context, metric string, limit int) (map[string][]model.MetricMetadata, error) {
	if s.plugin != nil {
		return s.plugin.Metadata(ctx, metric, limit)
	}
	rows, _, err := s.query(ctx, metricread.IndexQuery{}, func(q metricread.IndexQuery) string {
		return metricread.MetadataSQL(metric, limit, q.Cluster)
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string][]model.MetricMetadata{}
	for rows.Next() {
		var name string
		var m model.MetricMetadata
		if err := rows.Scan(&name, &m.Type, &m.Help, &m.Unit); err != nil {
			return nil, err
		}
		res[name] = []model.MetricMetadata{m}
	}
	return res, rows.Err()
}

// Exemplars returns the exemplars in [q.StartMs, q.EndMs] of the series q picks, per series,
// sorted by the series' label set.
func (s *MetricLabelsService) Exemplars(ctx context.Context, q metricread.IndexQuery) ([]model.ExemplarSeries, error) {
	if s.plugin != nil {
		return s.plugin.Exemplars(ctx, q)
	}
	rows, _, err := s.query(ctx, q, metricread.ExemplarsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := []model.ExemplarSeries{}
	var lastFp uint64
	for rows.Next() {
		var (
			fp     uint64
			series map[string]string
			ts     time.Time
			e      model.Exemplar
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
			res = append(res, model.ExemplarSeries{SeriesLabels: series})
			lastFp = fp
		}
		last := &res[len(res)-1]
		last.Exemplars = append(last.Exemplars, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(res, func(a, b model.ExemplarSeries) int {
		return labels.Compare(labels.FromMap(a.SeriesLabels), labels.FromMap(b.SeriesLabels))
	})
	return res, nil
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
