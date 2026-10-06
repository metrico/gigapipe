package model

import "github.com/prometheus/prometheus/model/labels"

// MetricIndexQuery picks the metric_series rows the label endpoints read.
type MetricIndexQuery struct {
	// Selectors are ORed; none picks every row.
	Selectors [][]*labels.Matcher
	// StartMs and EndMs bound the series' lifetime in unix milliseconds; nil drops the bound.
	StartMs, EndMs *int64
	// Limit reads one row more than it, for the truncation warning; 0 or math.MaxInt reads
	// every row.
	Limit int
	// Cluster reads the distributed table.
	Cluster bool
}

// MetricMetadata is a metric family's metadata entry.
type MetricMetadata struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
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
