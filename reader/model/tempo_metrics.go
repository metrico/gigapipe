package model

import (
	"encoding/json"
	"time"
)

// MetricsQueryRequest holds parsed parameters for a metrics query.
type MetricsQueryRequest struct {
	Query string
	From  time.Time
	To    time.Time
	Step  time.Duration
}

// MetricsKeyValue is a Tempo-compatible label pair.
type MetricsKeyValue struct {
	Key   string            `json:"key"`
	Value MetricsLabelValue `json:"value"`
}

// MetricsLabelValue wraps a typed value for JSON serialization.
type MetricsLabelValue struct {
	StringValue string   `json:"stringValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	IntValue    string   `json:"intValue,omitempty"`
}

// MetricsSample is a single (timestamp, value) point in a time series.
type MetricsSample struct {
	TimestampMs string  `json:"timestampMs"`
	Value       float64 `json:"value,omitempty"`
}

// MetricsTimeSeries is a single series with labels and samples (range query).
type MetricsTimeSeries struct {
	Labels     []MetricsKeyValue `json:"labels"`
	Samples    []MetricsSample   `json:"samples"`
	PromLabels string            `json:"promLabels,omitempty"`
	Exemplars  []interface{}     `json:"exemplars,omitempty"`
}

// MetricsQueryRangeResponse is the top-level response for /api/metrics/query_range.
type MetricsQueryRangeResponse struct {
	Series  []MetricsTimeSeries `json:"series"`
	Metrics json.RawMessage     `json:"metrics"`
}

// MetricsInstantSeries is a single series for instant queries.
type MetricsInstantSeries struct {
	Labels    []MetricsKeyValue `json:"labels"`
	Value     float64           `json:"value"`
	Exemplars []interface{}     `json:"exemplars"`
}

// MetricsQueryInstantResponse is the top-level response for /api/metrics/query.
type MetricsQueryInstantResponse struct {
	Series  []MetricsInstantSeries `json:"series"`
	Metrics json.RawMessage        `json:"metrics"`
}
