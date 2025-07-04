package model

import (
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

type SpanResponse struct {
	Span        *v1.Span
	ServiceName string
}

type TraceResponse struct {
	TraceID           string `json:"traceID"`
	RootServiceName   string `json:"rootServiceName"`
	RootTraceName     string `json:"rootTraceName"`
	StartTimeUnixNano int64  `json:"startTimeUnixNano"`
	DurationMs        int64  `json:"durationMs"`
}

type TSDBStatus struct {
	TotalSeries                  int32              `json:"totalSeries"`
	TotalLabelValuePairs         int32              `json:"totalLabelValuePairs"`
	SeriesCountByMetricName      []TSDBStatusMetric `json:"seriesCountByMetricName"`
	SeriesCountByLabelName       []TSDBStatusMetric `json:"seriesCountByLabelName"`
	SeriesCountByFocusLabelValue []TSDBStatusMetric `json:"seriesCountByFocusLabelValue"`
	SeriesCountByLabelValuePair  []TSDBStatusMetric `json:"seriesCountByLabelValuePair"`
	LabelValueCountByLabelName   []TSDBStatusMetric `json:"labelValueCountByLabelName"`
	Quota                        int32              `json:"quota"`
}

type TSDBStatusMetric struct {
	Name  string `json:"name"`
	Value int32  `json:"value"`
}

type TraceQLResponse struct {
	TraceInfo chan []TraceInfo
	Metrics   chan TraceMetricStream
}

type TraceMetricStream struct {
	Labels  []SpanAttr          `json:"labels"`
	Samples []TraceMetricSample `json:"samples"`
}

type TraceMetricSample struct {
	TimestampMs string  `json:"timestampMs"`
	Value       float64 `json:"value"`
}

type TraceInfo struct {
	TraceID           string    `json:"traceID"`
	RootServiceName   string    `json:"rootServiceName"`
	RootTraceName     string    `json:"rootTraceName"`
	StartTimeUnixNano string    `json:"startTimeUnixNano"`
	DurationMs        float64   `json:"durationMs"`
	SpanSet           SpanSet   `json:"spanSet"`
	SpanSets          []SpanSet `json:"spanSets"`
}

type SpanInfo struct {
	SpanID            string     `json:"spanID"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	DurationNanos     string     `json:"durationNanos"`
	Attributes        []SpanAttr `json:"attributes"`
}

type SpanSet struct {
	Spans   []SpanInfo `json:"spans"`
	Matched int        `json:"matched"`
}

type SpanAttr struct {
	Key   string        `json:"key"`
	Value SpanAttrValue `json:"value"`
}

type SpanAttrValue struct {
	StringValue string `json:"stringValue"`
}
