package model

import v1 "go.opentelemetry.io/proto/otlp/trace/v1"

type JSONSpan struct {
	TraceID           string              `json:"traceID"`
	TraceId           string              `json:"traceId"`
	SpanID            string              `json:"spanID"`
	SpanId            string              `json:"spanId"`
	Name              string              `json:"name"`
	StartTimeUnixNano uint64              `json:"startTimeUnixNano"`
	EndTimeUnixNano   uint64              `json:"endTimeUnixNano"`
	ParentSpanId      string              `json:"parentSpanId,omitempty"`
	ServiceName       string              `json:"serviceName"`
	Attributes        []JSONSpanAttribute `json:"attributes"`
	Events            []JSONSpanEvent     `json:"events"`
	Status            *v1.Status          `json:"status,omitempty"`
}

type JSONSpanAttribute struct {
	Key   string `json:"key"`
	Value struct {
		StringValue string `json:"stringValue"`
	} `json:"value"`
}

type JSONSpanEvent struct {
	TimeUnixNano uint64 `json:"timeUnixNano"`
	Name         string `json:"name"`
}
