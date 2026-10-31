package model

import (
	"encoding/json"
	"math"

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

type TraceInfo struct {
	TraceID           string    `json:"traceID"`
	RootServiceName   string    `json:"rootServiceName"`
	RootTraceName     string    `json:"rootTraceName"`
	StartTimeUnixNano string    `json:"startTimeUnixNano"`
	DurationMs        int64     `json:"durationMs"`
	SpanSet           SpanSet   `json:"spanSet"`
	SpanSets          []SpanSet `json:"spanSets"`
}

type SpanInfo struct {
	SpanID            string     `json:"spanID"`
	Name              string     `json:"name,omitempty"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	DurationNanos     string     `json:"durationNanos"`
	Attributes        []SpanAttr `json:"attributes"`
}

type SpanSet struct {
	Spans      []SpanInfo    `json:"spans"`
	Matched    int           `json:"matched"`
	Attributes []SpanSetAttr `json:"attributes,omitempty"`
}

// SpanSetAttr is a spanset attribute, such as an aggregate value, as an OTLP
// key-value pair.
type SpanSetAttr struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// AnyValue is an OTLP AnyValue in protobuf JSON: one field is set; an int is
// a decimal string.
type AnyValue struct {
	StringValue *string `json:"stringValue,omitempty"`
	IntValue    *string `json:"intValue,omitempty"`
	DoubleValue *Double `json:"doubleValue,omitempty"`
	BoolValue   *bool   `json:"boolValue,omitempty"`
}

// Double is a float that encodes infinities and NaN as protobuf JSON strings.
type Double float64

func (d Double) MarshalJSON() ([]byte, error) {
	f := float64(d)
	switch {
	case math.IsNaN(f):
		return []byte(`"NaN"`), nil
	case math.IsInf(f, 1):
		return []byte(`"Infinity"`), nil
	case math.IsInf(f, -1):
		return []byte(`"-Infinity"`), nil
	}
	return json.Marshal(f)
}

// SpanAttr is a span attribute holding either a string or an int value.
type SpanAttr struct {
	Key   string `json:"key"`
	Value struct {
		StringValue *string `json:"stringValue,omitempty"`
		IntValue    *string `json:"intValue,omitempty"`
	} `json:"value"`
}
