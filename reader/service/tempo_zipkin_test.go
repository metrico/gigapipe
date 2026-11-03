package service

import (
	"testing"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestParseZipkinJSON(t *testing.T) {
	payload := &zipkinPayload{
		traceId:     "0123456789abcdef",
		spanId:      "01234567",
		startTimeNs: 1000,
		durationNs:  500,
		payload: `{
			"traceId": "ignored", "kind": "SERVER", "name": "GET /xé",
			"parentId": "0102030405060708",
			"tags": {"http.method": "GET", "n": 1, "b": true, "o": {"a": "b"}, "e": ""},
			"localEndpoint": {"port": 8080, "ipv4": "10.0.0.1", "serviceName": "svc"},
			"remoteEndpoint": {"ipv6": "::1", "port": 1.5, "serviceName": 7},
			"annotations": [{"timestamp": 3, "value": "ev"}, {"timestamp": 0, "value": "skip"}, {"value": "no ts"}]
		}`,
	}
	span, serviceName, err := parseZipkinJSON(payload)
	if err != nil {
		t.Fatal(err)
	}
	if serviceName != "svc" {
		t.Fatalf("serviceName = %q", serviceName)
	}
	str := func(k, v string) *common.KeyValue {
		return &common.KeyValue{Key: k, Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: v}}}
	}
	want := &v1.Span{
		TraceId:           []byte("0123456789abcdef"),
		SpanId:            []byte("01234567"),
		ParentSpanId:      []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Name:              "GET /xé",
		Kind:              v1.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: 1000,
		EndTimeUnixNano:   1500,
		Attributes: []*common.KeyValue{
			str("http.method", "GET"),
			str("e", ""),
			str("localEndpoint.serviceName", "svc"),
			str("localEndpoint.ipv4", "10.0.0.1"),
			{Key: "localEndpoint.port", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 8080}}},
			str("remoteEndpoint.ipv6", "::1"),
		},
		Events: []*v1.Span_Event{{TimeUnixNano: 3000, Name: "ev"}},
		Status: &v1.Status{Code: v1.Status_STATUS_CODE_UNSET},
	}
	if !proto.Equal(span, want) {
		t.Fatalf("got  %v\nwant %v", span, want)
	}

	if _, _, err := parseZipkinJSON(&zipkinPayload{traceId: payload.traceId, spanId: payload.spanId, payload: `{"name":`}); err == nil {
		t.Fatal("expected an error on truncated payload")
	}
}
