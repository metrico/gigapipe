package unmarshal

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/metrico/qryn/reader/model"
	v12 "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func SpanToJSONSpan(span *v1.Span) *model.JSONSpan {
	res := &model.JSONSpan{
		TraceID:           hex.EncodeToString(span.TraceId),
		TraceId:           hex.EncodeToString(span.TraceId),
		SpanID:            hex.EncodeToString(span.SpanId),
		SpanId:            hex.EncodeToString(span.SpanId),
		Name:              span.Name,
		StartTimeUnixNano: span.StartTimeUnixNano,
		EndTimeUnixNano:   span.EndTimeUnixNano,
		ServiceName:       "",
		Attributes:        make([]model.JSONSpanAttribute, len(span.Attributes)),
		Events:            make([]model.JSONSpanEvent, len(span.Events)),
		Status:            span.Status,
	}
	for i, attr := range span.Attributes {
		_attr := model.JSONSpanAttribute{
			Key: attr.Key,
			Value: struct {
				StringValue string `json:"stringValue"`
			}{},
		}
		switch attr.Value.Value.(type) {
		case *v12.AnyValue_StringValue:
			_attr.Value.StringValue = attr.Value.GetStringValue()
		case *v12.AnyValue_BoolValue:
			_attr.Value.StringValue = fmt.Sprintf("%v", attr.Value.GetBoolValue())
		case *v12.AnyValue_IntValue:
			_attr.Value.StringValue = fmt.Sprintf("%v", attr.Value.GetIntValue())
		case *v12.AnyValue_DoubleValue:
			_attr.Value.StringValue = fmt.Sprintf("%v", attr.Value.GetDoubleValue())
		case *v12.AnyValue_BytesValue:
			_attr.Value.StringValue = base64.StdEncoding.EncodeToString(attr.Value.GetBytesValue())
		default:
			bVal, _ := json.Marshal(attr.Value.Value)
			_attr.Value.StringValue = string(bVal)
		}
		res.Attributes[i] = _attr
	}
	for _, attr := range span.Attributes {
		if attr.Key == "service.name" && attr.Value.GetStringValue() != "" {
			res.ServiceName = attr.Value.GetStringValue()
		}
	}
	if len(span.ParentSpanId) > 0 && hex.EncodeToString(span.ParentSpanId) != "0000000000000000" {
		res.ParentSpanId = hex.EncodeToString(span.ParentSpanId)
	}
	for i, evt := range span.Events {
		res.Events[i] = model.JSONSpanEvent{
			TimeUnixNano: evt.TimeUnixNano,
			Name:         evt.Name,
		}

	}
	return res
}
