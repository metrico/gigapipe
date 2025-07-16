package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func setTyped[T any](val any) T {
	var res T
	if val == nil {
		return res
	}
	if res, ok := val.(T); ok {
		return res
	}
	return res
}

func setInt64(val any) int64 {
	str := setTyped[string](val)
	if str == "" {
		return 0
	}
	res, _ := strconv.ParseInt(str, 10, 64)
	return res
}

func getRawAttr(attrs []any, key string) map[string]any {
	for _, attr := range attrs {
		_attr := attr.(map[string]any)
		if _attr["key"] == key {
			return _attr
		}
	}
	return nil
}

func getRawVal(attrs []any, key string) map[string]any {
	attr := getRawAttr(attrs, key)
	if attr == nil {
		return nil
	}
	return attr["value"].(map[string]any)
}

func otlpGetServiceNames(attrs []any) (string, string) {
	local := ""
	remote := ""
	for _, attr := range []string{
		"peer.service", "service.name", "faas.name", "k8s.deployment.name", "process.executable.name",
	} {
		val := getRawVal(attrs, attr)
		if val == nil {
			continue
		}
		_val, ok := val["stringValue"]
		if !ok {
			continue
		}
		local = _val.(string)
	}
	for _, attr := range []string{"service.name", "faas.name", "k8s.deployment.name", "process.executable.name"} {
		val := getRawVal(attrs, attr)
		if val == nil {
			continue
		}
		_val, ok := val["stringValue"]
		if !ok {
			continue
		}
		remote = _val.(string)
	}
	if local == "" {
		local = "OTLPResourceNoServiceName"
	}
	return local, remote
}

func toFloat64(val any) float64 {
	if _val, ok := val.(float64); ok {
		return _val
	}
	if _val, ok := val.(string); ok {
		res, _ := strconv.ParseFloat(_val, 64)
		return res
	}
	return 0
}

func toInt64(val any) int64 {
	if _val, ok := val.(int64); ok {
		return _val
	}
	if _val, ok := val.(string); ok {
		res, _ := strconv.ParseInt(_val, 10, 64)
		return res
	}
	return 0
}

func setRawValue(rawVal map[string]any, val *common.AnyValue) {
	if rawVal["stringValue"] != nil {
		val.Value = &common.AnyValue_StringValue{
			StringValue: rawVal["stringValue"].(string),
		}
	}
	if rawVal["intValue"] != nil {

		val.Value = &common.AnyValue_IntValue{
			IntValue: toInt64(rawVal["intValue"]),
		}
	}
	if rawVal["boolValue"] != nil {
		val.Value = &common.AnyValue_BoolValue{
			BoolValue: rawVal["boolValue"].(bool),
		}
	}
	if rawVal["doubleValue"] != nil {
		val.Value = &common.AnyValue_DoubleValue{
			DoubleValue: toFloat64(rawVal["doubleValue"]),
		}
	}
}

func getAttr(attrs []*common.KeyValue, key string) *common.KeyValue {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr
		}
	}
	return nil
}

func setOTLPIds(rawSpan map[string]any, span *v1.Span) error {
	base64DEcode := func(val any) ([]byte, error) {
		if val == nil {
			return nil, nil
		}
		if _, ok := val.(string); !ok {
			return nil, fmt.Errorf("invalid traceId")
		}
		res, err := base64.StdEncoding.DecodeString(val.(string))
		return res, err
	}
	var err error
	span.TraceId, err = base64DEcode(rawSpan["traceId"])
	if err != nil {
		return err
	}
	span.SpanId, err = base64DEcode(rawSpan["spanId"])
	if err != nil {
		return err
	}
	span.ParentSpanId, err = base64DEcode(rawSpan["parentSpanId"])
	if err != nil {
		return err
	}
	return nil
}

func setTimestamps(rawSpan map[string]any, span *v1.Span) {
	span.StartTimeUnixNano = uint64(setInt64(rawSpan["startTimeUnixNano"]))
	span.EndTimeUnixNano = uint64(setInt64(rawSpan["endTimeUnixNano"]))
	events := setTyped[[]any](rawSpan["events"])
	for i, e := range events {
		_e, ok := e.(map[string]any)
		if !ok {
			continue
		}
		span.Events[i].TimeUnixNano = uint64(setInt64(_e["timeUnixNano"]))
	}
}

func parseOTLPJson(payload *zipkinPayload) (*v1.Span, error) {
	span := &v1.Span{}
	rawSpan := make(map[string]any)
	err := json.Unmarshal([]byte(payload.payload), &rawSpan)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(payload.payload), span)
	if err != nil {
		return nil, err
	}
	err = setOTLPIds(rawSpan, span)
	if err != nil {
		return nil, err
	}
	setTimestamps(rawSpan, span)

	attributes := setTyped[[]any](rawSpan["attributes"])
	localServiceName, remoteServiceName := otlpGetServiceNames(attributes)
	attr := getAttr(span.Attributes, "service.name")
	if attr != nil {
		attr.Value.Value = &common.AnyValue_StringValue{
			StringValue: localServiceName,
		}
	} else {
		span.Attributes = append(span.Attributes, &common.KeyValue{
			Key: "service.name",
			Value: &common.AnyValue{
				Value: &common.AnyValue_StringValue{
					StringValue: localServiceName,
				},
			},
		})
	}
	attr = getAttr(span.Attributes, "remoteService.name")
	if attr != nil {
		attr.Value.Value = &common.AnyValue_StringValue{
			StringValue: remoteServiceName,
		}
	} else {
		span.Attributes = append(span.Attributes, &common.KeyValue{
			Key: "remoteService.name",
			Value: &common.AnyValue{
				Value: &common.AnyValue_StringValue{
					StringValue: remoteServiceName,
				},
			},
		})
	}

	for _, a := range attributes {
		_a, ok := a.(map[string]any)
		if !ok {
			continue
		}
		if _a["key"] == "service.name" || _a["key"] == "remoteService.name" {
			continue
		}

		attr := getAttr(span.Attributes, _a["key"].(string))
		if attr == nil {
			continue
		}
		setRawValue(_a["value"].(map[string]any), attr.Value)
	}
	return span, err
}
