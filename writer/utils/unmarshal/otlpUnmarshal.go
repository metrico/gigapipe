package unmarshal

import (
	"fmt"
	customErrors "github.com/metrico/qryn/writer/utils/errors"
	v11 "go.opentelemetry.io/proto/otlp/common/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type OTLPDecoder struct {
	ctx    *ParserCtx
	onSpan onSpanHandler
}

func getOtlpAttr(attrs []*v11.KeyValue, key string) *v11.KeyValue {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr
		}
	}
	return nil
}

func otlpGetServiceNames(attrs []*v11.KeyValue) (string, string) {
	local := ""
	remote := ""
	for _, attr := range []string{
		"peer.service", "service.name", "faas.name", "k8s.deployment.name", "process.executable.name",
	} {
		val := getOtlpAttr(attrs, attr)
		if val == nil {
			continue
		}
		_val, ok := val.Value.Value.(*v11.AnyValue_StringValue)
		if !ok {
			continue
		}
		local = _val.StringValue
	}
	for _, attr := range []string{"service.name", "faas.name", "k8s.deployment.name", "process.executable.name"} {
		val := getOtlpAttr(attrs, attr)
		if val == nil {
			continue
		}
		_val, ok := val.Value.Value.(*v11.AnyValue_StringValue)
		if !ok {
			continue
		}
		remote = _val.StringValue
	}
	if local == "" {
		local = "OTLPResourceNoServiceName"
	}
	return local, remote
}

func populateServiceNames(span *trace.Span) {
	local, remote := otlpGetServiceNames(span.Attributes)
	attr := getOtlpAttr(span.Attributes, "service.name")
	if attr == nil {
		span.Attributes = append(span.Attributes,
			&v11.KeyValue{Key: "service.name", Value: &v11.AnyValue{Value: &v11.AnyValue_StringValue{StringValue: local}}},
		)
	}
	attr = getOtlpAttr(span.Attributes, "remoteService.name")
	if attr == nil {
		span.Attributes = append(span.Attributes,
			&v11.KeyValue{Key: "remoteService.name", Value: &v11.AnyValue{Value: &v11.AnyValue_StringValue{StringValue: remote}}},
		)
	}
}

func (d *OTLPDecoder) Decode() error {
	obj := d.ctx.bodyObject.(*trace.TracesData)
	for _, res := range obj.ResourceSpans {
		for _, scope := range res.ScopeSpans {
			for _, span := range scope.Spans {
				span.Attributes = append(span.Attributes, res.Resource.Attributes...)
				attrsMap := map[string][]string{}
				populateServiceNames(span)
				d.initAttributesMap(span.Attributes, "", &attrsMap)
				payload, err := proto.Marshal(span)
				if err != nil {
					return customErrors.NewUnmarshalError(err)
				}
				attrsMap["name"] = []string{span.Name}
				keys := make([]string, 0, len(attrsMap))
				vals := make([]string, 0, len(attrsMap))
				for k, _v := range attrsMap {
					for _, v := range _v {
						keys = append(keys, k)
						vals = append(vals, v)
					}

				}
				serviceName := ""
				if len(attrsMap["service.name"]) > 0 {
					serviceName = attrsMap["service.name"][0]
				}
				err = d.onSpan(span.TraceId, span.SpanId, int64(span.StartTimeUnixNano),
					int64(span.EndTimeUnixNano-span.StartTimeUnixNano),
					string(span.ParentSpanId), span.Name, serviceName, payload,
					keys, vals)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *OTLPDecoder) SetOnEntry(h onSpanHandler) {
	d.onSpan = h
}

func (d *OTLPDecoder) getAttrValue(val any) string {
	switch val.(type) {
	case *v11.AnyValue_StringValue:
		return val.(*v11.AnyValue_StringValue).StringValue
	case *v11.AnyValue_BoolValue:
		return fmt.Sprintf("%v", val.(*v11.AnyValue_BoolValue).BoolValue)
	case *v11.AnyValue_DoubleValue:
		return fmt.Sprintf("%f", val.(*v11.AnyValue_DoubleValue).DoubleValue)
	case *v11.AnyValue_IntValue:
		return fmt.Sprintf("%d", val.(*v11.AnyValue_IntValue).IntValue)
	}
	return ""
}

func (d *OTLPDecoder) writeAttrValue(key string, val any, prefix string, res *map[string][]string) {
	switch val.(type) {
	case *v11.AnyValue_StringValue:
	case *v11.AnyValue_BoolValue:
	case *v11.AnyValue_DoubleValue:
	case *v11.AnyValue_IntValue:
		(*res)[prefix+key] = append((*res)[prefix+key], d.getAttrValue(val))
	case *v11.AnyValue_ArrayValue:

		for _, _val := range val.(*v11.AnyValue_ArrayValue).ArrayValue.Values {
			d.writeAttrValue(key, _val, prefix, res)
		}
	case *v11.AnyValue_KvlistValue:
		d.initAttributesMap(val.(*v11.AnyValue_KvlistValue).KvlistValue.Values, prefix+key+".", res)
	}
}

func (d *OTLPDecoder) initAttributesMap(attrs any, prefix string, res *map[string][]string) {
	if _attrs, ok := attrs.([]*v11.KeyValue); ok {
		for _, kv := range _attrs {
			d.writeAttrValue(kv.Key, kv.Value.Value, prefix, res)
		}
	}
}

var UnmarshalOTLPV2 = Build(
	withPayloadType(2),
	withBufferedBody,
	withParsedBody(func() proto.Message { return &trace.TracesData{} }),
	withSpansParser(func(ctx *ParserCtx) iSpansParser { return &OTLPDecoder{ctx: ctx} }))
