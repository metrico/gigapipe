package unmarshal

import (
	"encoding/base64"
	"encoding/json"
	"github.com/metrico/qryn/writer/model"
	otlpCommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpLogs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
	"regexp"
	"strconv"
	"time"
)

type otlpLogDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler
}

func (e *otlpLogDec) Decode() error {
	logs := e.ctx.bodyObject.(*otlpLogs.LogsData)

	for _, resLog := range logs.ResourceLogs {
		resourceAttrs := map[string]string{}
		e.initAttributesMap(resLog.Resource.Attributes, "", &resourceAttrs)
		for _, scopeLog := range resLog.ScopeLogs {
			scopeAttrs := map[string]string{}
			e.initAttributesMap(scopeLog.Scope.Attributes, "", &scopeAttrs)
			for _, logRecord := range scopeLog.LogRecords {
				var labels [][]string
				// Merge resource and scope attributes
				attrsMap := make(map[string]string)
				for k, v := range resourceAttrs {
					attrsMap[k] = v
				}
				for k, v := range scopeAttrs {
					attrsMap[k] = v
				}
				// Extract log record attributes
				e.initAttributesMap(logRecord.Attributes, "", &attrsMap)

				// Extract severity_text and add as level label
				if severityText := logRecord.SeverityText; severityText != "" {
					attrsMap["level"] = severityText
				}

				for k, v := range attrsMap {
					labels = append(labels, []string{k, v})
				}
				// Extract other log record fields
				message := logRecord.Body.GetStringValue()
				timestamp := logRecord.TimeUnixNano

				if timestamp == 0 {
					timestamp = logRecord.ObservedTimeUnixNano
				}
				if timestamp == 0 {
					timestamp = uint64(time.Now().UnixNano())
				}
				// Call onEntries with labels and other details
				err := e.onEntries(
					labels,
					[]int64{int64(timestamp)},
					[]string{message},
					[]float64{0},
					[]uint8{model.SAMPLE_TYPE_LOG},
				)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (e *otlpLogDec) initAttributesMap(attrs []*otlpCommon.KeyValue, prefix string, res *map[string]string) {
	for _, kv := range attrs {
		e.writeAttrValue(kv.Key, kv.Value, prefix, res)
	}
}

func (e *otlpLogDec) writeAttrValue(key string, value *otlpCommon.AnyValue, prefix string, res *map[string]string) {
	(*res)[prefix+SanitizeKey(key)] = SanitizeValue(value)
}

func SanitizeKey(key string) string {
	// Replace characters that are not a-z, A-Z, 0-9, or _ with _
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := re.ReplaceAllString(key, "_")

	// Prefix with _ if the first character is not a-z or A-Z
	if len(sanitized) == 0 || (sanitized[0] >= '0' && sanitized[0] <= '9') {
		sanitized = "_" + sanitized
	}

	return sanitized
}

func SanitizeValue(value *otlpCommon.AnyValue) string {
	switch v := value.Value.(type) {
	case *otlpCommon.AnyValue_StringValue:
		return v.StringValue
	case *otlpCommon.AnyValue_BoolValue:
		return strconv.FormatBool(v.BoolValue)
	case *otlpCommon.AnyValue_IntValue:
		return strconv.FormatInt(v.IntValue, 10)
	case *otlpCommon.AnyValue_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
	case *otlpCommon.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(v.BytesValue)
	case *otlpCommon.AnyValue_ArrayValue:
		items := make([]string, len(v.ArrayValue.Values))
		for i, item := range v.ArrayValue.Values {
			items[i] = SanitizeValue(item)
		}
		jsonItems, _ := json.Marshal(items)
		return string(jsonItems)
	case *otlpCommon.AnyValue_KvlistValue:
		kvMap := make(map[string]string)
		for _, kv := range v.KvlistValue.Values {
			kvMap[SanitizeKey(kv.Key)] = SanitizeValue(kv.Value)
		}
		jsonMap, _ := json.Marshal(kvMap)
		return string(jsonMap)
	default:
		return ""
	}
}

func (e *otlpLogDec) SetOnEntries(h onEntriesHandler) {
	e.onEntries = h
}

var UnmarshalOTLPLogsV2 = Build(
	withBufferedBody,
	withParsedBody(func() proto.Message { return &otlpLogs.LogsData{} }),
	withLogsParser(func(ctx *ParserCtx) iLogsParser {
		return &otlpLogDec{ctx: ctx}
	}))
