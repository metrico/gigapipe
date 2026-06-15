package unmarshal

import (
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

func kvlistBody(kvs ...*commonv1.KeyValue) *commonv1.AnyValue {
	return &commonv1.AnyValue{
		Value: &commonv1.AnyValue_KvlistValue{
			KvlistValue: &commonv1.KeyValueList{Values: kvs},
		},
	}
}

func stringBody(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{
		Value: &commonv1.AnyValue_StringValue{StringValue: s},
	}
}

func decodeOTLPLogs(t *testing.T, data *logsv1.LogsData) []string {
	t.Helper()
	var messages []string
	dec := &otlpLogDec{
		ctx: &ParserCtx{bodyObject: data},
	}
	dec.SetOnEntries(func(labels [][]string, timestampsNS []int64, msg []string, value []float64, types []uint8) error {
		messages = append(messages, msg...)
		return nil
	})
	if err := dec.Decode(); err != nil {
		t.Fatal(err)
	}
	return messages
}

func TestOTLPLogsKvlistBody(t *testing.T) {
	data := &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								TimeUnixNano: 1765906867319713000,
								Body: kvlistBody(
									otlpStringAttr("MESSAGE", "session closed for user root"),
									otlpStringAttr("SYSLOG_IDENTIFIER", "sudo"),
								),
							},
						},
					},
				},
			},
		},
	}

	messages := decodeOTLPLogs(t, data)
	if len(messages) == 0 {
		t.Fatal("no messages decoded")
	}
	if messages[0] == "" {
		t.Errorf("message is empty string; kvlistValue body not serialized")
	}
}

func TestOTLPLogsStringBody(t *testing.T) {
	data := &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								TimeUnixNano: 1765906867319713000,
								Body:         stringBody("hello world"),
							},
						},
					},
				},
			},
		},
	}

	messages := decodeOTLPLogs(t, data)
	if len(messages) == 0 {
		t.Fatal("no messages decoded")
	}
	if messages[0] != "hello world" {
		t.Errorf("message = %q, want %q", messages[0], "hello world")
	}
}

func TestOTLPLogsNilResourceAndScope(t *testing.T) {
	data := &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: nil,
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						Scope: nil,
						LogRecords: []*logsv1.LogRecord{
							{
								TimeUnixNano: 1765906867319713000,
								Body:         stringBody("nil resource test"),
							},
						},
					},
				},
			},
		},
	}

	messages := decodeOTLPLogs(t, data)
	if len(messages) == 0 {
		t.Fatal("no messages decoded; nil Resource/Scope caused panic")
	}
	if messages[0] != "nil resource test" {
		t.Errorf("message = %q, want %q", messages[0], "nil resource test")
	}
}
