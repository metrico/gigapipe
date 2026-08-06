package unmarshal

import (
	"context"
	"testing"
	"time"
	"unsafe"

	clconfig "github.com/metrico/cloki-config"
	clokiconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

func TestOTLPLogsFromData_EmitsSamples(t *testing.T) {
	// The full ParsingFunction path runs the logs decoder through onEntries ->
	// fingerprintLabels, which reads config.Cloki.Setting.FingerPrintType. Install
	// a minimal global config so that read does not nil-pointer panic.
	old := config.Cloki
	config.Cloki = &clconfig.ClokiConfig{Setting: &clokiconfig.ClokiBaseSettingServer{}}
	t.Cleanup(func() { config.Cloki = old })

	ld := &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{{
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{{
			TimeUnixNano: 1,
			Body:         &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "hello"}},
		}}}},
	}}}

	// The record's message must actually flow through the decoder into the
	// flushed SamplesRequest. Asserting only SamplesRequest != nil is vacuous:
	// doParseLogs always flushes a non-nil (possibly empty) TimeSamplesData.
	if got := collectMessages(t, ld); !containsMessage(got, "hello") {
		t.Fatalf("expected flushed SamplesRequest to contain %q; got messages %v", "hello", got)
	}

	// Negative sanity check: an empty LogsData must not produce the message,
	// proving the positive assertion above is not vacuous.
	if got := collectMessages(t, &logsv1.LogsData{}); containsMessage(got, "hello") {
		t.Fatalf("empty LogsData unexpectedly produced %q; got messages %v", "hello", got)
	}
}

// collectMessages runs the pre-decoded logs parser over ld and returns every
// message that reaches the flushed SamplesRequest (*model.TimeSamplesData).
func collectMessages(t *testing.T, ld *logsv1.LogsData) []string {
	t.Helper()
	cache := numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{})
	defer cache.Stop()

	var messages []string
	for resp := range OTLPLogsFromData(ld)(context.Background(), nil, cache) {
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if resp.SamplesRequest == nil {
			continue
		}
		spl, ok := resp.SamplesRequest.(*model.TimeSamplesData)
		if !ok {
			t.Fatalf("SamplesRequest is %T, want *model.TimeSamplesData", resp.SamplesRequest)
		}
		messages = append(messages, spl.MMessage...)
	}
	return messages
}

func containsMessage(messages []string, want string) bool {
	for _, m := range messages {
		if m == want {
			return true
		}
	}
	return false
}
