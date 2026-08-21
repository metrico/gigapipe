package unmarshal

import (
	"context"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestOTLPTracesFromData_EmitsSpans(t *testing.T) {
	td := &tracev1.TracesData{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
		}}},
		ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{{
			Name: "op", TraceId: []byte("0123456789abcdef"), SpanId: []byte("01234567"),
		}}}},
	}}}
	cache := numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{})
	defer cache.Stop()
	ch := OTLPTracesFromData(td)(context.Background(), nil, cache)
	var spans int
	var sawName, sawTraceID bool
	for resp := range ch {
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		ts, ok := resp.SpansRequest.(*model.TempoSamples)
		if !ok || ts == nil {
			continue
		}
		for _, name := range ts.MName {
			if strings.Contains(name, "op") {
				sawName = true
			}
		}
		for _, tid := range ts.MTraceId {
			if len(tid) > 0 {
				sawTraceID = true
			}
		}
		spans += len(ts.MName)
	}
	if spans == 0 {
		t.Fatal("expected at least one span in the emitted TempoSamples")
	}
	if !sawName {
		t.Fatal("expected an emitted span with Name containing \"op\"")
	}
	if !sawTraceID {
		t.Fatal("expected an emitted span with a non-empty TraceId")
	}
}
