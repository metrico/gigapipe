package unmarshal

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func otlpStringAttr(key, value string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key: key,
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: value},
		},
	}
}

func TestOtlpGetServiceNamesUsesFirstNonEmptyPreferredAttribute(t *testing.T) {
	local, remote := otlpGetServiceNames([]*commonv1.KeyValue{
		otlpStringAttr("peer.service", ""),
		otlpStringAttr("service.name", "svc"),
		otlpStringAttr("faas.name", "fn"),
		otlpStringAttr("process.executable.name", "proc"),
	})

	if local != "svc" {
		t.Fatalf("local service name = %q, want %q", local, "svc")
	}
	if remote != "svc" {
		t.Fatalf("remote service name = %q, want %q", remote, "svc")
	}
}

func decodeOTLPSpanDurations(t *testing.T, start, end uint64) (spans, attrs []int64) {
	t.Helper()
	td := &tracev1.TracesData{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{otlpStringAttr("service.name", "svc")}},
		ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{{
			Name: "op", TraceId: []byte("0123456789abcdef"), SpanId: []byte("01234567"),
			StartTimeUnixNano: start, EndTimeUnixNano: end,
		}}}},
	}}}
	cache := numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{})
	defer cache.Stop()
	for resp := range OTLPTracesFromData(td)(context.Background(), nil, cache) {
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if ts, ok := resp.SpansRequest.(*model.TempoSamples); ok && ts != nil {
			spans = append(spans, ts.MDurationNs...)
		}
		if tt, ok := resp.SpansAttrsRequest.(*model.TempoTag); ok && tt != nil {
			attrs = append(attrs, tt.MDurationNs...)
		}
	}
	return spans, attrs
}

func TestOTLPSpanDurationIsZeroWhenEndIsNotAfterStart(t *testing.T) {
	const start = uint64(1_700_000_000_000_000_000)
	cases := []struct {
		name string
		end  uint64
		want int64
	}{
		{"end before start", start - 5_000, 0},
		{"unset end", 0, 0},
		{"end equal to start", start, 0},
		{"normal span", start + 1_234_567, 1_234_567},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spans, attrs := decodeOTLPSpanDurations(t, start, tc.end)
			if len(spans) != 1 {
				t.Fatalf("span rows = %d, want 1", len(spans))
			}
			if spans[0] != tc.want {
				t.Fatalf("span duration = %d, want %d", spans[0], tc.want)
			}
			if len(attrs) == 0 {
				t.Fatal("no attribute rows emitted")
			}
			for _, d := range attrs {
				if d != tc.want {
					t.Fatalf("attribute duration = %d, want %d", d, tc.want)
				}
			}
		})
	}
}
