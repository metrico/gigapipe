package unmarshal

import (
	"context"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/writer/model"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func otlpTracesData(start, end uint64) *tracev1.TracesData {
	return &tracev1.TracesData{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{otlpStringAttr("service.name", "svc")}},
		ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{{
			Name: "op", TraceId: []byte("0123456789abcdef"), SpanId: []byte("01234567"),
			StartTimeUnixNano: start, EndTimeUnixNano: end,
		}}}},
	}}}
}

func TestOTLPTracesFromData_EmitsSpans(t *testing.T) {
	td := otlpTracesData(0, 0)
	cache := newTestNumberCache()
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

func TestOTLPTracesFromData_SpanDuration(t *testing.T) {
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
			cache := newTestNumberCache()
			defer cache.Stop()
			var spans, attrs []int64
			for resp := range OTLPTracesFromData(otlpTracesData(start, tc.end))(context.Background(), nil, cache) {
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
			if len(spans) != 1 || spans[0] != tc.want {
				t.Fatalf("span durations = %v, want [%d]", spans, tc.want)
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
