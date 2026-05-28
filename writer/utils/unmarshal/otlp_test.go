package unmarshal

import (
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
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
