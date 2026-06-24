package shared

import (
	"fmt"
	"testing"
	"time"
)

func TestPrepareLineFormatTemplateBuiltins(t *testing.T) {
	tpl, bind, err := PrepareLineFormatTemplate(`{{.k8s_object_name}} - {{__line__}}`)
	if err != nil {
		t.Fatalf("PrepareLineFormatTemplate() error = %v", err)
	}

	entry := LogEntry{
		TimestampNS: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
		Labels: map[string]string{
			"k8s_object_name": "worker-1",
		},
		Message: "original log line",
	}

	got, err := ExecuteLineFormatTemplate(tpl, bind, entry)
	if err != nil {
		t.Fatalf("ExecuteLineFormatTemplate() error = %v", err)
	}
	want := "worker-1 - original log line"
	if got != want {
		t.Fatalf("ExecuteLineFormatTemplate() = %q, want %q", got, want)
	}
}

func TestPrepareLineFormatTemplateTimestamp(t *testing.T) {
	tpl, bind, err := PrepareLineFormatTemplate(`{{ __timestamp__ | unixEpoch }}`)
	if err != nil {
		t.Fatalf("PrepareLineFormatTemplate() error = %v", err)
	}

	ts := time.Date(2024, 6, 1, 12, 34, 56, 0, time.UTC)
	entry := LogEntry{
		TimestampNS: ts.UnixNano(),
		Message:     "line",
	}

	got, err := ExecuteLineFormatTemplate(tpl, bind, entry)
	if err != nil {
		t.Fatalf("ExecuteLineFormatTemplate() error = %v", err)
	}
	if got != fmt.Sprintf("%d", ts.Unix()) {
		t.Fatalf("ExecuteLineFormatTemplate() = %q, want %q", got, fmt.Sprintf("%d", ts.Unix()))
	}
}

