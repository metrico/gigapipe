package planner

import (
	"testing"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

func TestShouldKeepLabel(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		val    string
		labels []string
		values []string
		want   bool
	}{
		{
			name:   "keep label without value constraint",
			key:    "level",
			val:    "info",
			labels: []string{"level"},
			values: []string{""},
			want:   true,
		},
		{
			name:   "keep label with matching value",
			key:    "method",
			val:    "GET",
			labels: []string{"method"},
			values: []string{"GET"},
			want:   true,
		},
		{
			name:   "keep label with non-matching value",
			key:    "method",
			val:    "POST",
			labels: []string{"method"},
			values: []string{"GET"},
			want:   false,
		},
		{
			name:   "label not in keep list",
			key:    "path",
			val:    "/",
			labels: []string{"level"},
			values: []string{""},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldKeepLabel(tt.key, tt.val, tt.labels, tt.values); got != tt.want {
				t.Fatalf("shouldKeepLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeepPlannerFilterLabels(t *testing.T) {
	planner := KeepPlanner{
		Labels: []string{"k8s_object_kind", "method"},
		Values: []string{"", "GET"},
	}

	entry := shared.LogEntry{
		Labels: map[string]string{
			"k8s_object_kind": "Node",
			"method":          "POST",
			"path":            "/",
		},
		Fingerprint: 123,
	}

	if err := planner.filterLabels(&entry); err != nil {
		t.Fatalf("filterLabels() error = %v", err)
	}

	if _, ok := entry.Labels["k8s_object_kind"]; !ok {
		t.Fatalf("expected k8s_object_kind to be kept")
	}
	if _, ok := entry.Labels["method"]; ok {
		t.Fatalf("expected method to be dropped when value does not match")
	}
	if _, ok := entry.Labels["path"]; ok {
		t.Fatalf("expected path to be dropped")
	}
}
