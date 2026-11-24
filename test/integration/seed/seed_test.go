package seed

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var testStart = time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)

// TestLogsAreDeterministic checks that generating the same fixture twice gives
// byte-identical push bodies, and that Run only changes the run label.
func TestLogsAreDeterministic(t *testing.T) {
	first := LokiPushJSON(Default("r1", testStart).Logs())
	second := LokiPushJSON(Default("r1", testStart).Logs())
	if !bytes.Equal(first, second) {
		t.Fatal("the same fixture produced different push bodies")
	}

	other := LokiPushJSON(Default("r2", testStart).Logs())
	if !bytes.Equal(bytes.ReplaceAll(other, []byte(`"r2"`), []byte(`"r1"`)), first) {
		t.Fatal("changing Run changed more than the run label")
	}
}

// TestLogsMatchDeclaration checks every stream against its Pod declaration:
// labels, line count, error count, timestamps and line content.
func TestLogsMatchDeclaration(t *testing.T) {
	f := Default("r1", testStart)
	streams := f.Logs()

	i := 0
	for _, svc := range f.Services {
		for _, pod := range svc.Pods {
			s := streams[i]
			i++
			want := map[string]string{RunLabel: "r1", "app": svc.Name, "pod": pod.Name}
			if len(s.Labels) != len(want) {
				t.Errorf("%s/%s: labels = %v, want %v", svc.Name, pod.Name, s.Labels, want)
			}
			for k, v := range want {
				if s.Labels[k] != v {
					t.Errorf("%s/%s: label %s = %q, want %q", svc.Name, pod.Name, k, s.Labels[k], v)
				}
			}
			if len(s.Lines) != pod.Lines {
				t.Fatalf("%s/%s: %d lines, want %d", svc.Name, pod.Name, len(s.Lines), pod.Lines)
			}
			errors := 0
			for n, l := range s.Lines {
				if wantTime := testStart.Add(time.Duration(n) * LineInterval); !l.Time.Equal(wantTime) {
					t.Errorf("%s/%s line %d: time %v, want %v", svc.Name, pod.Name, n, l.Time, wantTime)
				}
				var body struct {
					Level string `json:"level"`
					Msg   string `json:"msg"`
					Seq   int    `json:"seq"`
				}
				if err := json.Unmarshal([]byte(l.Text), &body); err != nil {
					t.Fatalf("%s/%s line %d is not JSON: %v", svc.Name, pod.Name, n, err)
				}
				if body.Level != l.Level || body.Seq != n || body.Msg == "" {
					t.Errorf("%s/%s line %d: body %+v does not match level %q, seq %d", svc.Name, pod.Name, n, body, l.Level, n)
				}
				if l.Level == "error" {
					errors++
				}
			}
			if errors != pod.ErrorLines {
				t.Errorf("%s/%s: %d error lines, want %d", svc.Name, pod.Name, errors, pod.ErrorLines)
			}
		}
	}
	if i != len(streams) {
		t.Errorf("%d streams, want %d", len(streams), i)
	}
}
