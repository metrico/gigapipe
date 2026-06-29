package ruler

import "testing"

func TestParseInstantVector_VectorResult(t *testing.T) {
	js := `{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"level":"error"},"value":[1700000000.5,"3.14"]}
	]}}`
	v, err := parseInstantVector(js)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(v) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(v))
	}
	s := v[0]
	if got := s.Metric.Get("level"); got != "error" {
		t.Errorf("label level = %q, want error", got)
	}
	if s.F != 3.14 {
		t.Errorf("value = %v, want 3.14", s.F)
	}
	if s.T != 1700000000500 { // seconds → ms
		t.Errorf("timestamp = %d, want 1700000000500", s.T)
	}
}

func TestParseInstantVector_Empty(t *testing.T) {
	v, err := parseInstantVector("")
	if err != nil || len(v) != 0 {
		t.Fatalf("empty input should yield empty vector, got %v err=%v", v, err)
	}
}

func TestNewLogQLEvaluator_MaxResultBytes(t *testing.T) {
	// Non-positive values fall back to the default cap.
	for _, n := range []int{0, -1} {
		if got := NewLogQLEvaluator(nil, n).maxResultBytes; got != DefaultMaxLogQLResultBytes {
			t.Errorf("maxResultBytes(%d) = %d, want default %d", n, got, DefaultMaxLogQLResultBytes)
		}
	}
	// A positive override is kept.
	if got := NewLogQLEvaluator(nil, 1234).maxResultBytes; got != 1234 {
		t.Errorf("maxResultBytes(1234) = %d, want 1234", got)
	}
}
