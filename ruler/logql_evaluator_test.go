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
