package controller

import (
	"testing"
	"time"
)

// Prometheus clients send fractional unix seconds. ParseInt rejects those, so
// they used to fall through to the caller-supplied default -- a silently wrong
// time window rather than an error.
func TestParserTimeStringAcceptsFractionalUnixSeconds(t *testing.T) {
	def := time.Unix(0, 0)
	got := parserTimeString("1700000000.500", def)
	if got.Unix() != 1700000000 {
		t.Fatalf("seconds = %d, want 1700000000 (fell back to the default)", got.Unix())
	}
	if ms := got.Nanosecond() / int(time.Millisecond); ms != 500 {
		t.Errorf("milliseconds = %d, want 500", ms)
	}
}

func TestParserTimeStringStillAcceptsTheOlderForms(t *testing.T) {
	def := time.Unix(42, 0)
	if got := parserTimeString("1700000000", def); got.Unix() != 1700000000 {
		t.Errorf("integer seconds = %d", got.Unix())
	}
	if got := parserTimeString("2026-10-23T00:00:00Z", def); got.Year() != 2026 {
		t.Errorf("RFC3339 = %v", got)
	}
	if got := parserTimeString("not a time", def); !got.Equal(def) {
		t.Errorf("garbage = %v, want the default %v", got, def)
	}
}
