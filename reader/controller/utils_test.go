package controller

import (
	"testing"
	"time"
)

func TestParseTimeSecOrRFCMagnitudes(t *testing.T) {
	def := time.Unix(0, 0)
	cases := []struct {
		raw  string
		want time.Time
	}{
		{"1785416892", time.Unix(1785416892, 0)},                                // seconds
		{"1785416892.5", time.Unix(1785416892, 500000000)},                      // fractional seconds
		{"1785416892000", time.Unix(1785416892, 0)},                             // milliseconds
		{"1785416892123", time.Unix(1785416892, 123000000)},                     // milliseconds, sub-second
		{"1785416892000000", time.Unix(1785416892, 0)},                          // microseconds
		{"1785416892123456", time.Unix(1785416892, 123456000)},                  // microseconds, sub-second
		{"1785416892000000000", time.Unix(0, 1785416892000000000)},              // nanoseconds
		{"1785416892123456789", time.Unix(0, 1785416892123456789)},              // nanoseconds, exact
		{"2026-07-30T13:00:00Z", time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC)}, // RFC3339
	}
	for _, c := range cases {
		got, err := ParseTimeSecOrRFC(c.raw, def)
		if err != nil {
			t.Fatalf("%s: %v", c.raw, err)
		}
		if !got.Equal(c.want) {
			t.Errorf("%s: got %v want %v", c.raw, got.UTC(), c.want.UTC())
		}
	}
	got, _ := ParseTimeSecOrRFC("", def)
	if !got.Equal(def) {
		t.Errorf("empty: got %v", got)
	}
}

// epochToTime is shared with the Tempo query path, which previously used a
// coarser variant that misread microsecond epochs as milliseconds and
// nanosecond epochs below 1e18 as milliseconds.
func TestEpochToTimeSharedWithTempo(t *testing.T) {
	cases := []struct {
		in   int64
		want time.Time
	}{
		{1785416892, time.Unix(1785416892, 0)},
		{1785416892123, time.Unix(1785416892, 123000000)},
		{1785416892123456, time.Unix(1785416892, 123456000)},
		{1785416892123456789, time.Unix(0, 1785416892123456789)},
		{999999999123456789, time.Unix(0, 999999999123456789)}, // ns epoch < 1e18
		{0, time.Unix(0, 0)},
	}
	for _, c := range cases {
		if got := epochToTime(c.in, 0); !got.Equal(c.want) {
			t.Errorf("%d: got %v want %v", c.in, got.UTC(), c.want.UTC())
		}
	}
}
