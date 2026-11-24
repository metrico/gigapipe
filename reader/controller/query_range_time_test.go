package controller

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// parseLokiRange must resolve start/end the way Loki's query_range documents
// them, and reject a window whose end is before its start.
func TestParseLokiRange(t *testing.T) {
	end := time.Unix(1_800_000_000, 0)
	ns := func(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

	cases := []struct {
		name      string
		query     string
		wantStart time.Time
		wantEnd   time.Time
	}{
		{"explicit start and end", "start=" + ns(end.Add(-5*time.Minute)) + "&end=" + ns(end), end.Add(-5 * time.Minute), end},
		{"start defaults to one hour before end", "end=" + ns(end), end.Add(-time.Hour), end},
		{"since sets start before end", "since=15m&end=" + ns(end), end.Add(-15 * time.Minute), end},
		{"start wins over since", "since=15m&start=" + ns(end.Add(-4*time.Hour)) + "&end=" + ns(end), end.Add(-4 * time.Hour), end},
		{"start equal to end", "start=" + ns(end) + "&end=" + ns(end), end, end},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			start, gotEnd, err := parseLokiRange(httptest.NewRequest("GET", "/?"+c.query, nil))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !start.Equal(c.wantStart) || !gotEnd.Equal(c.wantEnd) {
				t.Errorf("got [%v, %v], want [%v, %v]", start, gotEnd, c.wantStart, c.wantEnd)
			}
		})
	}

	// Without start and end the window is the last hour.
	start, gotEnd, err := parseLokiRange(httptest.NewRequest("GET", "/", nil))
	if err != nil {
		t.Fatalf("no start/end: unexpected error: %v", err)
	}
	if diff := time.Since(gotEnd); diff < 0 || diff > 5*time.Second {
		t.Errorf("no start/end: end = %v, want ~now", gotEnd)
	}
	if got := gotEnd.Sub(start); got != time.Hour {
		t.Errorf("no start/end: window = %v, want 1h", got)
	}

	for _, q := range []string{
		"start=" + ns(end) + "&end=" + ns(end.Add(-time.Nanosecond)),
		"start=" + ns(end.Add(5*time.Minute)) + "&end=" + ns(end),
		"since=abc",
		"start=abc",
		"end=abc",
	} {
		if _, _, err := parseLokiRange(httptest.NewRequest("GET", "/?"+q, nil)); err == nil {
			t.Errorf("%s: want an error", q)
		}
	}
}
