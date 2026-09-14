package controller

import (
	"testing"
	"time"
)

// snapQueryRangeToNativeResolution aligns query_range's Start/End to the
// metrics_15s table's 15s grid before those exact values become the PromQL
// engine's range-query Start/End. The engine emits a point at every
// Start+k*Step <= End, so End must only ever move backward (or stay put) --
// moving it forward, as an earlier math.Ceil-based version did, fabricated a
// data point strictly after the timestamp the caller asked for.
func TestSnapQueryRangeToNativeResolution_NeverExtendsEnd(t *testing.T) {
	for offset := int64(0); offset < 30; offset++ {
		start := time.Unix(1_700_000_000+offset, 0)
		end := time.Unix(1_700_001_000+offset, 0)

		gotStart, gotEnd := snapQueryRangeToNativeResolution(start, end)

		if gotEnd.After(end) {
			t.Fatalf("offset=%d: snapped end %v is after requested end %v (delta %v)",
				offset, gotEnd, end, gotEnd.Sub(end))
		}
		if gotStart.After(start) {
			t.Fatalf("offset=%d: snapped start %v is after requested start %v", offset, gotStart, start)
		}
		if gotEnd.Unix()%15 != 0 {
			t.Fatalf("offset=%d: snapped end %v is not on the 15s grid", offset, gotEnd)
		}
		if gotStart.Unix()%15 != 0 {
			t.Fatalf("offset=%d: snapped start %v is not on the 15s grid", offset, gotStart)
		}
		if gotStart.After(gotEnd) {
			t.Fatalf("offset=%d: snapped start %v is after snapped end %v", offset, gotStart, gotEnd)
		}
	}
}

// Bounds that already sit on the 15s grid must pass through unchanged --
// this is the common case (dashboards querying aligned windows) and a
// flooring implementation must not perturb it.
func TestSnapQueryRangeToNativeResolution_AlreadyAlignedIsUnchanged(t *testing.T) {
	start := time.Unix(1_700_000_010, 0)     // 1_700_000_010 % 15 == 0
	end := time.Unix(1_700_000_010+15*67, 0) // also a multiple of 15

	gotStart, gotEnd := snapQueryRangeToNativeResolution(start, end)

	if !gotStart.Equal(start) {
		t.Fatalf("aligned start %v was changed to %v", start, gotStart)
	}
	if !gotEnd.Equal(end) {
		t.Fatalf("aligned end %v was changed to %v", end, gotEnd)
	}
}

// Regression test for the specific bug report: end = start + 1000s, neither
// a multiple of 15, previously came back as end + (15 - end%15), i.e. one
// extra 15s bucket past what was requested.
func TestSnapQueryRangeToNativeResolution_RegressionExactCase(t *testing.T) {
	start := time.Unix(1_700_000_007, 0)
	end := time.Unix(1_700_001_007, 0) // 1_700_001_007 % 15 == 7, not aligned

	_, gotEnd := snapQueryRangeToNativeResolution(start, end)

	wantEnd := time.Unix(1_700_001_000, 0) // floor(1_700_001_007/15)*15
	if !gotEnd.Equal(wantEnd) {
		t.Fatalf("got end %v, want floored end %v (old buggy behavior ceiled to %v)",
			gotEnd, wantEnd, time.Unix(1_700_001_015, 0))
	}
}
