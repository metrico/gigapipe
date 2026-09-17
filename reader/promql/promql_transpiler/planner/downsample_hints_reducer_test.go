package planner

import (
	"fmt"
	"strings"
	"testing"
)

// TestDownsampleHintsReducerTrailingWindow characterizes the branch that had no
// coverage at all before this: a reducer whose step outruns its range reads only
// the trailing range of each step and snaps it forward onto the step it belongs
// to. The expected SQL here is the output of this planner before the switch that
// now selects between the three shapes was introduced, captured verbatim, so a
// change to that restructuring shows up here rather than in production.
func TestDownsampleHintsReducerTrailingWindow(t *testing.T) {
	const step, rng = int64(600000), int64(300000)
	wantBucket := fmt.Sprintf("intDiv(samples.timestamp_ns + %d * 1000000, %d * 1000000) * %d", rng, step, step)
	wantWhere := fmt.Sprintf("(((timestamp_ns %% %d000000) == (0)) or ((timestamp_ns %% %d000000) > (%d000000)))",
		step, step, step-rng)

	for _, fn := range []string{"sum_over_time", "count_over_time", "last_over_time", "absent_over_time"} {
		t.Run(fn, func(t *testing.T) {
			got := renderHints(t, fn, step, rng)
			if !strings.Contains(got, wantBucket) {
				t.Errorf("expected the trailing-window bucket (%s):\n%s", wantBucket, got)
			}
			if !strings.Contains(got, wantWhere) {
				t.Errorf("expected the trailing-window prefilter (%s):\n%s", wantWhere, got)
			}
		})
	}
}
