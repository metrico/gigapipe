package promql_transpiler

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestCounterCapsTheBandBelowTheRange covers the step band that neither side of
// this code had a test for: a step coarser than half the range but still finer
// than the range itself.
//
// The frame is (t-range, t], so a bucket grid of that width does put two slots
// in reach -- t and t-step -- and the query is not empty. But exactly two, with
// the earlier one sitting on the frame's first millisecond: a series that
// happens not to have reported into that one bucket collapses back to a single
// sample and the row is dropped, silently, the same symptom as #980 for a
// series that is merely sparse rather than for every series at once.
//
// Half the range is what the request layer has always used for this band
// (adjustHintsForRate) and what the cap is documented to do, so the two agree
// and the band keeps a third slot in reach.
func TestCounterCapsTheBandBelowTheRange(t *testing.T) {
	const half = "intDiv(timestamp_ns, 150000000000) * 150000"

	for _, fn := range []string{"rate", "increase", "delta", "resets", "changes"} {
		t.Run(fn, func(t *testing.T) {
			for _, step := range []time.Duration{
				151 * time.Second, // just past half the range
				200 * time.Second, // mid band
				299 * time.Second, // one second under the range
			} {
				ctx := rangeTestCtx()
				ctx.Step = step
				got := transpileRangeCtx(t, fn+`(x{job="j"}[5m])`, ctx)

				stepBucket := fmt.Sprintf("intDiv(timestamp_ns, %d) * %d",
					step.Nanoseconds(), step.Milliseconds())
				if strings.Contains(got, stepBucket) {
					t.Errorf("step=%s: bucketed at the query's own step (%s) -- leaves only two "+
						"slots in (t-range, t], the outer one on its first millisecond:\n%s",
						step, stepBucket, got)
				}
				if !strings.Contains(got, half) {
					t.Errorf("step=%s: expected the half-range bucket (%s):\n%s", step, half, got)
				}
			}
		})
	}
}

// TestCounterLeavesStepsFinerThanHalfTheRangeAlone is the other side of the same
// boundary: the cap must not reach down into steps that are already fine enough,
// where it would multiply the row count for no correctness gain.
func TestCounterLeavesStepsFinerThanHalfTheRangeAlone(t *testing.T) {
	for _, step := range []time.Duration{15 * time.Second, 60 * time.Second, 149 * time.Second, 150 * time.Second} {
		ctx := rangeTestCtx()
		ctx.Step = step
		got := transpileRangeCtx(t, `rate(x{job="j"}[5m])`, ctx)
		want := fmt.Sprintf("intDiv(timestamp_ns, %d) * %d", step.Nanoseconds(), step.Milliseconds())
		if !strings.Contains(got, want) {
			t.Errorf("step=%s: expected the query's own step bucket (%s):\n%s", step, want, got)
		}
	}
}
