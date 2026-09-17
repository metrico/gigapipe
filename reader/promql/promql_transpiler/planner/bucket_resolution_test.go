package planner

import (
	"testing"
	"time"
)

// TestBucketResolution covers every band of the step-to-duration ratio, because
// the boundaries are where this has gone wrong before: a bucket that is not
// strictly finer than the window it is read through cannot put two distinct
// samples inside it, and a function that measures a change between two samples
// then returns nothing at all rather than an error.
func TestBucketResolution(t *testing.T) {
	const d = 300 * time.Second
	half := d / 2

	for _, tc := range []struct {
		name string
		step time.Duration
		want time.Duration
	}{
		{"far finer than half the duration", 15 * time.Second, 15 * time.Second},
		{"just under half", half - time.Millisecond, half - time.Millisecond},
		{"exactly half", half, half},
		{"just over half", half + time.Millisecond, half},
		{"between half and the full duration", 200 * time.Second, half},
		{"one millisecond under the duration", d - time.Millisecond, half},
		{"exactly the duration", d, half},
		{"past the duration", 6 * time.Minute, half},
		{"far past the duration", time.Hour, half},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := BucketResolution(tc.step, d); got != tc.want {
				t.Errorf("BucketResolution(%s, %s) = %s, want %s", tc.step, d, got, tc.want)
			}
		})
	}
}

// TestBucketResolutionNeverReachesTheDuration is the property the whole cap
// exists for, asserted directly rather than through a rendered query: whatever
// the step, the bucket must stay strictly finer than the window, or a
// (t-duration, t] frame can hold only one of them.
func TestBucketResolutionNeverReachesTheDuration(t *testing.T) {
	for _, d := range []time.Duration{15 * time.Second, time.Minute, 5 * time.Minute, time.Hour} {
		for step := time.Second; step <= 2*time.Hour; step += 997 * time.Millisecond {
			if got := BucketResolution(step, d); got >= d {
				t.Fatalf("BucketResolution(%s, %s) = %s: not finer than the duration", step, d, got)
			}
		}
	}
}

// TestBucketResolutionIsIdempotent is what lets the request layer and the
// planners both apply it to the same query without fighting over the answer.
func TestBucketResolutionIsIdempotent(t *testing.T) {
	for _, d := range []time.Duration{time.Second, 15 * time.Second, 5 * time.Minute, time.Hour} {
		for step := time.Millisecond; step <= 2*time.Hour; step += 1013 * time.Millisecond {
			once := BucketResolution(step, d)
			if twice := BucketResolution(once, d); twice != once {
				t.Fatalf("BucketResolution(%s, %s) = %s, applied again = %s", step, d, once, twice)
			}
		}
	}
}

// TestBucketResolutionTinyDuration pins the floor. A duration of a millisecond
// or less cannot be halved on the millisecond grid the SQL is expressed on, and
// must not fall through to returning the step unchanged -- that is the very
// shape the cap exists to prevent.
func TestBucketResolutionTinyDuration(t *testing.T) {
	for _, d := range []time.Duration{0, time.Millisecond, 2 * time.Millisecond} {
		if got := BucketResolution(time.Hour, d); got != time.Millisecond {
			t.Errorf("BucketResolution(1h, %s) = %s, want 1ms", d, got)
		}
	}
}

// TestNeedsDistinctSamples pins the single list. A function added to the wrong
// side of it either loses the cap it needs (silent empty results, the #980 bug)
// or pays for one it does not (every reducer bucketing at half its range).
func TestNeedsDistinctSamples(t *testing.T) {
	needs := []string{"rate", "irate", "deriv", "delta", "idelta", "resets", "increase", "changes"}
	reduces := []string{
		"sum_over_time", "count_over_time", "min_over_time", "max_over_time",
		"avg_over_time", "last_over_time", "present_over_time", "absent_over_time",
		"stddev_over_time", "stdvar_over_time", "quantile_over_time",
		"", "abs", "sum", "topk",
	}
	for _, fn := range needs {
		if !NeedsDistinctSamples(fn) {
			t.Errorf("%s measures a change across samples and must be capped", fn)
		}
	}
	for _, fn := range reduces {
		if NeedsDistinctSamples(fn) {
			t.Errorf("%s answers from a single sample and must keep the query's own step", fn)
		}
	}
}
