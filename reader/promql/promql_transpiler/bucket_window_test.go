package promql_transpiler

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestBucketCoversTheIntervalEndingAtItsKey pins which samples a bucket holds,
// which is what decides the window every accelerated range function evaluates.
//
// Buckets are grouped by a key and the window frame then reaches the keys inside
// (t-range, t]. Keying a bucket by the FLOOR of its samples' timestamps makes it
// hold [key, key+b) -- the interval starting at its key -- so the frame at t
// reaches samples up to t+b, past the timestamp being evaluated: a window
// shifted forward by a whole bucket, reporting data the caller could not have
// seen yet. Prometheus evaluates (t-range, t] and nothing after t.
//
// Keying by the CEILING makes a bucket hold (key-b, key] instead, so the frame
// ends exactly at t and the buckets tile the range backwards from there.
func TestBucketCoversTheIntervalEndingAtItsKey(t *testing.T) {
	const bucketNs, bucketMs = int64(60000000000), int64(60000)
	floorKey := fmt.Sprintf("intDiv(timestamp_ns, %d) * %d", bucketNs, bucketMs)
	ceilKey := fmt.Sprintf("intDiv(timestamp_ns + %d, %d) * %d", bucketNs-1, bucketNs, bucketMs)

	for _, q := range []string{
		`sum_over_time(x{job="j"}[5m])`,
		`count_over_time(x{job="j"}[5m])`,
		`last_over_time(x{job="j"}[5m])`,
		`rate(x{job="j"}[5m])`,
		`increase(x{job="j"}[5m])`,
		`resets(x{job="j"}[5m])`,
	} {
		t.Run(q, func(t *testing.T) {
			ctx := rangeTestCtx()
			ctx.Step = time.Minute
			got := transpileRangeCtx(t, q, ctx)
			if strings.Contains(got, floorKey) {
				t.Errorf("bucket keyed by the floor of its samples (%s): it then holds the\n"+
					"interval STARTING at its key, so the frame at t reaches samples after t:\n%s",
					floorKey, got)
			}
			if !strings.Contains(got, ceilKey) {
				t.Errorf("expected a bucket keyed by the ceiling (%s), so it holds the\n"+
					"interval ending at its key:\n%s", ceilKey, got)
			}
		})
	}
}

// bucketExpr is the grouping key a BucketProducer renders for a bucket width.
// Buckets are keyed by the ceiling of a sample's timestamp, so a bucket holds
// the interval ending at its key; see
// TestBucketCoversTheIntervalEndingAtItsKey.
func bucketExpr(d time.Duration) string {
	return fmt.Sprintf("intDiv(timestamp_ns + %d, %d) * %d",
		d.Nanoseconds()-1, d.Nanoseconds(), d.Milliseconds())
}

// TestOverTimeTilesItsRangeWhateverTheStep replaces a test that asserted the
// opposite. sum_over_time and friends used to bucket at the query's own step
// however coarse it got, on the grounds that a reducer needs only one bucket in
// the window to have an answer. It does -- but the answer is then computed over
// that whole bucket rather than over the range: sum_over_time(x[5m]) at a 30m
// step summed thirty minutes and labelled it five.
//
// One bucket is enough only when the bucket is the window. Since the frame tiles
// (t-range, t] out of whole buckets keyed at their right edge, the width has to
// divide the range, which is exactly what the counter functions already need.
func TestOverTimeTilesItsRangeWhateverTheStep(t *testing.T) {
	fns := []string{"sum_over_time", "count_over_time", "min_over_time", "max_over_time", "avg_over_time", "last_over_time"}
	for _, fn := range fns {
		t.Run(fn, func(t *testing.T) {
			for _, step := range []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour} {
				ctx := rangeTestCtx()
				ctx.Step = step
				got := transpileRangeCtx(t, fn+`(x{job="j"}[5m])`, ctx)
				if wide := bucketExpr(step); strings.Contains(got, wide) {
					t.Errorf("step=%s: bucketed at the query's step (%s), which is wider than\n"+
						"the 5m range, so one bucket covers more than the window:\n%s", step, wide, got)
				}
				if want := bucketExpr(150 * time.Second); !strings.Contains(got, want) {
					t.Errorf("step=%s: expected a width that tiles the range (%s):\n%s", step, want, got)
				}
			}
		})
	}
}
