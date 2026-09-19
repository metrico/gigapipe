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

// TestCounterForwardEdgeCannotGoNegative ties the keying to the one column that
// measured how wrong it was.
//
// c_fwd_edge is how far the range's last real sample sits before the timestamp
// being evaluated, (timestamp_ms - last_ts) / 1000, and it feeds c_reach, which
// carries the observed change out to the edges of the range. last_ts is an
// argMax over a frame ending at the current row, so for a row carrying data it
// is that row's own val_ts -- the newest sample in its bucket. A bucket keyed by
// the floor of its samples holds [key, key+width), so that sample is routinely
// NEWER than the key, and c_fwd_edge goes negative: measured against the e2e
// fixture at a 150s bucket over 15s samples, 61 of 62 buckets, worst case -135s,
// which is bucket minus sample interval exactly. Keyed by the ceiling the same
// data gives 0 of 61.
//
// (For a uniform series the error cancels -- a back edge enlarged by the same
// shift puts c_reach back at 1 -- so this was a malformed intermediate rather
// than a demonstrated wrong rate. The cancellation is a property of the fixture,
// not of the arithmetic, which is why the keying is asserted instead.)
func TestCounterForwardEdgeCannotGoNegative(t *testing.T) {
	for _, fn := range []string{"rate", "increase", "delta"} {
		t.Run(fn, func(t *testing.T) {
			ctx := rangeTestCtx()
			ctx.Step = time.Minute
			got := transpileRangeCtx(t, fn+`(x{job="j"}[5m])`, ctx)

			// The column whose sign this is about.
			if want := "(timestamp_ms - last_ts) / 1000 as c_fwd_edge"; !strings.Contains(got, want) {
				t.Fatalf("expected %q; this test is anchored to that column:\n%s", want, got)
			}
			// Its sign is guaranteed by where the bucket sits relative to its key,
			// and by nothing else in the query.
			if bad := bucketExpr(time.Minute); strings.Contains(got,
				strings.Replace(bad, "timestamp_ns + 59999999999", "timestamp_ns", 1)) {
				t.Errorf("buckets keyed by the floor of their samples: last_ts can then\n"+
					"exceed timestamp_ms and c_fwd_edge goes negative:\n%s", got)
			}
			if !strings.Contains(got, bucketExpr(time.Minute)) {
				t.Errorf("expected ceiling-keyed buckets so no sample outlives its key:\n%s", got)
			}
		})
	}
}
