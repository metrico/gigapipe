package planner

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/storage"
)

// TestMain initializes the package-global config.Cloki once before any test in
// this package runs: DownsampleHintsPlanner.Process reads
// config.Cloki.Setting.ClokiReader.Compat_4_0_19 directly, and is otherwise nil
// in a plain `go test` of this package. Mirrors
// promql_transpiler's own setup_test.go for the same reason.
func TestMain(m *testing.M) {
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	os.Exit(m.Run())
}

// hintsStubProducer stands in for whatever plain per-step group-by a real Main
// planner hands DownsampleHintsPlanner: one row per (fingerprint, step bucket)
// with placeholder val/timestamp_ms columns for patchField to rewrite.
type hintsStubProducer struct{}

func (hintsStubProducer) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("0", "val"),
		sql.NewSimpleCol("0", "timestamp_ms")).
		From(sql.NewRawObject("metrics_15s as samples")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")), nil
}

func renderHints(t *testing.T, fn string, step, rng int64) string {
	t.Helper()
	p := &DownsampleHintsPlanner{
		Main:  hintsStubProducer{},
		Hints: &storage.SelectHints{Func: fn, Step: step, Range: rng},
	}
	req, err := p.Process(&shared.PlannerContext{})
	if err != nil {
		t.Fatal(err)
	}
	str, err := req.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	return str
}

// TestDownsampleHintsCapsChangeFunctionBucket guards a bug that let irate(),
// deriv(), delta(), idelta(), resets(), rate() and increase() come back empty
// through this legacy per-step resampling path -- not only once the query's
// step reached or exceeded the function's own range, but well before that too,
// and not consistently: whether a given step failed depended on an alignment
// between the epoch-anchored bucket grid and the query's own evaluation
// timestamps that the caller never controls, so the same step could work one
// second and fail the next as "now" ticked forward.
//
// The fix caps the resample bucket to at most range/2 for these functions, a
// pigeonhole guarantee -- two buckets of that width always fit inside any
// (t-range, t] window, regardless of where that window's boundary happens to
// fall relative to the grid -- rather than resampling at the query's own step,
// which only leaves room for two by accident.
func TestDownsampleHintsCapsChangeFunctionBucket(t *testing.T) {
	const rng = int64(300000) // 300s, matching the bug report's rate window
	halfRange := rng / 2
	// Renders the bucket column at a given width. What this test asserts is the
	// width; the keying is one rule owned by bucketTimestampCol and guarded by
	// TestDownsampleHintsKeysChangeFunctionBucketsByTheCeiling, so spelling the
	// expression out by hand here would pin a second behaviour by accident.
	bucketOfWidth := func(width int64) string {
		return bucketTimestampCol("samples.timestamp_ns", time.Duration(width)*time.Millisecond)
	}
	cappedBucket := bucketOfWidth(halfRange)

	for _, fn := range []string{"rate", "irate", "deriv", "delta", "idelta", "resets", "increase"} {
		t.Run(fn, func(t *testing.T) {
			// A step already finer than range/2 keeps its own width, so long as it
			// divides the range. 60s does; the bucket must not be narrowed for a
			// step that is already fine enough, which would multiply the row count
			// for no gain.
			fineStep := int64(60000)
			if got := renderHints(t, fn, fineStep, rng); !strings.Contains(got, bucketOfWidth(fineStep)) {
				t.Errorf("step=%d (divides the range): must bucket at the query's own step:\n%s", fineStep, got)
			}

			// A fine step that does NOT divide the range rounds down to the next
			// width that does. Buckets are keyed by their right edge, so the frame
			// tiles (t-range, t] out of whole buckets; at 149s the frame would
			// reach three of them, back to t-447s, over-including 147s of a 300s
			// window. 100s tiles it exactly.
			if got := renderHints(t, fn, halfRange-1000, rng); !strings.Contains(got, bucketOfWidth(100000)) {
				t.Errorf("step=%d (not a divisor): expected the next width that tiles "+
					"the range (%s):\n%s", halfRange-1000, bucketOfWidth(100000), got)
			}

			// Anywhere from just above range/2 up through and past range itself,
			// the bucket must be capped to range/2, not left at the query's step.
			for _, step := range []int64{halfRange + 1000, rng - 1000, rng, rng + 1000, 2 * rng} {
				got := renderHints(t, fn, step, rng)
				if strings.Contains(got, bucketOfWidth(step)) {
					t.Errorf("%s: step=%d bucketed at the uncapped step -- a (t-range, t] "+
						"window can then catch too few buckets to compute a change, depending "+
						"on alignment:\n%s", fn, step, got)
				}
				if !strings.Contains(got, cappedBucket) {
					t.Errorf("%s: step=%d expected the range/2 cap (%s):\n%s", fn, step, cappedBucket, got)
				}
			}
		})
	}
}

// TestDownsampleHintsLeavesPlainAggregatesAtTheQueryStep guards the functions
// that reduce over every sample in the window instead of measuring a change
// between two of them: a single bucket inside (t-range, t] already gives them
// everything they need, so resampling at the query's own step -- coarser,
// cheaper -- is correct and must be left alone.
func TestDownsampleHintsLeavesPlainAggregatesAtTheQueryStep(t *testing.T) {
	const rng = int64(300000)
	for _, fn := range []string{"sum_over_time", "min_over_time", "max_over_time", "avg_over_time", "last_over_time"} {
		t.Run(fn, func(t *testing.T) {
			// step > range takes a different (pre-existing, untouched) branch of
			// its own; kept to <= range here to isolate this assertion to the one
			// this fix touches.
			for _, step := range []int64{rng / 2, rng} {
				want := bucketTimestampCol("samples.timestamp_ns",
					time.Duration(step)*time.Millisecond)
				if got := renderHints(t, fn, step, rng); !strings.Contains(got, want) {
					t.Errorf("step=%d: expected the uncapped step bucket (%s):\n%s", step, want, got)
				}
			}
		})
	}
}

// TestDownsampleHintsKeysChangeFunctionBucketsByTheCeiling guards the keying of
// the bucket whose width the test above guards. The two are independent: a
// bucket can be the right width and still cover the wrong interval.
//
// deriv, irate and idelta are the change functions with no accelerated planner
// of their own -- they are absent from the optimizer's rangeFns, so they are
// not rewritten into BucketProducer and reach this path instead, where the
// bucketed rows are handed to the engine as if they were the raw series. The
// timestamp each bucket reports is therefore the timestamp the engine sees, and
// it selects (t-range, t] over those. Keyed by the floor a bucket holds
// [key, key+width), so argMaxMerge(samples.last) reports at t the newest sample
// of [t, t+width) -- a window shifted a whole bucket into the future, carrying
// data the caller could not yet have seen. Keyed by the ceiling it holds
// (key-width, key] and ends exactly at t, which is also what BucketResolution
// already documents its width to be chosen for.
func TestDownsampleHintsKeysChangeFunctionBucketsByTheCeiling(t *testing.T) {
	const rng = int64(300000)
	for _, fn := range []string{"deriv", "irate", "idelta"} {
		t.Run(fn, func(t *testing.T) {
			for _, step := range []int64{60000, rng / 2, rng} {
				width := BucketResolution(
					time.Duration(step)*time.Millisecond,
					time.Duration(rng)*time.Millisecond)
				got := renderHints(t, fn, step, rng)

				floorKey := fmt.Sprintf("intDiv(samples.timestamp_ns, %d * 1000000) * %d",
					width.Milliseconds(), width.Milliseconds())
				if strings.Contains(got, floorKey) {
					t.Errorf("step=%d: bucket keyed by the floor of its samples (%s): it then "+
						"holds [key, key+width), so the value reported at t is one belonging "+
						"after t:\n%s", step, floorKey, got)
				}

				want := bucketTimestampCol("samples.timestamp_ns", width)
				if !strings.Contains(got, want) {
					t.Errorf("step=%d: expected a bucket keyed by the ceiling (%s), so it holds "+
						"the interval ending at its key:\n%s", step, want, got)
				}
			}
		})
	}
}
