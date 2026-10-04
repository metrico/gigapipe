package metricread

import "slices"

// subBucketMinMs is the narrowest sub-bucket a raw read groups samples into: one no wider than a
// 15s scrape interval holds about one sample, so fanning out each sample reads as many rows.
const subBucketMinMs = 15000

// subBucketMs is the width a raw read of p groups samples into before the fan-out, gcd(step,
// range), or 0 to fan out each sample: at an instant, when the range reaches no further than one
// step, or when the width is at most subBucketMinMs.
func subBucketMs(p Pushdown) int64 {
	step, rng := p.Grid.StepMs, p.RangeMs
	if step <= 0 || rng <= step {
		return 0
	}
	w := gcd(step, rng)
	if w <= subBucketMinMs {
		return 0
	}
	return w
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// subBucketRowsSQL is the raw row shape per (fingerprint, t) read through sub-buckets of width
// w, each built as a tier bucket is. Sub-bucket ends lie on start + j·w; as w divides both step
// and range, both ends of every window (t − range, t] lie on that grid, so a window is exactly
// range / w whole sub-buckets.
func subBucketRowsSQL(p Pushdown, w int64) string {
	cols := shapeOf(p.Func)
	ms := subBuckets.merges(cols)
	return bucketRowsSQL(p, subBuckets, w, ms, subBucketsSQL(p, ms, cols))
}

// subBuckets reads raw samples grouped per sub-bucket. Each sub-bucket keeps the penult of its
// last sample, so irate and idelta stay exact.
var subBuckets = bucketRead{columns: &subBucketColumn, reads: &subBucketReads, lags: column.pairsAcross, ms: "bucket"}

// subBucketColumn is each column's aggregate over a window's sub-buckets.
var subBucketColumn = func() [nColumns]string {
	c := tierColumn
	c[colPenult] = "argMax(b_penult, b_ms) AS penult"
	return c
}()

// subBucketReads is the sub-bucket merges each column's aggregate reads.
var subBucketReads = func() [nColumns][]merge {
	r := columnMerges
	r[colPenult] = []merge{bPenult}
	return r
}()

// pairsAcross reports whether a column counts pairs of samples, inside a sub-bucket and across
// the edge between two.
func (c column) pairsAcross() bool {
	return c == colResets || c == colResetDrop || c == colChanges
}

// subBucketMerge is each merge over a sub-bucket's raw samples, as the tier views reduce a bucket.
// paired holds for a sample whose predecessor lies in the same sub-bucket.
var subBucketMerge = [nMerges]string{
	bFirst:     "(minIf(timestamp, NOT stale), argMinIf(value, timestamp, NOT stale))",
	bLast:      "(maxIf(timestamp, NOT stale), argMaxIf(value, timestamp, NOT stale))",
	bCount:     "countIf(NOT stale)",
	bSum:       "if(isFinite(sumIf(value, NOT stale)), sumKahanIf(value, NOT stale), sumIf(value, NOT stale))",
	bVar:       "varPopStableIfState(value, NOT stale)",
	bMin:       "minIf(if(isNaN(value), inf, value), NOT stale)",
	bMax:       "maxIf(if(isNaN(value), -inf, value), NOT stale)",
	bResets:    "countIf(paired AND value < prev_value)",
	bResetDrop: "sumIf(prev_value, paired AND value < prev_value)",
	bChanges:   "countIf(paired AND value != prev_value AND NOT (isNaN(value) AND isNaN(prev_value)))",
	bStaleAt:   "maxIf(timestamp, stale)",
	bPenult:    "argMaxIf(prev, timestamp, NOT stale)",
}

// subBucketsSQL groups p's raw samples per (fingerprint, sub-bucket), keyed by the sub-bucket's
// end in unix ms. A sub-bucket of stale markers alone is kept only for the stale_at column.
func subBucketsSQL(p Pushdown, ms merges, cols shape) string {
	window, having := "", " HAVING b_count > 0"
	if cols.prev() {
		window = ", " + prevSQL
	}
	if slices.ContainsFunc(cols, column.pairsAcross) {
		window += ", NOT stale AND prev_ms > start_ms + (j - 1) * w_ms AS paired"
	}
	if cols.has(colStaleAt) {
		having = ""
	}
	return "SELECT fingerprint, start_ms + j * w_ms AS bucket, " + ms.sql(&subBucketMerge) + " " +
		"FROM (SELECT fingerprint, timestamp, value, " +
		"toUnixTimestamp64Milli(timestamp) AS ts_ms, " +
		"reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale, " +
		"if(ts_ms >= start_ms, intDiv(ts_ms - start_ms + w_ms - 1, w_ms), -intDiv(start_ms - ts_ms, w_ms)) AS j" +
		window + " " +
		"FROM (" + rawWindowSQL(p) + ")) " +
		"GROUP BY fingerprint, j" + having
}
