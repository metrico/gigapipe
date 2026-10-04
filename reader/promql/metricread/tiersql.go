package metricread

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
)

// tierPushdownSQL evaluates p from p.Tier's buckets. Each evaluation timestamp is snapped down
// to the tier grid and each range widened to whole buckets, at least one; each point is
// stamped at the query's own timestamp. An aligned read is evaluated as it stands.
func tierPushdownSQL(p Pushdown) string {
	q := tierRead(p)
	rows := tierInstantRowsSQL(q)
	if q.Func != "" {
		rows = tierRowsSQL(q)
	}
	sql := pushdownSQL(q, rows)
	if q.Grid == p.Grid {
		return sql
	}
	return stamp(sql, p.Grid, p.Tier.WidthMs)
}

// tierRead is p as its tier evaluates it: the grid snapped down and a range function's range
// widened to whole buckets, at least one.
func tierRead(p Pushdown) Pushdown {
	w := p.Tier.WidthMs
	q := p
	q.Grid = snap(p.Grid, w)
	if q.Func != "" {
		q.RangeMs = max(w, (p.RangeMs+w-1)/w*w)
	}
	return q
}

// snap moves g's timestamps down to the grid of width w: its step stays when a multiple of w
// and becomes w otherwise.
func snap(g Grid, w int64) Grid {
	down := func(ms int64) int64 { return ms - ((ms%w)+w)%w }
	step := g.StepMs
	if step%w != 0 {
		step = w
	}
	return Grid{StartMs: down(g.StartMs), EndMs: down(g.EndMs), StepMs: step}
}

// stamp hands each point of sql, taken at a tier timestamp, to every timestamp of g that
// snaps down to it.
func stamp(sql string, g Grid, w int64) string {
	step := max(g.StepMs, 1)
	return fmt.Sprintf("WITH %d AS start_ms, %d AS step_ms, %d AS w_ms, "+
		"intDiv(%d - start_ms, step_ms) + 1 AS n_steps "+
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, value FROM ("+
		"SELECT fingerprint, t_ms AS tier_ms, value FROM (%s)) "+
		"ARRAY JOIN range(greatest(0, if(tier_ms >= start_ms, intDiv(tier_ms - start_ms + step_ms - 1, step_ms), "+
		"-intDiv(start_ms - tier_ms, step_ms))), "+
		"least(n_steps - 1, intDiv(tier_ms + w_ms - 1 - start_ms, step_ms)) + 1) AS k "+
		"ORDER BY fingerprint, t_ms",
		g.StartMs, step, w, g.EndMs, sql)
}

// tierRowsSQL is the row shape per (fingerprint, t) from whole buckets inside (t − range, t],
// partial rows merged per bucket, carrying the columns p's function reads. The pair spanning
// two buckets is the previous bucket's last and this bucket's first, counted when the previous
// bucket is in the window. penult is the previous bucket's last, or the bucket's own first when
// it is the window's only bucket; var merges the variance states of every bucket in the window.
// An all-NaN bucket holds min +Inf and max −Inf.
func tierRowsSQL(p Pushdown) string {
	cols := shapeOf(p.Func)
	prevIn, lag, window := "", "", ""
	if cols.prev() {
		prevIn = ", prev_b_ms > 0 AND prev_b_ms > start_ms + k * step_ms - range_ms AS prev_in"
		lag = "lagInFrame(b_last) OVER w AS prev_last, lagInFrame(b_ms) OVER w AS prev_b_ms, "
		window = " WINDOW w AS (PARTITION BY fingerprint ORDER BY bucket ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
	}
	merges := cols.merges()
	return fmt.Sprintf("WITH %d AS start_ms, %d AS end_ms, %d AS step_ms, %d AS range_ms, %d AS w_ms, "+
		"intDiv(end_ms - start_ms, step_ms) + 1 AS n_steps%s "+
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, %s "+
		"FROM (SELECT fingerprint, %s, "+
		"toUnixTimestamp64Milli(bucket) AS b_ms, %s"+
		"greatest(0, if(b_ms >= start_ms, intDiv(b_ms - start_ms + step_ms - 1, step_ms), "+
		"-intDiv(start_ms - b_ms, step_ms))) AS k_min, "+
		"least(n_steps - 1, intDiv(b_ms - w_ms + range_ms - start_ms, step_ms)) AS k_max "+
		"FROM (%s)%s) "+
		"ARRAY JOIN range(k_min, k_max + 1) AS k "+
		"GROUP BY fingerprint, k "+
		"HAVING count > 0",
		p.Grid.StartMs, p.Grid.EndMs, max(p.Grid.StepMs, 1), p.RangeMs, p.Tier.WidthMs, prevIn,
		cols.list(tierColumn), merges.names(), lag, bucketsSQL(p, merges), window)
}

// tierColumn is each column's aggregate over the merged buckets of a window.
var tierColumn = [nColumns]string{
	colFirst:     "(min(b_first.1), argMin(b_first.2, b_first.1)) AS first",
	colLast:      "(max(b_last.1), argMax(b_last.2, b_last.1)) AS last",
	colCount:     "sum(b_count) AS count",
	colSum:       "if(isFinite(sum(b_sum)), sumKahan(b_sum), sum(b_sum)) AS sum",
	colMin:       "if(min(b_min) > max(b_max), nan, min(b_min)) AS min",
	colMax:       "if(min(b_min) > max(b_max), nan, max(b_max)) AS max",
	colResets:    "sum(b_resets) + countIf(prev_in AND b_first.2 < prev_last.2) AS resets",
	colResetDrop: "sum(b_reset_drop) + sumIf(prev_last.2, prev_in AND b_first.2 < prev_last.2) AS reset_drop",
	colChanges: "sum(b_changes) + countIf(prev_in AND b_first.2 != prev_last.2 " +
		"AND NOT (isNaN(b_first.2) AND isNaN(prev_last.2))) AS changes",
	colStaleAt: "max(b_stale_at) AS stale_at",
	colPenult:  "argMax(if(prev_in, prev_last, b_first), b_ms) AS penult",
	colVar:     "varPopStableIfMerge(b_var) AS var",
}

// merge is one per-bucket merge of a tier's partial rows.
type merge int

// The bucket merges, in the order a bucket carries them; b_count is always carried.
const (
	bFirst merge = iota
	bLast
	bCount
	bSum
	bVar
	bMin
	bMax
	bResets
	bResetDrop
	bChanges
	bStaleAt
	nMerges
)

// mergeSQL is each merge with its alias.
var mergeSQL = [nMerges][2]string{
	bFirst:     {"minIfMerge(first)", "b_first"},
	bLast:      {"maxIfMerge(last)", "b_last"},
	bCount:     {"sum(count)", "b_count"},
	bSum:       {"sum(sum)", "b_sum"},
	bVar:       {"varPopStableIfMergeState(var)", "b_var"},
	bMin:       {"minIfMerge(min)", "b_min"},
	bMax:       {"maxIfMerge(max)", "b_max"},
	bResets:    {"sum(resets)", "b_resets"},
	bResetDrop: {"sum(reset_drop)", "b_reset_drop"},
	bChanges:   {"sum(changes)", "b_changes"},
	bStaleAt:   {"max(stale_at)", "b_stale_at"},
}

// columnMerges is the bucket merges each column's tier aggregate reads.
var columnMerges = [nColumns][]merge{
	colFirst:     {bFirst},
	colLast:      {bLast},
	colCount:     {bCount},
	colSum:       {bSum},
	colMin:       {bMin, bMax},
	colMax:       {bMin, bMax},
	colResets:    {bResets, bFirst},
	colResetDrop: {bResetDrop, bFirst},
	colChanges:   {bChanges, bFirst},
	colStaleAt:   {bStaleAt},
	colPenult:    {bFirst},
	colVar:       {bVar},
}

// merges is a set of bucket merges.
type merges [nMerges]bool

// merges is the bucket merges the shape reads; the predecessor is the previous bucket's last.
func (s shape) merges() merges {
	var res merges
	for _, c := range s {
		for _, m := range columnMerges[c] {
			res[m] = true
		}
	}
	if s.prev() {
		res[bLast] = true
	}
	return res
}

func (ms merges) names() string {
	var res []string
	for m, ok := range ms {
		if ok {
			res = append(res, mergeSQL[m][1])
		}
	}
	return strings.Join(res, ", ")
}

func (ms merges) sql() string {
	var res []string
	for m, ok := range ms {
		if ok {
			res = append(res, mergeSQL[m][0]+" AS "+mergeSQL[m][1])
		}
	}
	return strings.Join(res, ", ")
}

// bucketsSQL merges the partial rows of p's tier per (fingerprint, bucket) for the buckets inside
// (start_ms − range_ms, end_ms] that hold a sample.
func bucketsSQL(p Pushdown, ms merges) string {
	return "SELECT fingerprint, bucket, " + ms.sql() + " " +
		"FROM " + p.window().table(p.Tier.table) + " " +
		"WHERE " + seriesIn(p.window(), p.Matchers) + " " +
		"AND bucket > fromUnixTimestamp64Milli(start_ms - range_ms) " +
		"AND bucket <= fromUnixTimestamp64Milli(end_ms) " +
		"GROUP BY fingerprint, bucket " +
		"HAVING b_count > 0"
}

// tierInstantRowsSQL is the instant selector's row shape from a tier: at each t the last of
// the bucket keyed t, when that sample lies inside the lookback (t − range, t].
func tierInstantRowsSQL(p Pushdown) string {
	return fmt.Sprintf("WITH %d AS start_ms, %d AS end_ms, %d AS step_ms, %d AS lookback_ms "+
		"SELECT fingerprint, toUnixTimestamp64Milli(bucket) AS t_ms, b_last AS last, b_stale_at AS stale_at "+
		"FROM (SELECT fingerprint, bucket, maxIfMerge(last) AS b_last, max(stale_at) AS b_stale_at FROM %s "+
		"WHERE %s "+
		"AND bucket >= fromUnixTimestamp64Milli(start_ms) AND bucket <= fromUnixTimestamp64Milli(end_ms) "+
		"AND (toUnixTimestamp64Milli(bucket) - start_ms) %% step_ms = 0 "+
		"GROUP BY fingerprint, bucket) "+
		"WHERE toUnixTimestamp64Milli(last.1) > t_ms - lookback_ms",
		p.Grid.StartMs, p.Grid.EndMs, max(p.Grid.StepMs, 1), p.RangeMs, p.window().table(p.Tier.table),
		seriesIn(p.window(), p.Matchers))
}

// TierSamplesSQL selects what the engine reads of a tier in w: each bucket's last sample at
// its own timestamp, followed by the bucket's latest stale marker when that comes after it.
// Rows: fingerprint UInt64, timestamp DateTime64(3), value Float64, ordered by both.
func TierSamplesSQL(w Window, t Tier, selectors ...[]*labels.Matcher) string {
	return fmt.Sprintf("SELECT fingerprint, if(marker, b_stale_at, b_last.1) AS timestamp, "+
		"if(marker, reinterpretAsFloat64(toUInt64(%d)), b_last.2) AS value "+
		"FROM (SELECT fingerprint, bucket, maxIfMerge(last) AS b_last, max(stale_at) AS b_stale_at FROM %s "+
		"WHERE %s "+
		"AND bucket > fromUnixTimestamp64Milli(%d) "+
		"AND bucket < fromUnixTimestamp64Milli(%d) "+
		"GROUP BY fingerprint, bucket) "+
		"ARRAY JOIN [0, 1] AS marker "+
		"WHERE if(marker, b_stale_at > b_last.1, b_last.1 > toDateTime64(0, 3)) "+
		"AND timestamp > fromUnixTimestamp64Milli(%d) "+
		"AND timestamp <= fromUnixTimestamp64Milli(%d) "+
		"ORDER BY fingerprint, timestamp",
		value.StaleNaN, w.table(t.table), seriesIn(w, selectors...),
		w.FromMs, w.ToMs+t.WidthMs, w.FromMs, w.ToMs)
}
