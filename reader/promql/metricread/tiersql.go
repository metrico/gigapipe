package metricread

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
)

// tierPushdownSQL evaluates p from p.Tier's buckets. Each evaluation timestamp is snapped down
// to the tier grid and each range widened to whole buckets, at least one; each point is
// stamped at the query's own timestamp. An aligned read is evaluated as it stands.
func tierPushdownSQL(p Pushdown) string {
	w := p.Tier.WidthMs
	q := p
	q.Grid = snap(p.Grid, w)
	rows := tierInstantRowsSQL(q)
	if q.Func != "" {
		q.RangeMs = max(w, (p.RangeMs+w-1)/w*w)
		rows = tierRowsSQL(q)
	}
	sql := pushdownSQL(q, rows)
	if q.Grid == p.Grid {
		return sql
	}
	return stamp(sql, p.Grid, w)
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
		"SELECT fingerprint, labels, start_ms + k * step_ms AS t_ms, value FROM ("+
		"SELECT fingerprint, labels, t_ms AS tier_ms, value FROM (%s)) "+
		"ARRAY JOIN range(greatest(0, if(tier_ms >= start_ms, intDiv(tier_ms - start_ms + step_ms - 1, step_ms), "+
		"-intDiv(start_ms - tier_ms, step_ms))), "+
		"least(n_steps - 1, intDiv(tier_ms + w_ms - 1 - start_ms, step_ms)) + 1) AS k "+
		"ORDER BY fingerprint, t_ms",
		g.StartMs, step, w, g.EndMs, sql)
}

// tierRowsSQL is the row shape per (fingerprint, t) from whole buckets inside (t − range, t],
// partial rows merged per bucket. The pair spanning two buckets is the previous bucket's last
// and this bucket's first, counted when the previous bucket is in the window. penult is the
// previous bucket's last, or the bucket's own first when it is the window's only bucket; var
// merges the variance states of every bucket in the window. An all-NaN bucket holds min +Inf
// and max −Inf.
func tierRowsSQL(p Pushdown) string {
	return fmt.Sprintf("WITH %d AS start_ms, %d AS end_ms, %d AS step_ms, %d AS range_ms, %d AS w_ms, "+
		"intDiv(end_ms - start_ms, step_ms) + 1 AS n_steps, "+
		"prev_b_ms > 0 AND prev_b_ms > start_ms + k * step_ms - range_ms AS prev_in "+
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, "+
		"min(b_first) AS first, "+
		"max(b_last) AS last, "+
		"sum(b_count) AS count, "+
		"if(isFinite(sum(b_sum)), sumKahan(b_sum), sum(b_sum)) AS sum, "+
		"if(min(b_min) > max(b_max), nan, min(b_min)) AS min, "+
		"if(min(b_min) > max(b_max), nan, max(b_max)) AS max, "+
		"sum(b_resets) + countIf(prev_in AND b_first.2 < prev_last.2) AS resets, "+
		"sum(b_reset_drop) + sumIf(prev_last.2, prev_in AND b_first.2 < prev_last.2) AS reset_drop, "+
		"sum(b_changes) + countIf(prev_in AND b_first.2 != prev_last.2 "+
		"AND NOT (isNaN(b_first.2) AND isNaN(prev_last.2))) AS changes, "+
		"max(b_stale_at) AS stale_at, "+
		"argMax(if(prev_in, prev_last, b_first), b_ms) AS penult, "+
		"varPopStableIfMerge(b_var) AS var "+
		"FROM (SELECT fingerprint, b_first, b_last, b_count, b_sum, b_var, b_min, b_max, "+
		"b_resets, b_reset_drop, b_changes, b_stale_at, "+
		"toUnixTimestamp64Milli(bucket) AS b_ms, "+
		"lagInFrame(b_last) OVER w AS prev_last, "+
		"lagInFrame(b_ms) OVER w AS prev_b_ms, "+
		"greatest(0, if(b_ms >= start_ms, intDiv(b_ms - start_ms + step_ms - 1, step_ms), "+
		"-intDiv(start_ms - b_ms, step_ms))) AS k_min, "+
		"least(n_steps - 1, intDiv(b_ms - w_ms + range_ms - start_ms, step_ms)) AS k_max "+
		"FROM (%s) "+
		"WINDOW w AS (PARTITION BY fingerprint ORDER BY bucket ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) "+
		"ARRAY JOIN range(k_min, k_max + 1) AS k "+
		"GROUP BY fingerprint, k "+
		"HAVING count > 0",
		p.Grid.StartMs, p.Grid.EndMs, max(p.Grid.StepMs, 1), p.RangeMs, p.Tier.WidthMs, bucketsSQL(p.Tier))
}

// bucketsSQL merges the partial rows of t per (fingerprint, bucket) for the buckets inside
// (start_ms − range_ms, end_ms] that hold a sample.
func bucketsSQL(t Tier) string {
	return "SELECT fingerprint, bucket, " +
		"minIfMerge(first) AS b_first, maxIfMerge(last) AS b_last, " +
		"sum(count) AS b_count, sum(sum) AS b_sum, varPopStableIfMergeState(var) AS b_var, " +
		"minIfMerge(min) AS b_min, maxIfMerge(max) AS b_max, " +
		"sum(resets) AS b_resets, sum(reset_drop) AS b_reset_drop, sum(changes) AS b_changes, " +
		"max(stale_at) AS b_stale_at " +
		"FROM " + tables.GetTableName(t.table) + " " +
		"WHERE fingerprint IN (SELECT fingerprint FROM fp) " +
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
		"WHERE fingerprint IN (SELECT fingerprint FROM fp) "+
		"AND bucket >= fromUnixTimestamp64Milli(start_ms) AND bucket <= fromUnixTimestamp64Milli(end_ms) "+
		"AND (toUnixTimestamp64Milli(bucket) - start_ms) %% step_ms = 0 "+
		"GROUP BY fingerprint, bucket) "+
		"WHERE toUnixTimestamp64Milli(last.1) > t_ms - lookback_ms",
		p.Grid.StartMs, p.Grid.EndMs, max(p.Grid.StepMs, 1), p.RangeMs, tables.GetTableName(p.Tier.table))
}

// TierSamplesSQL selects what the engine reads of a tier in w: each bucket's last sample at
// its own timestamp, followed by the bucket's latest stale marker when that comes after it.
// Rows: fingerprint UInt64, timestamp DateTime64(3), value Float64, ordered by both.
func TierSamplesSQL(w Window, t Tier, selectors ...[]*labels.Matcher) string {
	return fmt.Sprintf("WITH fp AS (%s) "+
		"SELECT fingerprint, if(marker, b_stale_at, b_last.1) AS timestamp, "+
		"if(marker, reinterpretAsFloat64(toUInt64(%d)), b_last.2) AS value "+
		"FROM (SELECT fingerprint, bucket, maxIfMerge(last) AS b_last, max(stale_at) AS b_stale_at FROM %s "+
		"WHERE fingerprint IN (SELECT fingerprint FROM fp) "+
		"AND bucket > fromUnixTimestamp64Milli(%d) "+
		"AND bucket < fromUnixTimestamp64Milli(%d) "+
		"GROUP BY fingerprint, bucket) "+
		"ARRAY JOIN [0, 1] AS marker "+
		"WHERE if(marker, b_stale_at > b_last.1, b_last.1 > toDateTime64(0, 3)) "+
		"AND timestamp > fromUnixTimestamp64Milli(%d) "+
		"AND timestamp <= fromUnixTimestamp64Milli(%d) "+
		"ORDER BY fingerprint, timestamp",
		SeriesSQL(w, selectors...), value.StaleNaN, tables.GetTableName(t.table),
		w.FromMs, w.ToMs+t.WidthMs, w.FromMs, w.ToMs)
}
