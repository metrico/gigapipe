package metricread

import (
	"time"

	"github.com/metrico/qryn/v5/shared/metricretention"
)

// Tier is a table PromQL reads: the raw samples, or an aggregate tier of buckets WidthMs wide.
type Tier struct {
	Name    string
	table   string
	WidthMs int64
}

var (
	RawTier = Tier{Name: "raw", table: "metric_samples"}
	Tier5m  = Tier{Name: "5m", table: "metrics_5m", WidthMs: 300000}
	Tier1h  = Tier{Name: "1h", table: "metrics_1h", WidthMs: 3600000}
)

// TierNamed returns the tier GIGAPIPE_METRICS_READ_TIER names: raw, 5m or 1h.
func TierNamed(name string) (Tier, bool) {
	for _, t := range []Tier{RawTier, Tier5m, Tier1h} {
		if t.Name == name {
			return t, true
		}
	}
	return Tier{}, false
}

// Read is what tier selection knows of a PromQL query.
type Read struct {
	Grid Grid
	// EarliestMs is start − lookback: the earliest instant the query reads.
	EarliestMs int64
	// RangesMs holds the range of every pushed-down range function.
	RangesMs []int64
	// EngineReads is set when the engine reads a selector itself.
	EngineReads bool
}

// SelectTier picks the one tier a query is served from. A forced tier
// (GIGAPIPE_METRICS_READ_TIER) serves every read. Inside raw's lifetime the 5m tier serves an
// aligned read and raw the rest; past it, the finest tier whose lifetime covers the earliest read serves, and past the
// 1h tier's lifetime the 1h tier serves what it still holds.
func SelectTier(r Read, lifetimes metricretention.Tiers, now time.Time, forced string) Tier {
	if t, ok := TierNamed(forced); ok {
		return t
	}
	covers := func(days int) bool {
		return r.EarliestMs >= now.Add(-time.Duration(days)*24*time.Hour).UnixMilli()
	}
	switch {
	case covers(lifetimes.RawDays):
		// An hour-aligned read is 5m-aligned too, and the 5m tier outlives raw.
		if aligned(r, Tier5m) {
			return Tier5m
		}
		return RawTier
	case covers(lifetimes.FiveMinuteDays):
		return Tier5m
	default:
		return Tier1h
	}
}

// aligned reports whether r is an aligned read for t: every evaluation timestamp on t's grid,
// every range a multiple of its width and every selector pushed down.
func aligned(r Read, t Tier) bool {
	if r.EngineReads || r.Grid.StartMs%t.WidthMs != 0 || r.Grid.StepMs%t.WidthMs != 0 {
		return false
	}
	for _, rng := range r.RangesMs {
		if rng%t.WidthMs != 0 {
			return false
		}
	}
	return true
}
