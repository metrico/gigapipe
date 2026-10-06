package maintenance

import (
	"strconv"
	"time"
)

const (
	day         = 24 * time.Hour
	unitStarted = "started"
	unitDone    = "done"
)

// importUnit is one recorded step of the metric import over (from, to].
type importUnit struct {
	kind     string
	from, to time.Time
}

// importPlan lists the import chunks and the 15s spans, each newest first.
type importPlan struct {
	chunks []importUnit
	spans  []importUnit
}

// importFloor is the lower bound of the raw chunks: the start of the day before the oldest
// raw metric row, no further back than the old table's lifetime, or h0 without such a row.
func importFloor(h0, oldestRaw time.Time, samplesDays int) time.Time {
	if oldestRaw.IsZero() || oldestRaw.After(h0) {
		return h0
	}
	floor := oldestRaw.Add(-time.Millisecond).Truncate(day)
	limit := h0.Truncate(day).Add(-time.Duration(samplesDays+1) * day)
	if floor.Before(limit) {
		return limit
	}
	return floor
}

// planImport plans the raw chunks over (floor, h0], whole days newest first, then (toDate(h0), h0];
// and the 15s spans below floor, weeks newest first, down to the oldest rollup row within its lifetime.
func planImport(h0, floor, oldest15s time.Time, rollupDays int) importPlan {
	var p importPlan
	h0Day := h0.Truncate(day)
	for to := h0Day; to.After(floor); to = to.Add(-day) {
		p.chunks = append(p.chunks, importUnit{kind: "chunk", from: to.Add(-day), to: to})
	}
	if h0.After(h0Day) && floor.Before(h0) {
		p.chunks = append(p.chunks, importUnit{kind: "chunk", from: h0Day, to: h0})
	}
	if oldest15s.IsZero() {
		return p
	}
	if limit := floor.Add(-time.Duration(rollupDays+1) * day); oldest15s.Before(limit) {
		oldest15s = limit
	}
	for to := floor; to.After(oldest15s); to = to.Add(-7 * day) {
		p.spans = append(p.spans, importUnit{kind: "15s", from: to.Add(-7 * day), to: to})
	}
	return p
}

// name is the unit's record name in settings.
func (u importUnit) name() string {
	return u.kind + ":" + strconv.FormatInt(u.to.UnixMilli(), 10)
}

// pendingUnit is a unit still to run; redo marks one recorded started but not done.
type pendingUnit struct {
	unit importUnit
	redo bool
}

// pendingUnits lists the units not recorded done, in order.
func pendingUnits(units []importUnit, records map[string]string) []pendingUnit {
	var out []pendingUnit
	for _, u := range units {
		switch records[u.name()] {
		case unitDone:
		case unitStarted:
			out = append(out, pendingUnit{unit: u, redo: true})
		default:
			out = append(out, pendingUnit{unit: u})
		}
	}
	return out
}
