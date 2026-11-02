package maintenance

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	unitRaw = "raw"
	unit15s = "15s"
)

// migrationUnit is one hour (from, to] of a kind; recovery marks one recorded started, and
// rawRecord a 15s unit recorded under the hour's raw record.
type migrationUnit struct {
	kind      string
	from, to  time.Time
	recovery  bool
	rawRecord bool
}

// name is the unit's record name in settings.
func (u migrationUnit) name() string {
	kind := u.kind
	if u.rawRecord {
		kind = unitRaw
	}
	return kind + ":" + strconv.FormatInt(u.to.UnixMilli(), 10)
}

// migrationPlan is one run's units, newest first, between floor and top; the raw units end
// above edge.
type migrationPlan struct {
	floor, edge, top time.Time
	// watermark is the recorded top of an earlier run, zero on the first run.
	watermark time.Time
	units     []migrationUnit
}

func recordedTime(records map[string]string, name string) (time.Time, bool) {
	v, err := strconv.ParseInt(records[name], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(v).UTC(), true
}

// rollupHistory is metrics_15s's metric-row hours below the raw edge, by their ends; lifetime is
// the hour the table's rows start at or after, zero for none.
type rollupHistory struct {
	lifetime time.Time
	ends     []time.Time
}

// hourBefore is the hour of now − days.
func hourBefore(now time.Time, days int) time.Time {
	return now.UTC().Add(-time.Duration(days) * 24 * time.Hour).Truncate(time.Hour)
}

// planFloor is the recorded floor, else the hour of now − samplesDays.
func planFloor(now time.Time, samplesDays int, records map[string]string) time.Time {
	if floor, ok := recordedTime(records, floorRecord); ok {
		return floor
	}
	return hourBefore(now, samplesDays)
}

// planEdge is the hour 25h above now − rawDays, at least floor.
func planEdge(now time.Time, rawDays int, floor time.Time) time.Time {
	edge := hourBefore(now, rawDays).Add(25 * time.Hour)
	if edge.Before(floor) {
		return floor
	}
	return edge
}

// planMigration plans the hours (H, H+1h] newest first from now − margin down to the floor: raw
// above the edge; below it, 15s where rollup lists the hour or a 15s unit was started, else raw,
// skipping hours done of either kind; then the 15s hours of rollup at or below the floor.
func planMigration(now time.Time, samplesDays, rawDays int, margin, lookback time.Duration, records map[string]string, rollup *rollupHistory) migrationPlan {
	p := migrationPlan{top: now.UTC().Add(-margin).Truncate(time.Hour), floor: planFloor(now, samplesDays, records)}
	p.edge = planEdge(now, rawDays, p.floor)
	watermark, rerun := recordedTime(records, watermarkRecord)
	if rerun {
		p.watermark = watermark
	}
	add := func(u migrationUnit) {
		state := records[u.name()]
		if state == unitDone && !(rerun && u.to.After(watermark.Add(-lookback))) {
			return
		}
		u.recovery = state == unitStarted
		p.units = append(p.units, u)
	}
	listed := map[int64]bool{}
	if rollup != nil {
		for _, to := range rollup.ends {
			listed[to.UnixMilli()] = true
		}
	}
	for to := p.top; to.After(p.floor); to = to.Add(-time.Hour) {
		u := migrationUnit{kind: unitRaw, from: to.Add(-time.Hour), to: to}
		if to.After(p.edge) {
			add(u)
			continue
		}
		raw, r15s := records[u.name()], records[migrationUnit{kind: unit15s, to: to}.name()]
		switch {
		case raw == unitDone || r15s == unitDone:
			continue
		case rollup != nil && r15s == unitStarted, listed[to.UnixMilli()]:
			u.kind = unit15s
			u.recovery = r15s == unitStarted || raw == unitStarted
			u.rawRecord = r15s != unitStarted && raw == unitStarted
		default:
			u.recovery = raw == unitStarted
		}
		p.units = append(p.units, u)
	}
	if rollup == nil {
		return p
	}
	ends := slices.Clone(rollup.ends)
	for name, state := range records {
		if to, ok := strings.CutPrefix(name, unit15s+":"); ok && state == unitStarted {
			if v, err := strconv.ParseInt(to, 10, 64); err == nil {
				ends = append(ends, time.UnixMilli(v).UTC())
			}
		}
	}
	slices.SortFunc(ends, func(a, b time.Time) int { return b.Compare(a) })
	ends = slices.CompactFunc(ends, time.Time.Equal)
	for _, to := range ends {
		if to.After(p.floor) || !to.After(rollup.lifetime) {
			continue
		}
		add(migrationUnit{kind: unit15s, from: to.Add(-time.Hour), to: to.UTC()})
	}
	return p
}
