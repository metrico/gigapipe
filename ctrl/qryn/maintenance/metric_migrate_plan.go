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

// migrationUnit is one hour (from, to] of a kind; recovery marks one recorded started.
type migrationUnit struct {
	kind     string
	from, to time.Time
	recovery bool
}

// name is the unit's record name in settings.
func (u migrationUnit) name() string {
	return u.kind + ":" + strconv.FormatInt(u.to.UnixMilli(), 10)
}

// migrationPlan is one run's units, newest first, between floor and top.
type migrationPlan struct {
	floor, top time.Time
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

// rollupHistory is metrics_15s's metric-row hours below the floor, by their ends; lifetime is
// the hour the table's rows start after, zero for none.
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

// planMigration plans the raw hours (H, H+1h] from the last hour boundary at or before
// now − margin down to the floor, then the 15s hours of rollup and those recorded started,
// at or below the floor and ending after rollup.lifetime. Of each kind it plans every hour
// not recorded done and every hour ending after watermark − lookback, newest first.
func planMigration(now time.Time, samplesDays int, margin, lookback time.Duration, records map[string]string, rollup *rollupHistory) migrationPlan {
	p := migrationPlan{top: now.UTC().Add(-margin).Truncate(time.Hour), floor: planFloor(now, samplesDays, records)}
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
	for to := p.top; to.After(p.floor); to = to.Add(-time.Hour) {
		add(migrationUnit{kind: unitRaw, from: to.Add(-time.Hour), to: to})
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
