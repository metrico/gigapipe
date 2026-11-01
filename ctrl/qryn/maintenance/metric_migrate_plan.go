package maintenance

import (
	"strconv"
	"time"
)

const unitRaw = "raw"

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

// planMigration plans the raw hours (H, H+1h] from the last hour boundary at or before
// now − margin down to the floor: each hour not recorded done, and each hour ending after
// watermark − lookback. The floor is the recorded one, else the hour of now − samplesDays.
func planMigration(now time.Time, samplesDays int, margin, lookback time.Duration, records map[string]string) migrationPlan {
	p := migrationPlan{top: now.UTC().Add(-margin).Truncate(time.Hour)}
	floor, ok := recordedTime(records, floorRecord)
	if !ok {
		floor = now.UTC().Add(-time.Duration(samplesDays) * 24 * time.Hour).Truncate(time.Hour)
	}
	p.floor = floor
	watermark, rerun := recordedTime(records, watermarkRecord)
	if rerun {
		p.watermark = watermark
	}
	for to := p.top; to.After(floor); to = to.Add(-time.Hour) {
		u := migrationUnit{kind: unitRaw, from: to.Add(-time.Hour), to: to}
		state := records[u.name()]
		if state == unitDone && !(rerun && to.After(watermark.Add(-lookback))) {
			continue
		}
		u.recovery = state == unitStarted
		p.units = append(p.units, u)
	}
	return p
}
