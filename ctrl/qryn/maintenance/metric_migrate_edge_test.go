package maintenance

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// edgeNow with samplesDays = rawDays = 2 puts the floor at 10-01 09:00 and the raw edge at
// 10-02 10:00.
const edgeNow = "2026-10-03 09:10"

func names(units []migrationUnit) []string {
	var out []string
	for _, u := range units {
		out = append(out, u.name())
	}
	return out
}

func TestTheRawEdgeIsADayAboveTheRawLifetimeAndNeverBelowTheFloor(t *testing.T) {
	plan := planMigration(at(edgeNow), 2, 2, 5*time.Minute, time.Hour, nil, nil)
	if !plan.floor.Equal(at("2026-10-01 09:00")) || !plan.edge.Equal(at("2026-10-02 10:00")) {
		t.Errorf("floor = %s, edge = %s", plan.floor, plan.edge)
	}
	plan = planMigration(at(edgeNow), 2, 4, 5*time.Minute, time.Hour, nil, nil)
	if !plan.edge.Equal(plan.floor) {
		t.Errorf("edge = %s, want the floor %s", plan.edge, plan.floor)
	}
}

func TestTheHoursBelowTheEdgeHeldByMetrics15sAre15sUnitsAfterTheRawOnes(t *testing.T) {
	rollup := &rollupHistory{lifetime: at("2026-10-01 07:00"), ends: ends15s(
		"2026-10-02 10:00", "2026-10-02 09:00", "2026-10-01 10:00", "2026-10-01 08:00")}
	plan := planMigration(at(edgeNow), 2, 2, 5*time.Minute, time.Hour, nil, rollup)
	got := names(plan.units)
	if len(got) != 23+25+1 {
		t.Fatalf("%d units: %q", len(got), got)
	}
	sameStrings(t, "raw", got[21:23], []string{rawRecord("2026-10-02 12:00"), rawRecord("2026-10-02 11:00")})
	sameStrings(t, "below the edge", got[23:26], []string{
		rollupRecord("2026-10-02 10:00"), rollupRecord("2026-10-02 09:00"), rawRecord("2026-10-02 08:00"),
	})
	sameStrings(t, "oldest", got[46:], []string{
		rawRecord("2026-10-01 11:00"), rollupRecord("2026-10-01 10:00"), rollupRecord("2026-10-01 08:00"),
	})
}

func TestAnEdgeAtTheFloorPlansAsWithoutOne(t *testing.T) {
	rollup := &rollupHistory{ends: ends15s("2026-10-02 10:00", "2026-10-01 08:00")}
	plan := planMigration(at(edgeNow), 2, 4, 5*time.Minute, time.Hour, nil, rollup)
	got := names(plan.units)
	if len(got) != 49 {
		t.Fatalf("%d units: %q", len(got), got)
	}
	sameStrings(t, "oldest", got[47:], []string{rawRecord("2026-10-01 10:00"), rollupRecord("2026-10-01 08:00")})
}

func TestALaterRunPlansNothingBelowTheEdgeDoneOfEitherKind(t *testing.T) {
	records := map[string]string{
		floorRecord:     ms(at("2026-10-01 09:00")),
		watermarkRecord: ms(at("2026-10-03 09:00")),
	}
	for to := at("2026-10-01 10:00"); !to.After(at("2026-10-03 09:00")); to = to.Add(time.Hour) {
		records["raw:"+ms(to)] = unitDone
	}
	records[rollupRecord("2026-10-01 10:00")] = unitDone
	records[rawRecord("2026-10-01 10:00")] = ""
	rollup := &rollupHistory{ends: ends15s("2026-10-02 12:00", "2026-10-01 10:00")}
	plan := planMigration(at("2026-10-04 09:10"), 2, 2, 5*time.Minute, 48*time.Hour, records, rollup)
	if !plan.edge.Equal(at("2026-10-03 10:00")) {
		t.Fatalf("edge = %s", plan.edge)
	}
	for _, u := range plan.units {
		if !u.to.After(at("2026-10-03 09:00")) {
			t.Errorf("planned %s, done below the edge %s", u.name(), plan.edge)
		}
	}
}

func TestAStartedRawHourBelowTheEdgeRecoversAs15sUnderItsRawRecord(t *testing.T) {
	records := map[string]string{
		floorRecord:                   ms(at("2026-10-01 09:00")),
		rawRecord("2026-10-02 09:00"): unitStarted,
		rawRecord("2026-10-02 08:00"): unitStarted,
	}
	rollup := &rollupHistory{ends: ends15s("2026-10-02 09:00")}
	plan := planMigration(at(edgeNow), 2, 2, 5*time.Minute, time.Hour, records, rollup)
	var got []migrationUnit
	for _, u := range plan.units {
		if u.recovery {
			got = append(got, u)
		}
	}
	sameStrings(t, "recoveries", names(got), []string{rawRecord("2026-10-02 09:00"), rawRecord("2026-10-02 08:00")})
	sameStrings(t, "kinds", unitKinds15s(got), []string{unit15s, unitRaw})
}

func TestAStarted15sHourBelowTheEdgeRecoversAs15sWithoutMetrics15sRows(t *testing.T) {
	records := map[string]string{
		floorRecord:                      ms(at("2026-10-01 09:00")),
		rollupRecord("2026-10-02 09:00"): unitStarted,
	}
	plan := planMigration(at(edgeNow), 2, 2, 5*time.Minute, time.Hour, records, &rollupHistory{})
	for _, u := range plan.units {
		if u.to.Equal(at("2026-10-02 09:00")) && (u.kind != unit15s || !u.recovery) {
			t.Errorf("unit = %+v", u)
		}
	}
}

func TestAStartedRawHourBelowTheEdgeRebuildsItsTiersFromMetrics15s(t *testing.T) {
	f, opts := newRun("2026-10-01 08:00")
	opts.RawDays = 2
	with15s(f, 240, "2026-10-01 10:00", "2026-10-01 09:00")
	name := rawRecord("2026-10-01 09:00")
	f.record(name, unitStarted)
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	u := migrationUnit{kind: unit15s, from: at("2026-10-01 08:00"), to: at("2026-10-01 09:00")}
	raw := u
	raw.kind = unitRaw
	var steps []string
	for _, s := range f.statements() {
		if s.QueryID == unitQueryID("cloki", raw) {
			t.Errorf("raw statement %s", step(s))
		}
		if s.QueryID == unitQueryID("cloki", u) || (s.Op == opRecord && (s.Args[2] == name || s.Args[2] == rollupRecord("2026-10-01 09:00"))) {
			steps = append(steps, step(s))
		}
	}
	sameStrings(t, "steps", steps, []string{
		"record " + name + "=started",
		"count metrics_15s",
		"count metrics_5m",
		"delete metrics_5m",
		"rebuild metrics_5m",
		"count metrics_1h",
		"delete metrics_1h",
		"rebuild metrics_1h",
		"check metrics_5m",
		"check metrics_1h",
		"record " + rollupRecord("2026-10-01 09:00") + "=done",
		"record " + name + "=done",
	})
	if f.value(name) != unitDone || f.value(rollupRecord("2026-10-01 09:00")) != unitDone {
		t.Errorf("records: %s = %q, 15s = %q", name, f.value(name), f.value(rollupRecord("2026-10-01 09:00")))
	}
	if f.value(rollupRecord("2026-10-01 10:00")) != unitDone || report.Recovered != 1 || report.Rollup15sSamples != 480 {
		t.Errorf("report = %+v", report)
	}
	edge := at("2026-10-01 10:00")
	for _, s := range f.statements() {
		if s.Op == op15sHours && !strings.Contains(s.SQL, fmt.Sprintf("timestamp_ns < %d", edge.UnixNano())) {
			t.Errorf("hours query not bounded by the edge: %s", s.SQL)
		}
	}
}

func TestARecovered15sUnitWithNoSourceRowsDeletesItsTierBuckets(t *testing.T) {
	f, opts := newRun("2026-10-01 08:00")
	opts.RawDays = 2
	with15s(f, 240, "2026-10-01 10:00", "2026-10-01 09:00")
	u := migrationUnit{kind: unit15s, from: at("2026-10-01 08:00"), to: at("2026-10-01 09:00")}
	answer := f.on
	f.on = func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opCount && s.Table == legacyRollup && s.QueryID == unitQueryID("cloki", u) {
			return uint64(0), nil
		}
		return answer(ctx, s)
	}
	name := rawRecord("2026-10-01 09:00")
	f.record(name, unitStarted)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	var steps []string
	for _, s := range f.statements() {
		if s.QueryID == unitQueryID("cloki", u) || (s.Op == opRecord && (s.Args[2] == name || s.Args[2] == u.name())) {
			steps = append(steps, step(s))
		}
	}
	sameStrings(t, "steps", steps, []string{
		"record " + name + "=started",
		"count metrics_15s",
		"count metrics_5m",
		"delete metrics_5m",
		"count metrics_1h",
		"delete metrics_1h",
		"record " + u.name() + "=done",
		"record " + name + "=done",
	})
}

var (
	sqlTimeBound = regexp.MustCompile(`timestamp_ns (>=|>|<=|<) (\d+)`)
	sqlSampleAt  = regexp.MustCompile(`fromUnixTimestamp64Milli\(intDiv\(timestamp_ns, 1000000\) \+ (\d+)\) AS timestamp`)
	sqlHourOf    = regexp.MustCompile(`intDiv\(timestamp_ns, (\d+)\) \+ 1`)
)

// sqlRows is the conjunction of the timestamp_ns bounds in sql.
func sqlRows(sql string) func(ns int64) bool {
	bounds := sqlTimeBound.FindAllStringSubmatch(sql, -1)
	return func(ns int64) bool {
		for _, b := range bounds {
			v, _ := strconv.ParseInt(b[2], 10, 64)
			if !map[string]bool{">=": ns >= v, ">": ns > v, "<=": ns <= v, "<": ns < v}[b[1]] {
				return false
			}
		}
		return true
	}
}

// sourceSample is where the 15s unit's source SQL places the row at ts, and whether it reads it.
func sourceSample(u migrationUnit) func(ts time.Time) (time.Time, bool) {
	sql := localTables.source15sSQL(u, 1, 0)
	reads := sqlRows(sql)
	m := sqlSampleAt.FindStringSubmatch(sql)
	if m == nil {
		panic("no sample timestamp in " + sql)
	}
	shift, _ := strconv.ParseInt(m[1], 10, 64)
	return func(ts time.Time) (time.Time, bool) {
		if !reads(ts.UnixNano()) {
			return time.Time{}, false
		}
		return ts.Truncate(time.Millisecond).Add(time.Duration(shift) * time.Millisecond), true
	}
}

// rawCovers is whether the raw unit's copy holds the samples just after the row at ts starts.
func rawCovers(u migrationUnit) func(ts time.Time) bool {
	copies := sqlRows(localTables.copyRawSQL(u, 1, 0))
	return func(ts time.Time) bool { return copies(ts.Add(time.Millisecond).UnixNano()) }
}

// listed is the hours the listing SQL returns for lifetime and edge over the rows at the 15s
// steps from lo, newest first.
func listed(rows func(time.Time) bool, lo, lifetime, edge time.Time) []time.Time {
	sql := localTables.rollup15sHoursSQL(lifetime, edge)
	m := sqlHourOf.FindStringSubmatch(sql)
	width, _ := strconv.ParseInt(m[1], 10, 64)
	lists := sqlRows(sql)
	var ends []time.Time
	for ts := lo; !ts.After(edge); ts = ts.Add(15 * time.Second) {
		ns := ts.UnixNano()
		if !rows(ts) || !lists(ns) {
			continue
		}
		end := time.Unix(0, (ns/width+1)*width).UTC()
		if len(ends) == 0 || !ends[0].Equal(end) {
			ends = append([]time.Time{end}, ends...)
		}
	}
	return ends
}

// migrationRun is one run at now over the metrics_15s rows present then, whose lifetime is
// rollupDays days when set; the unit named fail fails it.
type migrationRun struct {
	now        string
	rows       func(time.Time) bool
	fail       string
	rollupDays int
}

// lifetime is the oldest hour metrics_15s keeps at the run, or zero without rollupDays.
func (r migrationRun) lifetime() time.Time {
	if r.rollupDays == 0 {
		return time.Time{}
	}
	return hourBefore(at(r.now), r.rollupDays)
}

// doneUnit is a unit recorded done: the raw copy's coverage, or the 15s source's reads over the
// rows present when it ran.
type doneUnit struct {
	migrationUnit
	covers func(time.Time) bool
	reads  func(time.Time) (time.Time, bool)
}

func newDoneUnit(u migrationUnit, rows func(time.Time) bool) doneUnit {
	if u.kind == unitRaw {
		return doneUnit{migrationUnit: u, covers: rawCovers(u)}
	}
	source := sourceSample(u)
	return doneUnit{migrationUnit: u, reads: func(ts time.Time) (time.Time, bool) {
		if !rows(ts) {
			return time.Time{}, false
		}
		return source(ts)
	}}
}

// planRuns runs each run's plan in order against records, from samplesDays = 2. A unit records
// started then done; a failed one stops its run before the watermark. It returns the last unit
// done for each hour, whose tiers are the hour's final ones, and the last run's top.
func planRuns(rawDays int, margin, lookback time.Duration, records map[string]string, runs ...migrationRun) (map[int64]doneUnit, time.Time) {
	final := map[int64]doneUnit{}
	for name, state := range records {
		if to, ok := strings.CutPrefix(name, unitRaw+":"); ok && state == unitDone {
			v, _ := strconv.ParseInt(to, 10, 64)
			u := migrationUnit{kind: unitRaw, from: time.UnixMilli(v).UTC().Add(-time.Hour), to: time.UnixMilli(v).UTC()}
			final[v] = newDoneUnit(u, nil)
		}
	}
	var top time.Time
	for _, r := range runs {
		floor := planFloor(at(r.now), 2, records)
		edge := planEdge(at(r.now), rawDays, floor)
		lo, lifetime := floor.Add(-4*time.Hour), r.lifetime()
		if !lifetime.IsZero() {
			lo = lifetime
		}
		rollup := &rollupHistory{lifetime: lifetime, ends: listed(r.rows, lo, lifetime, edge)}
		plan := planMigration(at(r.now), 2, rawDays, margin, lookback, records, rollup)
		records[floorRecord] = ms(plan.floor)
		top = plan.top
		failed := false
		for _, u := range plan.units {
			records[u.name()] = unitStarted
			if u.name() == r.fail {
				failed = true
				break
			}
			if u.rawRecord {
				records[migrationUnit{kind: unit15s, to: u.to}.name()] = unitDone
			}
			records[u.name()] = unitDone
			final[u.to.UnixMilli()] = newDoneUnit(u, r.rows)
		}
		if !failed {
			records[watermarkRecord] = ms(plan.top)
		}
	}
	return final, top
}

// checkRowOwners checks that each row from lo to hi is read by exactly one final 15s unit, as a
// sample in that unit's hour, or is covered by exactly one final raw unit.
func checkRowOwners(t *testing.T, final map[int64]doneUnit, rows func(time.Time) bool, lo, hi time.Time) {
	t.Helper()
	for ts := lo; !ts.After(hi); ts = ts.Add(15 * time.Second) {
		if !rows(ts) {
			continue
		}
		var owners []string
		for _, u := range final {
			if u.kind == unitRaw {
				if u.covers(ts) {
					owners = append(owners, u.name())
				}
				continue
			}
			sample, ok := u.reads(ts)
			if !ok {
				continue
			}
			owners = append(owners, u.name())
			if !sample.After(u.from) || sample.After(u.to) {
				t.Errorf("row at %s becomes a sample at %s, outside %s", ts.Format("01-02 15:04:05"), sample.Format("15:04:05.000"), u.name())
			}
		}
		if len(owners) != 1 {
			t.Errorf("row at %s has owners %v, want one", ts.Format("01-02 15:04:05"), owners)
		}
	}
}

func TestEach15sRowLandsInExactlyOneUnitAcrossRuns(t *testing.T) {
	all := func(time.Time) bool { return true }
	// except has no rows in the hours [g − 1h, g) of gaps.
	except := func(gaps ...string) func(time.Time) bool {
		return func(ts time.Time) bool {
			for _, g := range gaps {
				if !ts.Before(at(g).Add(-time.Hour)) && ts.Before(at(g)) {
					return false
				}
			}
			return true
		}
	}
	upTo := func(last string) func(time.Time) bool {
		return func(ts time.Time) bool { return !ts.After(at(last)) }
	}
	hourly := func(ts time.Time) bool { return ts.Truncate(time.Hour).Equal(ts) }
	runs := func(rows func(time.Time) bool, nows ...string) []migrationRun {
		var out []migrationRun
		for _, now := range nows {
			out = append(out, migrationRun{now: now, rows: rows})
		}
		return out
	}
	for _, c := range []struct {
		name     string
		rawDays  int
		margin   time.Duration
		lookback time.Duration
		records  map[string]string
		runs     []migrationRun
	}{
		{"single run, top at the edge", 2, 22*time.Hour + 50*time.Minute, 0, nil, runs(all, "2026-10-03 09:05")},
		{"top at the edge, then above it within the hour", 2, 22*time.Hour + 50*time.Minute, 0, nil,
			runs(all, "2026-10-03 09:05", "2026-10-03 09:55")},
		{"top at the edge, then an hour later", 2, 22*time.Hour + 50*time.Minute, 0, nil,
			runs(all, "2026-10-03 09:05", "2026-10-03 10:05", "2026-10-03 10:55")},
		{"top at the edge, then a day later", 2, 22*time.Hour + 50*time.Minute, 0, nil,
			runs(all, "2026-10-03 09:05", "2026-10-04 09:05")},
		{"raw edge hour at the top", 2, 22*time.Hour + 50*time.Minute, 0, nil,
			runs(except("2026-10-02 10:00"), "2026-10-03 09:05", "2026-10-03 10:05")},
		{"raw above the edge, then a day later", 2, 5 * time.Minute, 0, nil,
			runs(all, "2026-10-03 09:10", "2026-10-04 09:10")},
		{"raw above the edge, then within the hour", 2, 5 * time.Minute, 0, nil,
			runs(all, "2026-10-03 09:10", "2026-10-03 09:50")},
		{"top below the edge, then a day later", 1, 5 * time.Minute, 0, nil,
			runs(all, "2026-10-03 09:10", "2026-10-04 09:10")},
		{"top below the edge with a gap above it", 1, 5 * time.Minute, 0, nil,
			runs(except("2026-10-03 10:00"), "2026-10-03 09:10", "2026-10-04 09:10")},
		{"recoveries", 2, 22*time.Hour + 50*time.Minute, 0, map[string]string{
			rawRecord("2026-10-02 08:00"): unitStarted, rollupRecord("2026-10-02 06:00"): unitStarted,
			rawRecord("2026-10-02 04:00"): unitStarted},
			runs(except("2026-10-02 04:00"), "2026-10-03 09:05", "2026-10-03 10:05")},
		{"gaps at the floor", 2, 5 * time.Minute, 0, nil,
			runs(except("2026-10-01 10:00", "2026-10-01 08:00"), "2026-10-03 09:10")},
		{"raw done below 15s hours", 2, 5 * time.Minute, 0, map[string]string{
			floorRecord: ms(at("2026-10-01 09:00")), rawRecord("2026-10-02 07:00"): unitDone},
			runs(all, edgeNow)},
		{"a lone row at an hour's start below the floor", 2, 5 * time.Minute, 0, nil, runs(func(ts time.Time) bool {
			return ts.Equal(at("2026-10-01 07:00")) || !ts.Before(at("2026-10-01 09:00"))
		}, edgeNow)},
		{"rows arriving above a top below the edge", 1, 5 * time.Minute, 0, nil, []migrationRun{
			{now: "2026-10-03 09:10", rows: upTo("2026-10-03 09:00")}, {now: "2026-10-03 10:10", rows: all}}},
		{"hourly rows with the top an hour below the edge", 2, 24 * time.Hour, 0, nil,
			runs(hourly, "2026-10-03 09:05", "2026-10-03 10:05")},
		{"lookback replay leaving a raw hour started", 2, 5 * time.Minute, 23 * time.Hour, nil, []migrationRun{
			{now: "2026-10-03 09:10", rows: all},
			{now: "2026-10-03 09:40", rows: all, fail: rawRecord("2026-10-02 11:00")},
			{now: "2026-10-03 10:10", rows: all}}},
		{"rows from the metrics_15s lifetime", 2, 5 * time.Minute, 0, nil, []migrationRun{
			{now: edgeNow, rows: func(ts time.Time) bool { return !ts.Before(at("2026-09-30 09:00")) }, rollupDays: 3}}},
		{"a lone row at the metrics_15s lifetime", 2, 5 * time.Minute, 0, nil, []migrationRun{
			{now: edgeNow, rows: func(ts time.Time) bool {
				return ts.Equal(at("2026-09-30 09:00")) || !ts.Before(at("2026-10-01 09:00"))
			}, rollupDays: 3}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			records := map[string]string{}
			for k, v := range c.records {
				records[k] = v
			}
			final, top := planRuns(c.rawDays, c.margin, c.lookback, records, c.runs...)
			floor, _ := recordedTime(records, floorRecord)
			last := c.runs[len(c.runs)-1]
			lo := floor.Add(-2 * time.Hour)
			if lifetime := last.lifetime(); !lifetime.IsZero() {
				lo = lifetime
			}
			checkRowOwners(t, final, last.rows, lo, top.Add(-15*time.Second))
		})
	}
}

func TestEach15sRowLandsInThe5mBucketStartingAtOrBeforeIt(t *testing.T) {
	u := migrationUnit{kind: unit15s, from: at("2026-10-02 08:00"), to: at("2026-10-02 09:00")}
	source := sourceSample(u)
	perBucket := map[time.Time]int{}
	for ts := u.from.Add(-time.Hour); ts.Before(u.to.Add(time.Hour)); ts = ts.Add(15 * time.Second) {
		sample, ok := source(ts)
		if !ok {
			continue
		}
		// metrics_tier.sql puts a sample at s in the bucket (end − 5m, end] with end = ⌊s − 1ms⌋₅ₘ + 5m.
		start := sample.Add(-time.Millisecond).Truncate(5 * time.Minute)
		if want := ts.Truncate(5 * time.Minute); !start.Equal(want) {
			t.Errorf("row at %s lands in the bucket starting %s, want %s", ts.Format("15:04:05"), start.Format("15:04"), want.Format("15:04"))
		}
		perBucket[start]++
	}
	if len(perBucket) != int(time.Hour/(5*time.Minute)) {
		t.Errorf("%d buckets: %v", len(perBucket), perBucket)
	}
	for start, n := range perBucket {
		if want := int(5 * time.Minute / (15 * time.Second)); n != want {
			t.Errorf("bucket starting %s holds %d rows, want %d", start.Format("15:04"), n, want)
		}
	}
}
