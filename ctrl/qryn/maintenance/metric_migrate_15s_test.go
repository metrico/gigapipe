package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func rollupRecord(to string) string {
	return "15s:" + ms(at(to))
}

func ends15s(ends ...string) []time.Time {
	var out []time.Time
	for _, e := range ends {
		out = append(out, at(e))
	}
	return out
}

func unitKinds15s(units []migrationUnit) []string {
	var out []string
	for _, u := range units {
		out = append(out, u.kind)
	}
	return out
}

func TestThe15sHoursBelowTheFloorFollowTheRawUnitsNewestFirst(t *testing.T) {
	records := map[string]string{floorRecord: ms(at("2026-10-02 07:00"))}
	rollup := &rollupHistory{lifetime: at("2026-09-02 09:00"), ends: ends15s("2026-10-01 12:00", "2026-10-02 07:00", "2026-10-02 03:00")}
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, records, rollup)
	sameStrings(t, "units", bounds(plan.units), []string{
		"10-02 08:00 10-02 09:00",
		"10-02 07:00 10-02 08:00",
		"10-02 06:00 10-02 07:00",
		"10-02 02:00 10-02 03:00",
		"10-01 11:00 10-01 12:00",
	})
	sameStrings(t, "kinds", unitKinds15s(plan.units), []string{unitRaw, unitRaw, unit15s, unit15s, unit15s})
	if got := plan.units[2].name(); got != rollupRecord("2026-10-02 07:00") {
		t.Errorf("name = %s", got)
	}
}

func TestNo15sUnitsWithoutMetrics15s(t *testing.T) {
	records := map[string]string{
		floorRecord:                      ms(at("2026-10-02 07:00")),
		rollupRecord("2026-10-02 06:00"): unitStarted,
	}
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, records, nil)
	sameStrings(t, "kinds", unitKinds15s(plan.units), []string{unitRaw, unitRaw})
}

func TestThe15sUnitsStayAtOrBelowTheFloorAndAboveTheLifetime(t *testing.T) {
	records := map[string]string{floorRecord: ms(at("2026-10-02 07:00"))}
	rollup := &rollupHistory{lifetime: at("2026-09-30 09:00"), ends: ends15s(
		"2026-10-02 08:00", "2026-10-02 07:00", "2026-09-30 10:00", "2026-09-30 09:00")}
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, records, rollup)
	sameStrings(t, "units", bounds(plan.units[2:]), []string{
		"10-02 06:00 10-02 07:00",
		"09-30 09:00 09-30 10:00",
	})
}

func TestWithoutARollupLifetimeThe15sUnitsReachTheOldestHour(t *testing.T) {
	records := map[string]string{floorRecord: ms(at("2026-10-02 07:00"))}
	rollup := &rollupHistory{ends: ends15s("2025-01-01 01:00")}
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, records, rollup)
	sameStrings(t, "units", bounds(plan.units[2:]), []string{"01-01 00:00 01-01 01:00"})
}

func TestALaterRunSkipsThe15sHoursDoneAndRecoversTheStartedOnes(t *testing.T) {
	records := map[string]string{
		floorRecord:                      ms(at("2026-10-02 07:00")),
		watermarkRecord:                  ms(at("2026-10-02 09:00")),
		rawRecord("2026-10-02 09:00"):    unitDone,
		rawRecord("2026-10-02 08:00"):    unitDone,
		rollupRecord("2026-10-02 07:00"): unitDone,
		rollupRecord("2026-10-02 05:00"): unitStarted,
		// started, its rows since dropped by the TTL
		rollupRecord("2026-10-02 04:00"): unitStarted,
	}
	rollup := &rollupHistory{lifetime: at("2026-09-02 09:00"), ends: ends15s("2026-10-02 07:00", "2026-10-02 06:00", "2026-10-02 05:00")}
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, records, rollup)
	sameStrings(t, "units", bounds(plan.units), []string{
		"10-02 08:00 10-02 09:00",
		"10-02 05:00 10-02 06:00",
		"10-02 04:00 10-02 05:00",
		"10-02 03:00 10-02 04:00",
	})
	sameStrings(t, "recoveries", recoveries(plan.units), []string{"10-02 05:00", "10-02 04:00"})
}

// with15s answers metrics_15s as present with metric rows in the hours ending at ends, each
// 15s source count and tier count with rows, and each unit's raw count with 0 before its copy
// and rows after it.
func with15s(f *fakeExec, rows uint64, ends ...string) {
	var mu sync.Mutex
	counted := map[string]bool{}
	f.on = func(_ context.Context, s Statement) (any, error) {
		switch s.Op {
		case opExists:
			return uint8(1), nil
		case op15sHours:
			var out [][]any
			for _, e := range ends {
				out = append(out, []any{at(e).UnixMilli() / time.Hour.Milliseconds()})
			}
			return out, nil
		case opCount:
			if s.Table != rawSamples {
				return rows, nil
			}
			mu.Lock()
			defer mu.Unlock()
			if counted[s.QueryID] {
				return rows, nil
			}
			counted[s.QueryID] = true
			return uint64(0), nil
		}
		return nil, nil
	}
}

// steps15sOf lists the steps of the unit of kind ending at to: its records and its statements.
func steps15sOf(f *fakeExec, kind, to string) []string {
	u := migrationUnit{kind: kind, from: at(to).Add(-time.Hour), to: at(to)}
	var out []string
	for _, s := range f.statements() {
		if s.QueryID == unitQueryID("cloki", u) || (s.Op == opRecord && s.Args[2] == u.name()) {
			out = append(out, step(s))
		}
	}
	return out
}

func TestA15sUnitCountsItsSourceThenRebuildsEachTier(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 240, "2026-10-02 08:00", "2026-10-02 06:00")
	opts.RollupDays = 30
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	if report.Units != 3 {
		t.Fatalf("units = %d", report.Units)
	}
	rec := rollupRecord("2026-10-02 06:00")
	sameStrings(t, "15s steps", steps15sOf(f, unit15s, "2026-10-02 06:00"), []string{
		"record " + rec + "=started",
		"count metrics_15s",
		"count metrics_5m",
		"delete metrics_5m",
		"rebuild metrics_5m",
		"count metrics_1h",
		"delete metrics_1h",
		"rebuild metrics_1h",
		"check metrics_5m",
		"check metrics_1h",
		"record " + rec + "=done",
	})
	for _, s := range f.statements() {
		if s.QueryID == unitQueryID("cloki", migrationUnit{kind: unit15s, to: at("2026-10-02 08:00")}) && s.Table == rawSamples {
			t.Errorf("15s unit touches raw: %s", step(s))
		}
	}
	if f.value(rollupRecord("2026-10-02 08:00")) != unitDone {
		t.Error("15s unit not recorded done")
	}
}

func TestA15sUnitWithNoSourceRowsIsDoneWithoutTouchingTheTiers(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 0, "2026-10-02 06:00")
	opts.RollupDays = 30
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	rec := rollupRecord("2026-10-02 06:00")
	sameStrings(t, "15s steps", steps15sOf(f, unit15s, "2026-10-02 06:00"), []string{
		"record " + rec + "=started",
		"count metrics_15s",
		"record " + rec + "=done",
	})
	if report.Rollup15sSamples != 0 {
		t.Errorf("15s = %d", report.Rollup15sSamples)
	}
}

func TestTheReportCountsThe15sSamplesApartFromTheRawOnes(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 240, "2026-10-02 08:00", "2026-10-02 06:00")
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	if report.RawSamples != 240 || report.Rollup15sSamples != 480 {
		t.Errorf("raw = %d, 15s = %d", report.RawSamples, report.Rollup15sSamples)
	}
	if !strings.Contains(report.Summary(), "240 raw samples, 480 approximate 15s samples") {
		t.Errorf("summary = %s", report.Summary())
	}
}

func TestTheRunAsksForThe15sHoursBeforeTheUnits(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 0, "2026-10-02 08:00")
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	var head []string
	for _, s := range f.statements() {
		if s.QueryID != "" {
			break
		}
		if s.Op != opGate && !strings.HasPrefix(step(s), "record lease") {
			head = append(head, step(s))
		}
	}
	sameStrings(t, "head", head, []string{
		"setting system.settings",
		"exists metrics_15s",
		op15sHours + " metrics_15s",
		"record " + rawRecord("2026-10-02 09:00") + "=started",
	})
}

func TestWithoutMetrics15sTheRunAsksForNoHours(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range f.statements() {
		if s.Op == op15sHours {
			t.Errorf("asked %s", step(s))
		}
	}
	if report.Units != 1 {
		t.Errorf("units = %d", report.Units)
	}
}

func TestAFirstRunPlansFromOneReadingOfTheClock(t *testing.T) {
	clock := &testClock{at(testNow)}
	f := newFakeExec(clock)
	with15s(f, 240, "2026-10-01 10:00", "2026-10-01 11:00", "2026-10-01 12:00", "2026-10-01 13:00", "2026-10-01 14:00")
	opts := testOptions(clock)
	opts.RollupDays = 30
	opts.now = func() time.Time {
		t := clock.t
		clock.t = clock.t.Add(time.Hour)
		return t
	}
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	v, err := strconv.ParseInt(f.value(floorRecord), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	floor := time.UnixMilli(v).UTC()
	for _, s := range f.statements() {
		if s.Op == op15sHours && !strings.Contains(s.SQL, fmt.Sprintf("timestamp_ns < %d", floor.UnixNano())) {
			t.Errorf("hours query floor differs from %s: %s", floor, s.SQL)
		}
	}
	if f.value(rollupRecord(floor.Format("2006-01-02 15:04"))) != unitDone {
		t.Errorf("15s hour ending at the floor %s not migrated", floor)
	}
}

func TestThe15sRebuildTakesEachRowOfItsHourAsOneSample1msAfterItsStart(t *testing.T) {
	u := migrationUnit{kind: unit15s, from: at("2026-10-02 05:00"), to: at("2026-10-02 06:00")}
	sql := rebuild15sTierSQL(metricTiers[0], u, 1, 0)
	for _, want := range []string{
		"INSERT INTO metrics_5m (" + tierColumns + ")",
		"fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000) + 1) AS timestamp",
		"prev.1 AS prev_timestamp, prev.2 AS prev_value, 1 AS aggregate",
		"OVER (PARTITION BY fingerprint ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev",
		"FROM " + legacyRollup + " WHERE " + legacyMetricRows,
		fmt.Sprintf("timestamp_ns >= %d AND timestamp_ns < %d GROUP BY fingerprint, timestamp_ns)))",
			at("2026-10-02 05:00").UnixNano(), at("2026-10-02 06:00").UnixNano()),
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("rebuild lacks %q: %s", want, sql)
		}
	}
	if strings.Contains(sql, rawSamples) {
		t.Errorf("15s rebuild reads raw: %s", sql)
	}
}

func TestThe15sCountReadsTheRowsStartingInTheHour(t *testing.T) {
	u := migrationUnit{kind: unit15s, from: at("2026-10-02 05:00"), to: at("2026-10-02 06:00")}
	// Each row becomes a sample 1 ms after its start, in the bucket its start opens.
	want := fmt.Sprintf("SELECT count(), countIf(reinterpretAsUInt64(value) != 0x7ff0000000000002) FROM (SELECT fingerprint, "+
		"fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000) + 1) AS timestamp, "+
		"argMaxMerge(last) AS value FROM metrics_15s WHERE type IN (2, 0) AND timestamp_ns >= %d AND timestamp_ns < %d "+
		"GROUP BY fingerprint, timestamp_ns)", u.from.UnixNano(), u.to.UnixNano())
	if sql := count15sSQL(u); sql != want {
		t.Errorf("count = %s", sql)
	}
}

func TestThe15sHoursQueryListsEachRowUnderTheHourItStartsIn(t *testing.T) {
	edge, lifetime := at("2026-10-02 07:00"), at("2026-09-02 09:00")
	hour := fmt.Sprintf("intDiv(timestamp_ns, %d) + 1", int64(time.Hour))
	below := fmt.Sprintf("%s AND timestamp_ns < %d", legacyMetricRows, edge.UnixNano())
	after := fmt.Sprintf("timestamp_ns >= %d", lifetime.UnixNano())
	if sql := rollup15sHoursSQL(lifetime, edge); !strings.Contains(sql, hour) || !strings.Contains(sql, below) || !strings.Contains(sql, after) {
		t.Errorf("with lifetime: %s", sql)
	}
	if sql := rollup15sHoursSQL(time.Time{}, edge); !strings.Contains(sql, below) || strings.Contains(sql, "timestamp_ns >") {
		t.Errorf("without lifetime: %s", sql)
	}
}

func TestA15sRebuildOverTheMemoryLimitRetriesInTwoParts(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 240, "2026-10-02 06:00")
	opts.RollupDays = 30
	u := migrationUnit{kind: unit15s, from: at("2026-10-02 05:00"), to: at("2026-10-02 06:00")}
	id := unitQueryID("cloki", u)
	base, failed := f.on, false
	f.on = func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opRebuild && s.QueryID == id && !failed {
			failed = true
			return nil, limitHit(241)
		}
		return base(ctx, s)
	}
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	var steps []string
	for _, s := range f.statements() {
		if s.QueryID != id {
			continue
		}
		if s.Parts > 1 {
			steps = append(steps, fmt.Sprintf("%s %d/%d", step(s), s.Part, s.Parts))
			if !strings.Contains(s.SQL, splitWhere(s.Parts, s.Part)+" GROUP BY") {
				t.Errorf("part %d/%d lacks its share before grouping: %s", s.Part, s.Parts, s.SQL)
			}
			if !strings.Contains(s.SQL, "FROM "+legacyRollup) {
				t.Errorf("15s rebuild part reads %s", s.SQL)
			}
		} else {
			steps = append(steps, step(s))
		}
	}
	sameStrings(t, "15s split steps", steps, []string{
		"count metrics_15s",
		"count metrics_5m",
		"delete metrics_5m",
		"rebuild metrics_5m",
		"delete metrics_5m",
		"rebuild metrics_5m 0/2",
		"rebuild metrics_5m 1/2",
		"count metrics_1h",
		"delete metrics_1h",
		"rebuild metrics_1h 0/2",
		"rebuild metrics_1h 1/2",
		"check metrics_5m",
		"check metrics_1h",
	})
	if f.value(rollupRecord("2026-10-02 06:00")) != unitDone {
		t.Error("15s unit not recorded done")
	}
}
