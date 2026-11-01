package maintenance

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testNow = "2026-10-02 09:10"

func testOptions(clock *testClock) MetricMigrationOptions {
	return MetricMigrationOptions{
		Database:    "cloki",
		Instance:    "me",
		SamplesDays: 1,
		Logger:      quietLogger(),
		now:         clock.now,
		sleep:       func(context.Context, time.Duration) error { return nil },
	}
}

// newRun returns a fake with floor recorded, so a run plans the hours above it.
func newRun(floor string) (*fakeExec, MetricMigrationOptions) {
	clock := &testClock{at(testNow)}
	f := newFakeExec(clock)
	f.record(floorRecord, ms(at(floor)))
	return f, testOptions(clock)
}

func migrate(t *testing.T, f *fakeExec, opts MetricMigrationOptions) (MetricMigrationReport, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return RunMetricMigration(ctx, f, opts)
}

// unitSteps lists the steps of the unit ending at to: its records and its statements.
func unitSteps(f *fakeExec, to string) []string {
	u := migrationUnit{kind: unitRaw, from: at(to).Add(-time.Hour), to: at(to)}
	var out []string
	for _, s := range f.statements() {
		if s.QueryID == unitQueryID("cloki", u) || (s.Op == opRecord && s.Args[2] == u.name()) {
			out = append(out, step(s))
		}
	}
	return out
}

func TestAUnitCopiesThenRebuildsEachTierBetweenItsRecords(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	f.on = recoveryRawGrowth(10, 17, 3, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	name := rawRecord("2026-10-02 09:00")
	sameStrings(t, "steps", unitSteps(f, "2026-10-02 09:00"), []string{
		"record " + name + "=started",
		"count metric_samples",
		"copy metric_samples",
		"count metric_samples",
		"count metrics_5m",
		"delete metrics_5m",
		"rebuild metrics_5m",
		"count metrics_1h",
		"delete metrics_1h",
		"rebuild metrics_1h",
		"record " + name + "=done",
	})
}

func TestTheFirstRunRecordsEachUnitTheFloorTheWatermarkAndTheSeries(t *testing.T) {
	clock := &testClock{at(testNow)}
	f := newFakeExec(clock)
	report, err := migrate(t, f, testOptions(clock))
	if err != nil {
		t.Fatal(err)
	}
	if report.Units != 24 {
		t.Errorf("units = %d", report.Units)
	}
	if got := f.value(floorRecord); got != ms(at("2026-10-01 09:00")) {
		t.Errorf("floor = %s", got)
	}
	for to := at("2026-10-02 09:00"); to.After(at("2026-10-01 09:00")); to = to.Add(-time.Hour) {
		if got := f.value("raw:" + ms(to)); got != unitDone {
			t.Errorf("unit to %s = %q", to, got)
		}
	}
	if got := f.value(watermarkRecord); got != ms(at("2026-10-02 09:00")) {
		t.Errorf("watermark = %s", got)
	}
	if f.value(seriesRecord) != unitDone {
		t.Error("series not recorded done")
	}
	if f.value(leaseRecord) != "" {
		t.Error("lease not released")
	}
}

func TestTheWatermarkAdvancesAfterTheUnitsSeriesAndMetadata(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	var tail []string
	for _, s := range f.statements() {
		if (s.QueryID == "" || s.QueryID == runQueryID("cloki", s.Op)) && s.Op != opGate && !strings.HasPrefix(step(s), "record lease") {
			tail = append(tail, step(s))
		}
	}
	unit := rawRecord("2026-10-02 09:00")
	sameStrings(t, "run steps", tail, []string{
		"setting system.settings",
		"exists metrics_15s",
		"record " + unit + "=started",
		"record " + unit + "=done",
		"series metric_series",
		"record series=done",
		"metadata metric_metadata",
		"record watermark=" + ms(at("2026-10-02 09:00")),
	})
}

func TestASeriesOrMetadataFailureLeavesTheWatermarkForTheNextRun(t *testing.T) {
	for _, op := range []string{opSeries, opMetadata} {
		t.Run(op, func(t *testing.T) {
			f, opts := newRun("2026-10-02 07:00")
			f.record(watermarkRecord, ms(at("2026-10-02 00:30")))
			f.record(seriesRecord, unitDone)
			boom := errors.New("code: 159, timeout exceeded")
			f.on = func(_ context.Context, s Statement) (any, error) {
				if s.Op == op {
					return nil, boom
				}
				return nil, nil
			}
			_, err := migrate(t, f, opts)
			var failed *MigrationFailedError
			if !errors.As(err, &failed) || !errors.Is(err, boom) || failed.Step != op {
				t.Fatalf("err = %v", err)
			}
			if got := f.value(watermarkRecord); got != ms(at("2026-10-02 00:30")) {
				t.Fatalf("watermark = %s", got)
			}
			f.on = nil
			if _, err = migrate(t, f, opts); err != nil {
				t.Fatal(err)
			}
			series := 0
			for _, s := range f.statements() {
				if s.Op == opSeries {
					series++
					if !strings.Contains(s.SQL, "date >= toDate('2026-10-01')") {
						t.Errorf("series since: %s", s.SQL)
					}
				}
			}
			if series != 2 {
				t.Errorf("%d series statements", series)
			}
			if got := f.value(watermarkRecord); got != ms(at("2026-10-02 09:00")) {
				t.Errorf("watermark = %s", got)
			}
		})
	}
}

func TestALaterRunCopiesTheSeriesSinceTheLookbackDay(t *testing.T) {
	f, opts := newRun("2026-10-02 07:00")
	f.record(watermarkRecord, ms(at("2026-10-02 00:30")))
	f.record(seriesRecord, unitDone)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.statements() {
		if s.Op == opSeries && !strings.Contains(s.SQL, "date >= toDate('2026-10-01')") {
			t.Errorf("series since: %s", s.SQL)
		}
		if s.Op == opRecord && s.Args[2] == seriesRecord {
			t.Error("series recorded again")
		}
	}
}

func TestSeriesDoneWithoutAWatermarkCopiesEverySeries(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	f.record(seriesRecord, unitDone)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.statements() {
		if s.Op == opSeries && strings.Contains(s.SQL, "date >=") {
			t.Errorf("series since: %s", s.SQL)
		}
	}
}

func TestTheReportSumsTheRawCountDifferencesAndTheRowsWritten(t *testing.T) {
	f, opts := newRun("2026-10-02 06:00")
	counts := map[string]int{}
	f.on = func(_ context.Context, s Statement) (any, error) {
		switch s.Op {
		case opCount:
			counts[s.QueryID]++
			if counts[s.QueryID] == 1 {
				return uint64(10), nil
			}
			return uint64(17), nil
		case opSeries:
			return 30, nil
		case opMetadata:
			return 4, nil
		}
		return nil, nil
	}
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	if report.Units != 3 || report.RawSamples != 21 || report.Series != 30 || report.Metadata != 4 {
		t.Fatalf("report = %+v", report)
	}
	if report.Database != "cloki" {
		t.Errorf("database = %q", report.Database)
	}
}

func TestAHeldLeaseNamesItsHolderAndRunsNothing(t *testing.T) {
	clock := &testClock{at(testNow).Add(-time.Minute)}
	f := newFakeExec(clock)
	f.record(leaseRecord, "other:1790931000")
	clock.t = at(testNow)
	_, err := migrate(t, f, testOptions(clock))
	var held *MigrationLeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("err = %v", err)
	}
	if held.Holder != "other" || !held.Renewed.Equal(at(testNow).Add(-time.Minute)) || held.TakeoverIn != 2*time.Minute {
		t.Fatalf("held = %+v", held)
	}
	for _, s := range f.statements() {
		if s.Op != opRecord || s.Args[2] != leaseRecord {
			t.Fatalf("ran %s", step(s))
		}
	}
}

func TestALeaseTakenBetweenUnitsFailsTheRunBeforeTheNext(t *testing.T) {
	f, opts := newRun("2026-10-02 07:00")
	first := rawRecord("2026-10-02 09:00")
	f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opRecord && s.Args[2] == first && s.Args[3] == unitDone {
			f.record(leaseRecord, "other:1")
		}
		return nil, nil
	}
	_, err := migrate(t, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || !errors.Is(err, errLeaseLost) {
		t.Fatalf("err = %v", err)
	}
	if failed.Step != "raw (2026-10-02T07:00:00Z, 2026-10-02T08:00:00Z]" {
		t.Errorf("step = %q", failed.Step)
	}
	if got := unitSteps(f, "2026-10-02 08:00"); len(got) != 0 {
		t.Errorf("next unit ran %q", got)
	}
	if f.value(watermarkRecord) != "" {
		t.Error("watermark advanced")
	}
	if f.value(leaseRecord) != "other:1" {
		t.Error("released another holder's lease")
	}
}

func TestALeaseLostOnRenewalCancelsTheRunningUnit(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	opts.Renew = time.Millisecond
	f.on = func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opCopy {
			f.record(leaseRecord, "other:1")
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, nil
	}
	_, err := migrate(t, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || !errors.Is(err, errLeaseLost) {
		t.Fatalf("err = %v", err)
	}
	if got := f.value(rawRecord("2026-10-02 09:00")); got != unitStarted {
		t.Errorf("unit = %q, want started", got)
	}
}

func TestARenewalErrorCancelsTheRunningUnitNamingTheError(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	opts.Renew = time.Millisecond
	boom := errors.New("code: 210, connection refused")
	var copying atomic.Bool
	f.on = func(ctx context.Context, s Statement) (any, error) {
		switch {
		case s.Op == opCopy:
			copying.Store(true)
			<-ctx.Done()
			return nil, ctx.Err()
		case copying.Load() && s.Op == opRecord && s.Args[2] == leaseRecord:
			return nil, boom
		}
		return nil, nil
	}
	_, err := migrate(t, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || !errors.Is(err, boom) || errors.Is(err, errLeaseLost) {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(err.Error(), "renew the metric migration lease: code: 210") {
		t.Errorf("err = %v", err)
	}
}

func TestAFailingStatementFailsTheRunAtItsUnitLeavingItStarted(t *testing.T) {
	f, opts := newRun("2026-10-02 07:00")
	boom := errors.New("code: 241, memory limit exceeded")
	f.on = recoveryRawGrowth(10, 17, 0, func(_ context.Context, s Statement) (any, error) {
		if s.Op == opRebuild && strings.HasSuffix(s.QueryID, ms(at("2026-10-02 08:00"))) {
			return nil, boom
		}
		return nil, nil
	})
	_, err := migrate(t, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if failed.Step != "raw (2026-10-02T07:00:00Z, 2026-10-02T08:00:00Z]" || failed.Database != "cloki" {
		t.Errorf("failed = %+v", failed)
	}
	if f.value(rawRecord("2026-10-02 09:00")) != unitDone || f.value(rawRecord("2026-10-02 08:00")) != unitStarted {
		t.Error("unit records")
	}
	if f.value(watermarkRecord) != "" {
		t.Error("watermark advanced")
	}
	if f.value(leaseRecord) != "" {
		t.Error("lease not released")
	}
}
