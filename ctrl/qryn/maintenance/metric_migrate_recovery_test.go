package maintenance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAZeroDifferenceUnitIsRecordedDoneWithoutTouchingTheTiers(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	name := rawRecord("2026-10-02 09:00")
	sameStrings(t, "steps", unitSteps(f, "2026-10-02 09:00"), []string{
		"record " + name + "=started",
		"count metric_samples",
		"copy metric_samples",
		"count metric_samples",
		"record " + name + "=done",
	})
	if report.Units != 1 || report.RawSamples != 0 || report.Recovered != 0 {
		t.Errorf("report = %+v", report)
	}
}

func TestAStartedUnitRebuildsWithAZeroDifferenceAndCountsAsRecovered(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	name := rawRecord("2026-10-02 09:00")
	f.record(name, unitStarted)
	f.on = recoveryRawGrowth(0, 0, 0, nil)
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "steps", unitSteps(f, "2026-10-02 09:00"), []string{
		"record " + name + "=started",
		"count metric_samples",
		"copy metric_samples",
		"count metric_samples",
		"count metrics_5m",
		"rebuild metrics_5m",
		"count metrics_1h",
		"rebuild metrics_1h",
		"check metrics_5m",
		"check metrics_1h",
		"record " + name + "=done",
	})
	if report.Recovered != 1 || report.RawSamples != 0 || report.NoNewData() {
		t.Errorf("report = %+v", report)
	}
}

func TestOnlyATierWithBucketsInTheHourIsDeleted(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	growth := recoveryRawGrowth(10, 17, 0, nil)
	f.on = func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opCount && s.Table == "metrics_5m" {
			return uint64(3), nil
		}
		return growth(ctx, s)
	}
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "tier steps", unitSteps(f, "2026-10-02 09:00")[4:8], []string{
		"count metrics_5m",
		"delete metrics_5m",
		"rebuild metrics_5m",
		"count metrics_1h",
	})
	for _, s := range f.statements() {
		if s.Op == opDelete && s.Table == "metrics_1h" {
			t.Error("deleted from an empty metrics_1h hour")
		}
	}
}

func TestACancelledRunKillsTheUnitsQueryAndLeavesItStarted(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var killCtxErr error
	f.on = func(qctx context.Context, s Statement) (any, error) {
		switch s.Op {
		case opCopy:
			cancel()
			return nil, qctx.Err()
		case opKill:
			killCtxErr = qctx.Err()
		}
		return nil, nil
	}
	_, err := RunMetricMigration(ctx, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	u := migrationUnit{kind: unitRaw, from: at("2026-10-02 08:00"), to: at("2026-10-02 09:00")}
	kills := 0
	for _, s := range f.statements() {
		if s.Op == opKill {
			kills++
			if !strings.Contains(s.SQL, "'"+unitQueryID("cloki", u)+"'") || !strings.Contains(s.SQL, "SYNC") {
				t.Errorf("kill = %s", s.SQL)
			}
			if len(s.Settings) != 0 {
				t.Errorf("kill carries settings %v", s.Settings)
			}
		}
	}
	if kills != 1 || killCtxErr != nil {
		t.Errorf("%d kills, kill ctx err %v", kills, killCtxErr)
	}
	if got := f.value(u.name()); got != unitStarted {
		t.Errorf("unit = %q, want started", got)
	}
	if f.value(leaseRecord) != "" {
		t.Error("lease not released")
	}
}

func TestACancelledSeriesCopyIsKilledUnderTheRunsQueryID(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var seriesID string
	f.on = func(qctx context.Context, s Statement) (any, error) {
		if s.Op == opSeries {
			seriesID = s.QueryID
			cancel()
			return nil, qctx.Err()
		}
		return nil, nil
	}
	_, err := RunMetricMigration(ctx, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || failed.Step != opSeries {
		t.Fatalf("err = %v", err)
	}
	if seriesID != runQueryID("cloki", opSeries) {
		t.Errorf("series query id = %q", seriesID)
	}
	var kills []string
	for _, s := range f.statements() {
		if s.Op == opKill {
			kills = append(kills, s.SQL)
		}
	}
	if len(kills) != 1 || !strings.Contains(kills[0], "'"+runQueryID("cloki", opSeries)+"'") {
		t.Errorf("kills = %q", kills)
	}
}

func TestKillSQLEscapesTheQueryID(t *testing.T) {
	if got, want := killSQL(`a'b\c`), `KILL QUERY WHERE query_id = 'a\'b\\c' SYNC`; got != want {
		t.Errorf("killSQL = %s, want %s", got, want)
	}
}

func TestALeaseLostMidUnitKillsTheUnitsQuery(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	opts.Renew = time.Millisecond
	var killCtxErr error
	f.on = func(ctx context.Context, s Statement) (any, error) {
		switch s.Op {
		case opCopy:
			f.record(leaseRecord, "other:1")
			<-ctx.Done()
			return nil, ctx.Err()
		case opKill:
			killCtxErr = ctx.Err()
		}
		return nil, nil
	}
	_, err := migrate(t, f, opts)
	if !errors.Is(err, errLeaseLost) {
		t.Fatalf("err = %v", err)
	}
	u := migrationUnit{kind: unitRaw, from: at("2026-10-02 08:00"), to: at("2026-10-02 09:00")}
	var kills []string
	for _, s := range f.statements() {
		if s.Op == opKill {
			kills = append(kills, s.SQL)
		}
	}
	if len(kills) != 1 || !strings.Contains(kills[0], "'"+unitQueryID("cloki", u)+"'") || killCtxErr != nil {
		t.Errorf("kills = %q, kill ctx err %v", kills, killCtxErr)
	}
	if got := f.value(u.name()); got != unitStarted {
		t.Errorf("unit = %q, want started", got)
	}
}

func TestNoNewDataMeansNoSamplesAndNoRecoveredUnit(t *testing.T) {
	for _, c := range []struct {
		r    MetricMigrationReport
		want bool
	}{
		{MetricMigrationReport{Units: 3, Series: 5, Metadata: 2}, true},
		{MetricMigrationReport{Units: 3, RawSamples: 1}, false},
		{MetricMigrationReport{Units: 3, Rollup15sSamples: 1}, false},
		{MetricMigrationReport{Units: 3, Recovered: 1}, false},
	} {
		if got := c.r.NoNewData(); got != c.want {
			t.Errorf("%+v: NoNewData = %v", c.r, got)
		}
	}
}

// recoveryRawGrowth answers each unit's first raw count with before and its second with after, every
// tier count with buckets, and any other statement with next, when set.
func recoveryRawGrowth(before, after, buckets uint64, next func(context.Context, Statement) (any, error)) func(context.Context, Statement) (any, error) {
	var mu sync.Mutex
	seen := map[string]int{}
	return func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opCount && s.Table == rawSamples {
			mu.Lock()
			defer mu.Unlock()
			seen[s.QueryID]++
			if seen[s.QueryID] == 1 {
				return before, nil
			}
			return after, nil
		}
		if s.Op == opCount {
			return buckets, nil
		}
		if next != nil {
			return next(ctx, s)
		}
		return nil, nil
	}
}

func TestA15sUnitWithNoSourceRowsIsNoNewData(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 0, "2026-10-02 06:00")
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	name := rollupRecord("2026-10-02 06:00")
	sameStrings(t, "15s steps", steps15sOf(f, unit15s, "2026-10-02 06:00"), []string{
		"record " + name + "=started",
		"count metrics_15s",
		"record " + name + "=done",
	})
	if report.Rollup15sSamples != 0 || report.Recovered != 0 || !report.NoNewData() {
		t.Errorf("report = %+v", report)
	}
}

func TestAStarted15sUnitWithNoSourceRowsIsDoneAndCountsAsRecovered(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 0, "2026-10-02 06:00")
	name := rollupRecord("2026-10-02 06:00")
	f.record(name, unitStarted)
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "15s steps", steps15sOf(f, unit15s, "2026-10-02 06:00"), []string{
		"record " + name + "=started",
		"count metrics_15s",
		"record " + name + "=done",
	})
	if report.Recovered != 1 || report.Rollup15sSamples != 0 || report.NoNewData() {
		t.Errorf("report = %+v", report)
	}
}

func TestAStarted15sUnitRedoesItsCountDeleteAndRebuild(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 240, "2026-10-02 06:00")
	name := rollupRecord("2026-10-02 06:00")
	f.record(name, unitStarted)
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "15s steps", steps15sOf(f, unit15s, "2026-10-02 06:00"), []string{
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
		"record " + name + "=done",
	})
	if report.Recovered != 1 || report.Rollup15sSamples != 240 {
		t.Errorf("report = %+v", report)
	}
}

func TestA15sUnitsSourceCountAloneIsNewData(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 0, "2026-10-02 06:00")
	base := f.on
	id := unitQueryID("cloki", migrationUnit{kind: unit15s, from: at("2026-10-02 05:00"), to: at("2026-10-02 06:00")})
	f.on = func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opCount && s.Table == legacyRollup && s.QueryID == id {
			return uint64(240), nil
		}
		return base(ctx, s)
	}
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	if report.RawSamples != 0 || report.Rollup15sSamples != 240 || report.NoNewData() {
		t.Errorf("report = %+v", report)
	}
}

func TestATierDeleteCarriesTheLimitsAndWaitsForItsMutation(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	opts.MaxThreads, opts.MaxMemory, opts.MaxExecutionTime = 3, 1<<30, 10*time.Minute
	opts.ExternalSort, opts.ExternalGroupBy, opts.MaxSpill = 1<<28, 1<<27, 1<<32
	f.on = recoveryRawGrowth(10, 17, 3, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	m := &migration{opts: opts}
	want := m.limitSettings()
	want["mutations_sync"] = 2
	deletes := 0
	for _, s := range f.statements() {
		if s.Op != opDelete {
			continue
		}
		deletes++
		for k, v := range want {
			if k == externalSortRatio {
				continue
			}
			if s.Settings[k] != v {
				t.Errorf("%s: %s = %v, want %v", step(s), k, s.Settings[k], v)
			}
		}
	}
	if deletes != 2 {
		t.Fatalf("%d deletes", deletes)
	}
}
