package maintenance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// checkedRaw answers each raw unit's counts with before then after, each tier's bucket count
// with 3 and each tier check with its sum in sums.
func checkedRaw(before, after any, sums map[string]uint64) func(context.Context, Statement) (any, error) {
	var mu sync.Mutex
	seen := map[string]int{}
	return func(_ context.Context, s Statement) (any, error) {
		switch {
		case s.Op == opCheck:
			return sums[s.Table], nil
		case s.Op == opCount && s.Table == rawSamples:
			mu.Lock()
			defer mu.Unlock()
			seen[s.QueryID]++
			if seen[s.QueryID] == 1 {
				return before, nil
			}
			return after, nil
		case s.Op == opCount:
			return uint64(3), nil
		}
		return nil, nil
	}
}

func TestAUnitWithATierShortOfItsSamplesFailsLeavingItStarted(t *testing.T) {
	f, opts := newRun("2026-10-02 07:00")
	log := &gateLogger{}
	opts.Logger = log
	f.on = checkedRaw(uint64(10), []any{uint64(17), uint64(15)}, map[string]uint64{"metrics_5m": 15, "metrics_1h": 14})
	_, err := migrate(t, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("err = %v", err)
	}
	want := "metric migration of cloki failed at unit 1/2 raw (2026-10-02T08:00:00Z, 2026-10-02T09:00:00Z]: " +
		"metrics_1h holds 14 samples, raw holds 15; the hour stays started and the next run rebuilds it"
	if err.Error() != want {
		t.Errorf("err = %q\nwant %q", err.Error(), want)
	}
	if got := f.value(rawRecord("2026-10-02 09:00")); got != unitStarted {
		t.Errorf("unit = %q, want started", got)
	}
	if got := unitSteps(f, "2026-10-02 08:00"); len(got) != 0 {
		t.Errorf("next unit ran %q", got)
	}
	if got := log.matching("1/2 raw"); len(got) != 0 {
		t.Errorf("finished line %q", got)
	}
}

func TestAUnitWithTiersAboveItsSamplesPasses(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	log := &gateLogger{}
	opts.Logger = log
	f.on = checkedRaw(uint64(10), []any{uint64(17), uint64(15)}, map[string]uint64{"metrics_5m": 15, "metrics_1h": 16})
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	if report.Units != 1 || report.RawSamples != 7 {
		t.Errorf("report = %+v", report)
	}
	if got := f.value(rawRecord("2026-10-02 09:00")); got != unitDone {
		t.Errorf("unit = %q, want done", got)
	}
	if got := log.matching("1/1 raw (2026-10-02T08:00:00Z, 2026-10-02T09:00:00Z]: 7 rows in"); len(got) != 1 {
		t.Errorf("finished lines %q", log.lines)
	}
}

func TestAUnitThatSkipsItsRebuildIsNotChecked(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	f.on = checkedRaw([]any{uint64(17), uint64(15)}, []any{uint64(17), uint64(15)}, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.statements() {
		if s.Op == opCheck || s.Op == opRebuild {
			t.Errorf("ran %s", step(s))
		}
	}
	if got := f.value(rawRecord("2026-10-02 09:00")); got != unitDone {
		t.Errorf("unit = %q, want done", got)
	}
}

func TestA15sUnitWithNoSourceRowsIsNotChecked(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 0, "2026-10-02 06:00")
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.statements() {
		if s.Op == opCheck {
			t.Errorf("ran %s", step(s))
		}
	}
}

func TestA15sUnitWithATierShortOfItsSourceFailsNamingMetrics15s(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	with15s(f, 240, "2026-10-02 06:00")
	answer := f.on
	f.on = func(ctx context.Context, s Statement) (any, error) {
		switch {
		case s.Op == opCount && s.Table == legacyRollup:
			return []any{uint64(240), uint64(238)}, nil
		case s.Op == opCheck:
			return uint64(237), nil
		}
		return answer(ctx, s)
	}
	_, err := migrate(t, f, opts)
	want := "metric migration of cloki failed at unit 2/2 15s (2026-10-02T05:00:00Z, 2026-10-02T06:00:00Z]: " +
		"metrics_5m holds 237 samples, metrics_15s holds 238; the hour stays started and the next run rebuilds it"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v\nwant %s", err, want)
	}
	if got := f.value(rollupRecord("2026-10-02 06:00")); got != unitStarted {
		t.Errorf("unit = %q, want started", got)
	}
}

func TestTheChecksCountNonStaleSamplesAndSumEachTiersHour(t *testing.T) {
	u := migrationUnit{kind: unitRaw, from: at("2026-10-02 08:00"), to: at("2026-10-02 09:00")}
	bounds := fmt.Sprintf("> fromUnixTimestamp64Milli(%d) AND ", u.from.UnixMilli())
	bounds2 := fmt.Sprintf(" <= fromUnixTimestamp64Milli(%d)", u.to.UnixMilli())
	if got, want := countRawSQL(u), "SELECT count(), countIf(reinterpretAsUInt64(value) != 0x7ff0000000000002) FROM metric_samples FINAL PREWHERE timestamp "+bounds+"timestamp"+bounds2; got != want {
		t.Errorf("count = %s", got)
	}
	if got, want := checkTierSQL(metricTiers[0], u), "SELECT sum(count) FROM metrics_5m WHERE bucket "+bounds+"bucket"+bounds2; got != want {
		t.Errorf("check = %s", got)
	}
}
