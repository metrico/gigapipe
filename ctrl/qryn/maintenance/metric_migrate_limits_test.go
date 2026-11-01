package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
)

// limitedStatements are the logged statements other than lease and run-state records
// and the back-off gate's own reads.
func limitedStatements(f *fakeExec) []Statement {
	var out []Statement
	for _, s := range f.statements() {
		if !bookkeeping(s) {
			out = append(out, s)
		}
	}
	return out
}

// bookkeeping reports whether s is one of the run-state records and the back-off gate's reads.
func bookkeeping(s Statement) bool {
	return s.Op == opRecord || s.Op == opRecords || s.Op == opRecordAge || s.Op == opGate || s.Op == opGateTable
}

// requireTierSteps fails t unless each tier saw a count, a delete and a rebuild.
func requireTierSteps(t *testing.T, ss []Statement) {
	t.Helper()
	seen := map[string]bool{}
	for _, s := range ss {
		seen[s.Op+" "+s.Table] = true
	}
	for _, tier := range metricTiers {
		for _, op := range []string{opCount, opDelete, opRebuild} {
			if !seen[op+" "+tier.Table] {
				t.Fatalf("no %s %s", op, tier.Table)
			}
		}
	}
}

func TestEveryStatementCarriesTheLimitsAndNoPriority(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	opts.MaxThreads = 3
	opts.MaxMemory = 5 * gib
	opts.MaxExecutionTime = 10 * time.Minute
	opts.ExternalSort = 100 * mib
	opts.ExternalGroupBy = 200 * mib
	opts.MaxSpill = 7 * gib
	f.on = recoveryRawGrowth(10, 17, 3, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"max_threads":                               3,
		"max_insert_threads":                        1,
		"max_memory_usage":                          5 * gib,
		"max_execution_time":                        600,
		"max_bytes_before_external_sort":            100 * mib,
		"max_bytes_before_external_group_by":        200 * mib,
		"max_temporary_data_on_disk_size_for_query": 7 * gib,
		"insert_deduplicate":                        0,
	}
	ss := limitedStatements(f)
	requireTierSteps(t, ss)
	seen := map[string]bool{}
	for _, s := range ss {
		seen[s.Op] = true
	}
	for _, op := range []string{opCopy, opSeries, opMetadata} {
		if !seen[op] {
			t.Fatalf("no %s statement", op)
		}
	}
	for _, s := range ss {
		for k, v := range want {
			if s.Settings[k] != v {
				t.Errorf("%s: %s = %v, want %v", step(s), k, s.Settings[k], v)
			}
		}
		if _, ok := s.Settings["priority"]; ok {
			t.Errorf("%s sends priority", step(s))
		}
		if _, ok := s.Settings["insert_deduplication_token"]; ok {
			t.Errorf("%s sends a deduplication token", step(s))
		}
		if s.Op == opDelete && s.Settings["mutations_sync"] != 2 {
			t.Errorf("%s: mutations_sync = %v", step(s), s.Settings["mutations_sync"])
		}
	}
	for _, s := range f.statements() {
		if _, ok := s.Settings["insert_deduplicate"]; ok && bookkeeping(s) {
			t.Errorf("%s: run-state or gate statement carries insert_deduplicate", step(s))
		}
	}
}

func TestTheExternalSortRatioIsSentOnlyWhenTheServerListsIt(t *testing.T) {
	for _, listed := range []bool{true, false} {
		f, opts := newRun("2026-10-02 08:00")
		f.on = func(_ context.Context, s Statement) (any, error) {
			if s.Op == opSetting && listed {
				return 1, nil
			}
			return nil, nil
		}
		f.on = recoveryRawGrowth(10, 17, 3, f.on)
		if _, err := migrate(t, f, opts); err != nil {
			t.Fatal(err)
		}
		requireTierSteps(t, limitedStatements(f))
		probes := 0
		for _, s := range limitedStatements(f) {
			if s.Op == opSetting {
				probes++
				continue
			}
			v, ok := s.Settings["max_bytes_ratio_before_external_sort"]
			if listed && v != 0 {
				t.Errorf("listed: %s ratio = %v", step(s), v)
			}
			if !listed && ok {
				t.Errorf("not listed: %s sends the ratio", step(s))
			}
		}
		if probes != 1 {
			t.Errorf("listed=%v: %d probes", listed, probes)
		}
	}
}

// splitSteps lists the statements of the unit ending at to, naming each split part.
func splitSteps(f *fakeExec, to string) []string {
	u := migrationUnit{kind: unitRaw, from: at(to).Add(-time.Hour), to: at(to)}
	var out []string
	for _, s := range f.statements() {
		if s.QueryID != unitQueryID("cloki", u) {
			continue
		}
		if s.Parts > 1 {
			out = append(out, fmt.Sprintf("%s %d/%d", step(s), s.Part, s.Parts))
		} else {
			out = append(out, step(s))
		}
	}
	return out
}

func limitHit(code int32) error {
	return &clickhouse.Exception{Code: code, Message: "limit"}
}

func TestACopyOverTheMemoryLimitRetriesInTwoParts(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	failed := false
	f.on = recoveryRawGrowth(10, 17, 3, func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCopy && !failed {
			failed = true
			return nil, limitHit(241)
		}
		return nil, nil
	})
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "steps", splitSteps(f, "2026-10-02 09:00"), []string{
		"count metric_samples",
		"copy metric_samples",
		"copy metric_samples 0/2",
		"copy metric_samples 1/2",
		"count metric_samples",
		"count metrics_5m",
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
	if f.value(rawRecord("2026-10-02 09:00")) != unitDone {
		t.Error("unit not done")
	}
}

func TestARebuildOverTheTimeLimitDeletesAgainBeforeItsParts(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	failed := false
	f.on = recoveryRawGrowth(10, 17, 3, func(_ context.Context, s Statement) (any, error) {
		if s.Op == opRebuild && s.Table == "metrics_5m" && !failed {
			failed = true
			return nil, limitHit(159)
		}
		return nil, nil
	})
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "steps", splitSteps(f, "2026-10-02 09:00"), []string{
		"count metric_samples",
		"copy metric_samples",
		"count metric_samples",
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
}

func TestARebuildRetryDeletesTheHourEvenWhenTheTierHeldNone(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	failed := false
	f.on = recoveryRawGrowth(10, 17, 0, func(_ context.Context, s Statement) (any, error) {
		if s.Op == opRebuild && s.Table == "metrics_5m" && !failed {
			failed = true
			return nil, limitHit(159)
		}
		return nil, nil
	})
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "steps", splitSteps(f, "2026-10-02 09:00"), []string{
		"count metric_samples",
		"copy metric_samples",
		"count metric_samples",
		"count metrics_5m",
		"rebuild metrics_5m",
		"delete metrics_5m",
		"rebuild metrics_5m 0/2",
		"rebuild metrics_5m 1/2",
		"count metrics_1h",
		"rebuild metrics_1h 0/2",
		"rebuild metrics_1h 1/2",
		"check metrics_5m",
		"check metrics_1h",
	})
}

func TestASplitStartsAtTheConfiguredSplit(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	opts.Split = 4
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	var copies []string
	for _, s := range splitSteps(f, "2026-10-02 09:00") {
		if strings.HasPrefix(s, "copy") {
			copies = append(copies, s)
		}
	}
	sameStrings(t, "copies", copies, []string{
		"copy metric_samples 0/4", "copy metric_samples 1/4", "copy metric_samples 2/4", "copy metric_samples 3/4",
	})
}

// copySplits lists the split of each copy attempt of the unit ending at to.
func copySplits(f *fakeExec, to string) []int {
	u := migrationUnit{kind: unitRaw, from: at(to).Add(-time.Hour), to: at(to)}
	var out []int
	for _, s := range f.statements() {
		if s.QueryID == unitQueryID("cloki", u) && s.Op == opCopy && s.Part == 0 {
			out = append(out, max(s.Parts, 1))
		}
	}
	return out
}

func TestALimitAtTheMaximumSplitFailsTheRunLeavingTheUnitStarted(t *testing.T) {
	f, opts := newRun("2026-10-02 07:00")
	opts.MaxSplit = 6
	f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCopy && strings.HasSuffix(s.QueryID, ms(at("2026-10-02 08:00"))) {
			return nil, limitHit(396)
		}
		return nil, nil
	}
	_, err := migrate(t, f, opts)
	var failed *MigrationFailedError
	var ex *clickhouse.Exception
	if !errors.As(err, &failed) || !errors.As(err, &ex) || ex.Code != 396 {
		t.Fatalf("err = %v", err)
	}
	if got := fmt.Sprint(copySplits(f, "2026-10-02 08:00")); got != "[1 2 4 6]" {
		t.Errorf("splits = %s", got)
	}
	if f.value(rawRecord("2026-10-02 08:00")) != unitStarted {
		t.Error("unit not left started")
	}
	if f.value(watermarkRecord) != "" {
		t.Error("watermark advanced")
	}
}

func TestOtherErrorsFailTheRunWithoutRetrying(t *testing.T) {
	for name, fail := range map[string]error{
		"other code":  limitHit(60),
		"rows limit":  limitHit(307),
		"plain error": errors.New("connection reset"),
	} {
		f, opts := newRun("2026-10-02 08:00")
		f.on = func(_ context.Context, s Statement) (any, error) {
			if s.Op == opCopy {
				return nil, fail
			}
			return nil, nil
		}
		if _, err := migrate(t, f, opts); !errors.Is(err, fail) {
			t.Fatalf("%s: err = %v", name, err)
		}
		if got := fmt.Sprint(copySplits(f, "2026-10-02 09:00")); got != "[1]" {
			t.Errorf("%s: splits = %s", name, got)
		}
		if f.value(rawRecord("2026-10-02 09:00")) != unitStarted {
			t.Errorf("%s: unit not left started", name)
		}
	}
}

func TestALimitInAWholeHourStepFailsTheRunWithoutRetrying(t *testing.T) {
	for _, op := range []string{opCount, opDelete} {
		f, opts := newRun("2026-10-02 08:00")
		f.on = func(_ context.Context, s Statement) (any, error) {
			if s.Op == op {
				return nil, limitHit(241)
			}
			return nil, nil
		}
		if op == opDelete {
			f.on = recoveryRawGrowth(10, 17, 3, f.on)
		}
		if _, err := migrate(t, f, opts); err == nil {
			t.Fatalf("%s: run succeeded", op)
		}
		n := 0
		for _, s := range splitSteps(f, "2026-10-02 09:00") {
			if strings.HasPrefix(s, op) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s ran %d times", op, n)
		}
	}
}

func TestASplitPartSelectsItsShareOfTheSeries(t *testing.T) {
	u := migrationUnit{kind: unitRaw, from: at("2026-10-02 08:00"), to: at("2026-10-02 09:00")}
	for name, sql := range map[string]string{
		"copy":    localTables.copyRawSQL(u, 4, 3),
		"rebuild": localTables.rebuildTierSQL(metricTiers[0], u, 4, 3),
	} {
		if !strings.Contains(sql, "fingerprint >= 13835058055282163712") {
			t.Errorf("%s: %s", name, sql)
		}
	}
	if strings.Contains(localTables.copyRawSQL(u, 1, 0), "fingerprint >=") {
		t.Error("whole copy is split")
	}
}

func TestSplitPartsAreContiguousFingerprintRangesCoveringTheSpace(t *testing.T) {
	re := regexp.MustCompile(`^ AND fingerprint >= (\d+)(?: AND fingerprint < (\d+))?$`)
	space := new(big.Int).Lsh(big.NewInt(1), 64)
	for _, n := range []int{1, 2, 6, 16} {
		next := new(big.Int)
		for p := 0; p < n; p++ {
			w := splitWhere(n, p)
			if n == 1 {
				if w != "" {
					t.Errorf("n=1: %q", w)
				}
				next.Set(space)
				break
			}
			m := re.FindStringSubmatch(w)
			if m == nil {
				t.Fatalf("n=%d part %d: %q", n, p, w)
			}
			lo, _ := new(big.Int).SetString(m[1], 10)
			if lo.Cmp(next) != 0 {
				t.Errorf("n=%d part %d starts at %s, want %s", n, p, lo, next)
			}
			if m[2] == "" {
				if p != n-1 {
					t.Errorf("n=%d part %d is open-ended", n, p)
				}
				next.Set(space)
				continue
			}
			hi, _ := new(big.Int).SetString(m[2], 10)
			if hi.Cmp(lo) <= 0 || hi.Cmp(space) >= 0 {
				t.Errorf("n=%d part %d: [%s, %s)", n, p, lo, hi)
			}
			next = hi
		}
		if next.Cmp(space) != 0 {
			t.Errorf("n=%d covers up to %s", n, next)
		}
	}
	if got := splitWhere(2, 1); got != " AND fingerprint >= 9223372036854775808" {
		t.Errorf("upper half = %q", got)
	}
}

func TestRawFinalReadsFilterTheHourInPrewhere(t *testing.T) {
	u := migrationUnit{kind: unitRaw, from: at("2026-10-02 08:00"), to: at("2026-10-02 09:00")}
	for name, sql := range map[string]string{
		"count":   localTables.countRawSQL(u),
		"rebuild": localTables.rebuildTierSQL(metricTiers[0], u, 2, 1),
	} {
		if !strings.Contains(sql, "FINAL PREWHERE timestamp > ") || strings.Contains(sql, "FINAL WHERE") {
			t.Errorf("%s: %s", name, sql)
		}
	}
	if !strings.Contains(localTables.rebuildTierSQL(metricTiers[0], u, 2, 1), "AND fingerprint >= 9223372036854775808))") {
		t.Error("split part is outside the rebuild's PREWHERE")
	}
}

func TestALimitWarningNamesTheCodeAndTheSetting(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	opts.MaxSplit = 2
	var logged bytes.Buffer
	l := logrus.New()
	l.SetOutput(&logged)
	opts.Logger = l
	f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCopy {
			return nil, limitHit(241)
		}
		return nil, nil
	}
	if _, err := migrate(t, f, opts); err == nil {
		t.Fatal("run did not fail")
	}
	out := logged.String()
	for _, want := range []string{
		"hit max_memory_usage (code 241); retrying split 2",
		"hit max_memory_usage (code 241) at the maximum split 2; giving up",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
}

func TestASubSecondExecutionTimeRoundsUpToWholeSeconds(t *testing.T) {
	for d, want := range map[time.Duration]int{
		time.Millisecond:        1,
		500 * time.Millisecond:  1,
		1500 * time.Millisecond: 2,
		10 * time.Minute:        600,
	} {
		f, opts := newRun("2026-10-02 08:00")
		opts.MaxExecutionTime = d
		f.on = recoveryRawGrowth(10, 17, 3, nil)
		if _, err := migrate(t, f, opts); err != nil {
			t.Fatal(err)
		}
		requireTierSteps(t, limitedStatements(f))
		for _, s := range limitedStatements(f) {
			if s.Settings["max_execution_time"] != want {
				t.Errorf("%s: %s max_execution_time = %v, want %d", d, step(s), s.Settings["max_execution_time"], want)
			}
		}
	}
}

func TestAWrappedLimitErrorRetriesSmaller(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	failed := false
	f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCopy && !failed {
			failed = true
			return nil, fmt.Errorf("insert: %w", limitHit(159))
		}
		return nil, nil
	}
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(copySplits(f, "2026-10-02 09:00")); got != "[1 2]" {
		t.Errorf("splits = %s", got)
	}
}
