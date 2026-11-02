package maintenance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// gateLogger records each logged line.
type gateLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *gateLogger) Error(args ...any) { l.add(args) }
func (l *gateLogger) Debug(args ...any) { l.add(args) }
func (l *gateLogger) Info(args ...any)  { l.add(args) }

func (l *gateLogger) add(args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprint(args...))
}

// matching returns the lines containing every part.
func (l *gateLogger) matching(parts ...string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range l.lines {
		ok := true
		for _, p := range parts {
			ok = ok && strings.Contains(line, p)
		}
		if ok {
			out = append(out, line)
		}
	}
	return out
}

// gateRun is a run whose sleeps advance its clock and are kept in order.
type gateRun struct {
	f      *fakeExec
	opts   MetricMigrationOptions
	clock  *testClock
	log    *gateLogger
	sleeps []time.Duration
}

func newGateRun(floor string) *gateRun {
	f, opts := newRun(floor)
	r := &gateRun{f: f, opts: opts, clock: &testClock{at(testNow)}, log: &gateLogger{}}
	r.f.records = newMemRecords(r.clock)
	r.f.record(floorRecord, ms(at(floor)))
	r.opts.now = r.clock.now
	r.opts.Logger = r.log
	r.opts.sleep = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.sleeps = append(r.sleeps, d)
		r.clock.t = r.clock.t.Add(d)
		return nil
	}
	return r
}

// gateRows is one gate answer: every signal at rest except over, which reads value against
// limit and names cloki.busy (and mutation 0000000042) where its signal carries a table.
func gateRows(over string, value, limit float64) [][]any {
	rows := [][]any{
		{"parts", 10.0, 300.0, "", ""},
		{"merges", 2.0, 0.0, "", ""},
		{"mutations", 0.0, 0.0, "", ""},
		{"delayed", 0.0, 0.0, "", ""},
		{"memory", 1e9, 8e9, "", ""},
	}
	for _, r := range rows {
		if r[0] == over {
			r[1], r[2] = value, limit
		}
		if r[0] == over && (over == "merges" || over == "mutations") {
			r[3] = "cloki.busy"
		}
		if r[0] == over && over == "mutations" {
			r[4] = "0000000042"
		}
	}
	return rows
}

// gateSleeps counts the gate's 10 s polls.
func (r *gateRun) gateSleeps() int {
	n := 0
	for _, d := range r.sleeps {
		if d == 10*time.Second {
			n++
		}
	}
	return n
}

func TestTheGateWaitsWhileASignalIsOverItsThresholdAndResumesBelowIt(t *testing.T) {
	for _, c := range []struct {
		signal      string
		over, under float64
		limit       float64
		threshold   string
	}{
		{signal: "parts", over: 150, under: 149, limit: 300, threshold: "150"},
		{signal: "merges", over: 9, under: 8, threshold: "8"},
		{signal: "mutations", over: 1, under: 0, threshold: "0"},
		{signal: "delayed", over: 1, under: 0, threshold: "0"},
		{signal: "memory", over: 8e9, under: 7.9e9, limit: 10e9, threshold: "7.5 GiB"},
	} {
		t.Run(c.signal, func(t *testing.T) {
			r := newGateRun("2026-10-02 08:00")
			polls := 0
			r.f.on = func(_ context.Context, s Statement) (any, error) {
				if s.Op == opGateTable {
					return []any{"cloki.busy", 200.0}, nil
				}
				if s.Op != opGate {
					return nil, nil
				}
				polls++
				if polls <= 3 {
					return gateRows(c.signal, c.over, c.limit), nil
				}
				return gateRows(c.signal, c.under, c.limit), nil
			}
			if _, err := migrate(t, r.f, r.opts); err != nil {
				t.Fatal(err)
			}
			if got := r.gateSleeps(); got != 3 {
				t.Errorf("gate polls slept %d times, want 3", got)
			}
			if got := r.log.matching("metric migration:", "back-off on", c.signal, c.threshold); len(got) != 1 {
				t.Errorf("start lines = %q", got)
			}
			if got := r.log.matching("metric migration:", "resumed after 30s"); len(got) != 1 {
				t.Errorf("resume lines = %q", got)
			}
		})
	}
}

func TestBackOffLinesNameWhatHoldsTheGateOnStartAndEveryMinute(t *testing.T) {
	for _, c := range []struct {
		signal string
		value  float64
		limit  float64
		lines  []string
	}{
		{signal: "parts", value: 150, limit: 300, lines: []string{
			"metric migration: back-off on parts: 150 at threshold 150, table cloki.busy",
			"metric migration: back-off on parts: 150 at threshold 150, table cloki.busy, waited 1m0s",
		}},
		{signal: "merges", value: 17, lines: []string{
			"metric migration: back-off on merges: 17 at threshold 8, table cloki.busy",
			"metric migration: back-off on merges: 17 at threshold 8, table cloki.busy, waited 1m0s",
		}},
		{signal: "mutations", value: 1, lines: []string{
			"metric migration: back-off on mutations: 1 at threshold 0, table cloki.busy, mutation 0000000042",
			"metric migration: back-off on mutations: 1 at threshold 0, table cloki.busy, mutation 0000000042, waited 1m0s",
		}},
		{signal: "delayed", value: 1, lines: []string{
			"metric migration: back-off on delayed: 1 at threshold 0",
			"metric migration: back-off on delayed: 1 at threshold 0, waited 1m0s",
		}},
		{signal: "memory", value: 8e9, limit: 10e9, lines: []string{
			"metric migration: back-off on memory: 7.5 GiB at threshold 7.5 GiB",
			"metric migration: back-off on memory: 7.5 GiB at threshold 7.5 GiB, waited 1m0s",
		}},
	} {
		t.Run(c.signal, func(t *testing.T) {
			r := newGateRun("2026-10-02 08:00")
			polls := 0
			r.f.on = func(_ context.Context, s Statement) (any, error) {
				if s.Op == opGateTable {
					return []any{"cloki.busy", 200.0}, nil
				}
				if s.Op != opGate {
					return nil, nil
				}
				polls++
				if polls <= 7 {
					return gateRows(c.signal, c.value, c.limit), nil
				}
				return gateRows("", 0, 0), nil
			}
			if _, err := migrate(t, r.f, r.opts); err != nil {
				t.Fatal(err)
			}
			sameStrings(t, "back-off lines", r.log.matching("back-off"),
				append(c.lines, "metric migration: back-off resumed after 1m10s"))
		})
	}
}

func TestAPartsTableHoldingFewerPartsThanTheSignalIsNotVisible(t *testing.T) {
	for _, answer := range [][]any{{"cloki.small", 3.0}, {"", 0.0}} {
		t.Run(fmt.Sprint(answer), func(t *testing.T) {
			r := newGateRun("2026-10-02 08:00")
			polls := 0
			r.f.on = func(_ context.Context, s Statement) (any, error) {
				if s.Op == opGateTable {
					return answer, nil
				}
				if s.Op != opGate {
					return nil, nil
				}
				polls++
				if polls <= 1 {
					return gateRows("parts", 150, 300), nil
				}
				return gateRows("", 0, 0), nil
			}
			if _, err := migrate(t, r.f, r.opts); err != nil {
				t.Fatal(err)
			}
			sameStrings(t, "back-off lines", r.log.matching("back-off on"), []string{
				"metric migration: back-off on parts: 150 at threshold 150, table not visible to this user",
			})
		})
	}
}

func TestAFailingMutationDoesNotHoldTheGate(t *testing.T) {
	sql := gateSQL()
	row := sql[strings.Index(sql, "'mutations'"):]
	row = row[:strings.Index(row, "UNION ALL")]
	for _, want := range []string{"NOT is_done", "latest_fail_reason = ''", "argMin(", "mutation_id", "create_time"} {
		if !strings.Contains(row, want) {
			t.Errorf("mutations signal %q lacks %q", row, want)
		}
	}
}

func TestALongBackOffLogsTheCurrentValueEveryMinute(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	polls := 0
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op != opGate {
			return nil, nil
		}
		polls++
		if polls <= 19 {
			return gateRows("merges", float64(16+polls), 0), nil
		}
		return gateRows("merges", 3, 0), nil
	}
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "back-off lines", r.log.matching("back-off"), []string{
		"metric migration: back-off on merges: 17 at threshold 8, table cloki.busy",
		"metric migration: back-off on merges: 23 at threshold 8, table cloki.busy, waited 1m0s",
		"metric migration: back-off on merges: 29 at threshold 8, table cloki.busy, waited 2m0s",
		"metric migration: back-off on merges: 35 at threshold 8, table cloki.busy, waited 3m0s",
		"metric migration: back-off resumed after 3m10s",
	})
}

func TestACancelDuringABackOffStopsTheRunWithTheUnitStarted(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := rawRecord("2026-10-02 09:00")
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op != opGate || r.f.value(u) != unitStarted {
			return nil, nil
		}
		if r.gateSleeps() == 2 {
			cancel()
		}
		return gateRows("delayed", 4, 0), nil
	}
	_, err := RunMetricMigration(ctx, r.f, r.opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a failed run cancelled", err)
	}
	if !strings.HasPrefix(failed.Step, "unit 1/1 raw (") {
		t.Errorf("step = %q, want the unit", failed.Step)
	}
	if got := r.f.value(u); got != unitStarted {
		t.Errorf("unit record = %q, want started", got)
	}
	kills := 0
	for _, s := range r.f.statements() {
		if s.Op == opKill {
			kills++
		}
	}
	if kills != 1 {
		t.Errorf("%d kills, want 1", kills)
	}
	if r.f.value(leaseRecord) != "" {
		t.Error("lease not released")
	}
}

func TestTheKillRunsWithoutTheGateWhileTheGateHolds(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var killCtxErr error
	r.f.on = func(qctx context.Context, s Statement) (any, error) {
		switch s.Op {
		case opCopy:
			cancel()
			return nil, qctx.Err()
		case opKill:
			killCtxErr = qctx.Err()
		case opGate:
			if ctx.Err() != nil {
				return gateRows("mutations", 1, 0), nil
			}
		}
		return nil, nil
	}
	if _, err := RunMetricMigration(ctx, r.f, r.opts); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	ss := r.f.statements()
	kills := 0
	for i, s := range ss {
		if s.Op != opKill {
			continue
		}
		kills++
		if i > 0 && ss[i-1].Op == opGate {
			t.Error("the kill ran behind the gate")
		}
		if len(s.Settings) != 0 {
			t.Errorf("kill carries settings %v", s.Settings)
		}
	}
	if kills != 1 || killCtxErr != nil {
		t.Errorf("%d kills, kill ctx err %v", kills, killCtxErr)
	}
}

func TestThePauseRunsBetweenUnitsUnlogged(t *testing.T) {
	r := newGateRun("2026-10-02 06:00")
	r.opts.Pause = 7 * time.Second
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	pauses := 0
	for _, d := range r.sleeps {
		if d == 7*time.Second {
			pauses++
		}
	}
	if pauses != 2 {
		t.Errorf("paused %d times between 3 units, want 2", pauses)
	}
	if got := r.log.matching("metric migration:"); len(got) != 3 {
		t.Errorf("lines = %q, want the 3 unit lines", got)
	}
}

func TestACancelDuringThePauseFailsTheRunAtPauseWithTheUnitDone(t *testing.T) {
	r := newGateRun("2026-10-02 06:00")
	r.opts.Pause = 7 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sleep := r.opts.sleep
	r.opts.sleep = func(ctx context.Context, d time.Duration) error {
		if d == r.opts.Pause {
			cancel()
		}
		return sleep(ctx, d)
	}
	_, err := RunMetricMigration(ctx, r.f, r.opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) || failed.Step != "pause" || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a cancel failed at pause", err)
	}
	if got := r.f.value(rawRecord("2026-10-02 09:00")); got != unitDone {
		t.Errorf("first unit record = %q, want done", got)
	}
	if got := r.f.value(rawRecord("2026-10-02 08:00")); got != "" {
		t.Errorf("second unit record = %q, want none", got)
	}
}

func TestTheGateReportsSignalsInAFixedOrder(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	polls := 0
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op != opGate {
			return nil, nil
		}
		polls++
		if polls > 7 {
			return gateRows("", 0, 0), nil
		}
		rows := gateRows("merges", 20, 0)
		rows[0] = []any{"parts", 200.0, 300.0, "", ""}
		if polls%2 == 1 {
			rows[0], rows[1] = rows[1], rows[0]
		}
		return rows, nil
	}
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	if got := r.log.matching("back-off on merges"); len(got) != 0 {
		t.Errorf("merges lines = %q, want parts reported first", got)
	}
	if got := r.log.matching("back-off on parts"); len(got) != 2 {
		t.Errorf("parts lines = %q, want start and one minute line", got)
	}
}

func TestTheGateLooksUpTheBusiestPartsTableOnlyWhenPartsHoldIt(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	polls := 0
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		switch s.Op {
		case opGateTable:
			return []any{"cloki.busy", 200.0}, nil
		case opGate:
			polls++
			if polls > 2 && polls <= 4 {
				return gateRows("parts", 200, 300), nil
			}
			return gateRows("", 0, 0), nil
		}
		return nil, nil
	}
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	lookups := 0
	for _, s := range r.f.log {
		if s.Op == opGateTable {
			lookups++
		}
	}
	if lookups != 1 {
		t.Errorf("parts table looked up %d times, want once at the start of the wait", lookups)
	}
	if got := r.log.matching("back-off on parts", "table cloki.busy"); len(got) != 1 {
		t.Errorf("start lines = %q", got)
	}
}

func TestMemoryBackOffLogsBytesInBinaryUnits(t *testing.T) {
	for _, c := range []struct {
		n    float64
		want string
	}{
		{12884901888, "12.0 GiB"},
		{3 << 20, "3.0 MiB"},
		{1536, "1.5 KiB"},
		{512, "512 B"},
	} {
		if got := formatBytes(c.n); got != c.want {
			t.Errorf("formatBytes(%g) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestEachFinishedUnitLogsItsPositionKindRangeRowsAndDuration(t *testing.T) {
	r := newGateRun("2026-10-02 07:00")
	counts := 0
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCount && s.QueryID == unitQueryID("cloki", migrationUnit{kind: unitRaw, to: at("2026-10-02 09:00")}) {
			counts++
			if counts == 2 {
				r.clock.t = r.clock.t.Add(90 * time.Second)
				return 40, nil
			}
		}
		return nil, nil
	}
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	if got := r.log.matching("metric migration: 1/2 raw (2026-10-02T08:00:00Z, 2026-10-02T09:00:00Z]", "40 rows", "1m30s"); len(got) != 1 {
		t.Errorf("first unit line missing: %q", r.log.matching("metric migration:"))
	}
	if got := r.log.matching("metric migration: 2/2 raw (2026-10-02T07:00:00Z, 2026-10-02T08:00:00Z]", "0 rows"); len(got) != 1 {
		t.Errorf("second unit line missing: %q", r.log.matching("metric migration:"))
	}
}

func TestTheGateRunsBeforeEveryStatementButTheRecords(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	gated := 0
	prev := ""
	for _, s := range r.f.statements() {
		switch s.Op {
		case opGate:
		case opRecord:
			if prev == opGate {
				t.Errorf("gate before %s", step(s))
			}
		default:
			gated++
			if prev != opGate {
				t.Errorf("%s ran without the gate", step(s))
			}
		}
		prev = s.Op
	}
	if gated == 0 {
		t.Error("no gated statements")
	}
}

// progressEach has the copy report rows read once per step of its clock.
func (r *gateRun) progressEach(step time.Duration, reads ...uint64) {
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCopy {
			for _, n := range reads {
				r.clock.t = r.clock.t.Add(step)
				s.Progress(n, 0)
			}
		}
		return nil, nil
	}
}

func TestAUnitRunningPastAMinuteLogsRowsReadEveryMinute(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	r.progressEach(30*time.Second, 100, 200, 300, 400, 500)
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	id := unitQueryID("cloki", migrationUnit{kind: unitRaw, to: at("2026-10-02 09:00")})
	sameStrings(t, "running lines", r.log.matching("running"), []string{
		"metric migration: " + id + " copy metric_samples running 1m0s: 200 rows read",
		"metric migration: " + id + " copy metric_samples running 2m0s: 400 rows read",
	})
}

func TestAUnitUnderAMinuteLogsNoRunningLine(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	r.progressEach(10*time.Second, 100, 200, 300, 400, 500)
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	if got := r.log.matching("running"); len(got) != 0 {
		t.Errorf("running lines = %q", got)
	}
}

func TestEveryUnitStepSplitPartAndRunStatementPassesTheGateAndLogsItsHeartbeat(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	r.opts.RollupDays = 30
	u := migrationUnit{kind: unit15s, from: at("2026-10-02 05:00"), to: at("2026-10-02 06:00")}
	id := unitQueryID("cloki", u)
	failed := false
	r.f.on = recoveryRawGrowth(10, 17, 3, func(ctx context.Context, s Statement) (any, error) {
		switch {
		case s.Op == opRebuild && s.QueryID == id && !failed:
			failed = true
			return nil, limitHit(241)
		case s.Op == opRebuild && s.QueryID == id && s.Table == metricTiers[0].Table && s.Parts == 2 && s.Part == 1,
			s.Op == opSeries:
			r.clock.t = r.clock.t.Add(61 * time.Second)
			s.Progress(500, 0)
		case s.Op == opExists:
			return uint8(1), nil
		case s.Op == op15sHours:
			return [][]any{{at("2026-10-02 06:00").UnixMilli() / time.Hour.Milliseconds()}}, nil
		}
		return nil, nil
	})
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	prev := ""
	for _, s := range r.f.statements() {
		switch s.Op {
		case opGate, opRecord, opRecords, opRecordAge:
		default:
			if s.Parts > 1 {
				seen[fmt.Sprintf("%s part", s.Op)] = true
			}
			seen[s.Op] = true
			if prev != opGate {
				t.Errorf("%s ran without the gate", step(s))
			}
		}
		prev = s.Op
	}
	for _, op := range []string{opSetting, opExists, op15sHours, opCount, opCopy, "rebuild part", opDelete, opRebuild, opCheck, opSeries, opMetadata} {
		if !seen[op] {
			t.Errorf("no gated %s", op)
		}
	}
	if got := r.log.matching(id+" rebuild ", "running 1m1s: 500 rows read"); len(got) != 1 {
		t.Errorf("split part heartbeat = %q", r.log.matching("rows read"))
	}
	if got := r.log.matching(runQueryID("cloki", opSeries)+" series ", "500 rows read"); len(got) != 1 {
		t.Errorf("series heartbeat = %q", r.log.matching("rows read"))
	}
	if got := r.log.matching("metric migration: 2/2 " + unitLabel(u) + ":"); len(got) != 1 {
		t.Errorf("15s unit line missing: %q", r.log.matching("metric migration:"))
	}
}
