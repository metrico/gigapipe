package traceql_transpiler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// fakeWarner is a warner over a fake clock, probe and logger.
type fakeWarner struct {
	*unresolvedWarner
	clock  time.Time
	probes int
	found  []unresolvedFound
	err    error
	warns  []string
	debugs int
}

func newFakeWarner() *fakeWarner {
	f := &fakeWarner{clock: time.Unix(1000, 0)}
	f.unresolvedWarner = &unresolvedWarner{
		now: func() time.Time { return f.clock },
		probe: func(context.Context, *traceql_ir.Query, *shared.PlannerContext) ([]unresolvedFound, error) {
			f.probes++
			return f.found, f.err
		},
		warn:    func(s string) { f.warns = append(f.warns, s) },
		debug:   func(...any) { f.debugs++ },
		timeout: time.Second,
	}
	return f
}

func (f *fakeWarner) runProbe() {
	f.run(context.Background(), nil, &shared.PlannerContext{AncestryRounds: 3})
}

func TestUnresolvedWarningLine(t *testing.T) {
	f := newFakeWarner()
	f.found = []unresolvedFound{{Jump: "j", N: 2, IDs: []string{"2a", "29"}}}
	f.runProbe()
	want := "TraceQL structural query: 2 trace(s) have unresolved ancestry " +
		"(deeper than 2^3 spans at GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS=3, or parent links form a cycle); " +
		"their unresolved spans are treated as unreachable; trace_ids=[29, 2a]"
	if len(f.warns) != 1 || f.warns[0] != want {
		t.Errorf("warns = %q, want %q", f.warns, want)
	}
}

func TestUnresolvedWarningIsSilentWhenNothingIsFound(t *testing.T) {
	for name, found := range map[string][]unresolvedFound{
		"no rows":     nil,
		"zero counts": {{Jump: "j"}, {Jump: "k"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeWarner()
			f.found = found
			f.runProbe()
			if len(f.warns) != 0 {
				t.Errorf("warns = %q", f.warns)
			}
		})
	}
}

// The count sums a jump's rows, whose traces are disjoint, and takes the
// largest jump, at least the distinct ids listed.
func TestUnresolvedWarningMergesRows(t *testing.T) {
	for _, c := range []struct {
		name  string
		found []unresolvedFound
		n     int
		ids   string
	}{
		{
			name: "portions of one jump add up",
			found: []unresolvedFound{
				{Jump: "j", N: 1, IDs: []string{"a"}},
				{Jump: "j", N: 2, IDs: []string{"b", "c"}},
			},
			n: 3, ids: "a, b, c",
		},
		{
			name: "two shards listing one id list it once",
			found: []unresolvedFound{
				{Jump: "j", N: 2, IDs: []string{"a", "b"}},
				{Jump: "j", N: 2, IDs: []string{"a", "c"}},
			},
			n: 4, ids: "a, b, c",
		},
		{
			name: "jumps sharing traces take the largest",
			found: []unresolvedFound{
				{Jump: "j", N: 3, IDs: []string{"a", "b", "c"}},
				{Jump: "k", N: 1, IDs: []string{"c"}},
				{Jump: "l", N: 2, IDs: []string{"a", "c"}},
			},
			n: 3, ids: "a, b, c",
		},
		{
			name: "distinct ids across jumps floor the count",
			found: []unresolvedFound{
				{Jump: "j", N: 1, IDs: []string{"a"}},
				{Jump: "k", N: 1, IDs: []string{"b"}},
			},
			n: 2, ids: "a, b",
		},
		{
			name: "jumps with the same traces list each once",
			found: []unresolvedFound{
				{Jump: "j", N: 2, IDs: []string{"b", "a"}},
				{Jump: "k", N: 2, IDs: []string{"a", "b"}},
			},
			n: 2, ids: "a, b",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeWarner()
			f.found = c.found
			f.runProbe()
			if len(f.warns) != 1 {
				t.Fatalf("warns = %q", f.warns)
			}
			if !strings.Contains(f.warns[0], fmt.Sprintf(": %d trace(s)", c.n)) {
				t.Errorf("want %d traces: %s", c.n, f.warns[0])
			}
			if !strings.HasSuffix(f.warns[0], "trace_ids=["+c.ids+"]") {
				t.Errorf("want ids %s: %s", c.ids, f.warns[0])
			}
		})
	}
}

func TestUnresolvedWarningCapsIDs(t *testing.T) {
	var found []unresolvedFound
	for i := range 5 {
		var ids []string
		for j := range 5 {
			ids = append(ids, fmt.Sprintf("%d%d", i, j))
		}
		found = append(found, unresolvedFound{Jump: "j", N: 5, IDs: ids})
	}
	f := newFakeWarner()
	f.found = found
	f.runProbe()
	if len(f.warns) != 1 {
		t.Fatalf("warns = %q", f.warns)
	}
	if !strings.Contains(f.warns[0], ": 25 trace(s)") {
		t.Errorf("want 25 traces: %s", f.warns[0])
	}
	_, list, _ := strings.Cut(f.warns[0], "trace_ids=[")
	if got := strings.Count(list, ",") + 1; got != 20 {
		t.Errorf("listed %d ids: %s", got, list)
	}
	if !strings.HasPrefix(list, "00, 01,") || !strings.HasSuffix(list, "33, 34]") {
		t.Errorf("want the first ids in order: %s", list)
	}
}

func TestUnresolvedProbeErrorKeepsWhatWasFound(t *testing.T) {
	f := newFakeWarner()
	f.err = errors.New("late portion")
	f.runProbe()
	if len(f.warns) != 0 || f.debugs != 1 {
		t.Errorf("nothing found: warns = %q, debugs = %d", f.warns, f.debugs)
	}

	f = newFakeWarner()
	f.err = errors.New("late portion")
	f.found = []unresolvedFound{{Jump: "j", N: 1, IDs: []string{"a"}}}
	f.runProbe()
	if len(f.warns) != 1 || f.debugs != 1 {
		t.Errorf("found before the error: warns = %q, debugs = %d", f.warns, f.debugs)
	}
}

func TestUnresolvedProbeRunsOnceAMinute(t *testing.T) {
	for name, setup := range map[string]func(*fakeWarner){
		"finding nothing": func(*fakeWarner) {},
		"failing":         func(f *fakeWarner) { f.err = errors.New("failed") },
		"warning": func(f *fakeWarner) {
			f.found = []unresolvedFound{{Jump: "j", N: 1, IDs: []string{"a"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeWarner()
			setup(f)
			f.runProbe()
			f.clock = f.clock.Add(59 * time.Second)
			f.runProbe()
			if f.probes != 1 {
				t.Fatalf("probes within the minute = %d", f.probes)
			}
			f.clock = f.clock.Add(time.Second)
			f.runProbe()
			if f.probes != 2 {
				t.Errorf("probes after the minute = %d", f.probes)
			}
		})
	}
}

func structuralQuery(t *testing.T) *traceql_ir.Query {
	t.Helper()
	q, err := traceql_ir.Parse(`{.a = "1"} >> {.b = "1"}`)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// The probe starts when the request context ends, on a context of its own
// that keeps the request's values.
func TestUnresolvedProbeStartsAfterTheRequest(t *testing.T) {
	type key struct{}
	type started struct {
		err         error
		hasDeadline bool
		value       any
		window      time.Time
	}
	ch := make(chan started, 1)
	f := newFakeWarner()
	f.timeout = time.Hour
	f.probe = func(ctx context.Context, _ *traceql_ir.Query, planCtx *shared.PlannerContext) ([]unresolvedFound, error) {
		_, hasDeadline := ctx.Deadline()
		ch <- started{ctx.Err(), hasDeadline, ctx.Value(key{}), planCtx.From}
		return nil, nil
	}

	reqCtx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "plugin"))
	defer cancel()
	from := time.Unix(100, 0)
	planCtx := &shared.PlannerContext{From: from}
	f.after(reqCtx, structuralQuery(t), planCtx)
	// The request may go on using its context after registering.
	planCtx.From = time.Unix(200, 0)

	select {
	case <-ch:
		t.Fatal("probe started before the request ended")
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	got := <-ch
	if got.err != nil || !got.hasDeadline || got.value != "plugin" {
		t.Errorf("probe context: err = %v, deadline = %v, value = %v", got.err, got.hasDeadline, got.value)
	}
	if !got.window.Equal(from) {
		t.Errorf("probe from = %v, want %v", got.window, from)
	}
}

// A panic on the probe's goroutine stays there and reports like a probe error.
func TestUnresolvedProbePanicIsReported(t *testing.T) {
	reported := make(chan string, 1)
	f := newFakeWarner()
	f.probe = func(context.Context, *traceql_ir.Query, *shared.PlannerContext) ([]unresolvedFound, error) {
		panic("boom")
	}
	f.debug = func(args ...any) { reported <- fmt.Sprint(args...) }

	reqCtx, cancel := context.WithCancel(context.Background())
	f.after(reqCtx, structuralQuery(t), &shared.PlannerContext{})
	cancel()

	if got := <-reported; !strings.Contains(got, "boom") {
		t.Errorf("reported %q", got)
	}
}

func TestUnresolvedProbeNeedsAStructuralQuery(t *testing.T) {
	plain, err := traceql_ir.Parse(`{.a = "1"}`)
	if err != nil {
		t.Fatal(err)
	}
	for name, query := range map[string]*traceql_ir.Query{"no query": nil, "plain": plain} {
		t.Run(name, func(t *testing.T) {
			ran := make(chan struct{}, 1)
			f := newFakeWarner()
			f.probe = func(context.Context, *traceql_ir.Query, *shared.PlannerContext) ([]unresolvedFound, error) {
				ran <- struct{}{}
				return nil, nil
			}
			reqCtx, cancel := context.WithCancel(context.Background())
			f.after(reqCtx, query, &shared.PlannerContext{})
			cancel()
			select {
			case <-ran:
				t.Error("probe ran")
			case <-time.After(10 * time.Millisecond):
			}
		})
	}
}

// Each portion reads its own hash of traces, over the probe's context.
func TestUnresolvedPortionContext(t *testing.T) {
	type key struct{}
	probeCtx := context.WithValue(context.Background(), key{}, "probe")
	planCtx := &shared.PlannerContext{
		Ctx: context.Background(), Portions: 3,
		RandomFilter: shared.RandomFilter{Max: 3, I: 2},
	}
	got := unresolvedPortion(probeCtx, planCtx, 1)
	if got.RandomFilter != (shared.RandomFilter{Max: 3, I: 1}) || got.Ctx != probeCtx {
		t.Errorf("portion = %+v", got)
	}
	if planCtx.RandomFilter.I != 2 {
		t.Errorf("plan context changed: %+v", planCtx)
	}
	for _, portions := range []int64{0, 1} {
		planCtx.Portions = portions
		if got := unresolvedPortion(probeCtx, planCtx, 0); got.RandomFilter != (shared.RandomFilter{}) {
			t.Errorf("%d portions: filter = %+v", portions, got.RandomFilter)
		}
	}
}
