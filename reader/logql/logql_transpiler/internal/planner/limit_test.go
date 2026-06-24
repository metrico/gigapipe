package planner

import (
	"context"
	"testing"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type passthroughProcessor struct {
	in chan []shared.LogEntry
}

func (p *passthroughProcessor) Process(_ *shared.PlannerContext, _ chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return p.in, nil
}

func (p *passthroughProcessor) IsMatrix() bool { return false }

func feedEntries(n int) chan []shared.LogEntry {
	ch := make(chan []shared.LogEntry, 1)
	go func() {
		entries := make([]shared.LogEntry, n)
		for i := range entries {
			entries[i] = shared.LogEntry{TimestampNS: int64(i)}
		}
		ch <- entries
		close(ch)
	}()
	return ch
}

func collectEntries(ch chan []shared.LogEntry) []shared.LogEntry {
	var result []shared.LogEntry
	for batch := range ch {
		result = append(result, batch...)
	}
	return result
}

func TestLimitPlannerZeroMeansNoLimit(t *testing.T) {
	ctx := &shared.PlannerContext{Limit: 0, Ctx: context.Background()}
	in := feedEntries(500)
	p := &LimitPlanner{GenericPlanner{&passthroughProcessor{in}}}
	out, err := p.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEntries(out)
	if len(got) != 500 {
		t.Errorf("limit=0 should return all entries; got %d, want 500", len(got))
	}
}

func TestLimitPlannerRespectsNonZeroLimit(t *testing.T) {
	ctx := &shared.PlannerContext{Limit: 10, Ctx: context.Background()}
	in := feedEntries(500)
	p := &LimitPlanner{GenericPlanner{&passthroughProcessor{in}}}
	out, err := p.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEntries(out)
	if len(got) != 10 {
		t.Errorf("limit=10 should return 10 entries; got %d", len(got))
	}
}

func TestLimitPlannerLimitLargerThanInput(t *testing.T) {
	ctx := &shared.PlannerContext{Limit: 1000, Ctx: context.Background()}
	in := feedEntries(50)
	p := &LimitPlanner{GenericPlanner{&passthroughProcessor{in}}}
	out, err := p.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEntries(out)
	if len(got) != 50 {
		t.Errorf("limit=1000 with 50 entries should return 50; got %d", len(got))
	}
}
