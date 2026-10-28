package logql_transpiler

import (
	"context"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

type chanProcessor struct {
	ch chan []shared.LogEntry
}

func (p *chanProcessor) Process(*shared.PlannerContext, chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return p.ch, nil
}

func (p *chanProcessor) IsMatrix() bool { return true }

// A zero step fails the query with an error instead of dividing by zero in a
// planner goroutine.
func TestGridRejectsZeroStep(t *testing.T) {
	g := &GridPlanner{Main: &chanProcessor{make(chan []shared.LogEntry)}, Duration: time.Second}
	ctx := &shared.PlannerContext{Ctx: context.Background(), From: time.Unix(0, 0), To: time.Unix(60, 0)}
	if _, err := g.Process(ctx, nil); err == nil {
		t.Fatal("want an error for a zero step")
	}
}
