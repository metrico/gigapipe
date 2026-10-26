package logql_transpiler

import (
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

func TestMatrixStepDrainsUpstreamOnPanic(t *testing.T) {
	in := make(chan []shared.LogEntry)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 3; i++ {
			in <- []shared.LogEntry{{TimestampNS: int64(i)}}
		}
		close(in)
	}()

	m := &MatrixStepPlanner{Main: &chanProcessor{in}, Duration: time.Second}
	// A nil context makes the step loop panic on the first entry.
	out, err := m.Process(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	var got []shared.LogEntry
	for batch := range out {
		got = append(got, batch...)
	}
	if len(got) != 1 || got[0].Err == nil {
		t.Fatalf("want one error entry from the recovered panic, got %+v", got)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("upstream is still blocked sending: it was not drained")
	}
}
