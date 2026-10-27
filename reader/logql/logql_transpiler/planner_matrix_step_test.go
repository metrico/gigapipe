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

// feedUnbuffered sends n single-entry batches on an unbuffered channel, like
// the ClickHouse getter, and closes done once every batch has been taken.
func feedUnbuffered(n int) (chan []shared.LogEntry, chan struct{}) {
	ch := make(chan []shared.LogEntry)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			ch <- []shared.LogEntry{{TimestampNS: int64(i)}}
		}
		close(ch)
	}()
	return ch, done
}

// assertPanicRecovered checks that out carries exactly the error entry from
// the recovered panic and that the upstream feeding the planner was drained.
func assertPanicRecovered(t *testing.T, out chan []shared.LogEntry, done chan struct{}) {
	t.Helper()
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

func TestMatrixStepDrainsUpstreamOnPanic(t *testing.T) {
	in, done := feedUnbuffered(3)
	m := &MatrixStepPlanner{Main: &chanProcessor{in}, Duration: time.Second}
	// A nil context makes the step loop panic on the first entry.
	out, err := m.Process(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	assertPanicRecovered(t, out, done)
}
