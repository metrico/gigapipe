package planner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

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

func assertUpstreamFinishes(t *testing.T, done chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("upstream is still blocked sending: it was not drained")
	}
}

func TestWrapProcessDrainsUpstreamOnExit(t *testing.T) {
	cases := map[string]func(*shared.LogEntry) error{
		"panic": func(*shared.LogEntry) error { panic("boom") },
		"error": func(*shared.LogEntry) error { return errors.New("fail") },
	}
	for name, onEntry := range cases {
		t.Run(name, func(t *testing.T) {
			in, done := feedUnbuffered(3)
			g := GenericPlanner{&passthroughProcessor{in}}
			out, err := g.WrapProcess(&shared.PlannerContext{Ctx: context.Background()}, in,
				GenericPlannerOps{OnEntry: onEntry})
			if err != nil {
				t.Fatal(err)
			}
			got := collectEntries(out)
			if len(got) != 1 || got[0].Err == nil {
				t.Fatalf("want one error entry, got %+v", got)
			}
			assertUpstreamFinishes(t, done)
		})
	}
}
