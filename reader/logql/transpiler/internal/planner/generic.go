package planner

import (
	"io"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

type GenericPlanner struct {
	Main shared.RequestProcessor
}

func (g *GenericPlanner) IsMatrix() bool {
	return false
}
func (g *GenericPlanner) WrapProcess(ctx *shared.PlannerContext,
	in chan []shared.LogEntry, ops GenericPlannerOps) (chan []shared.LogEntry, error) {
	_in, err := g.Main.Process(ctx, in)
	if err != nil {
		return nil, err
	}
	out := make(chan []shared.LogEntry)

	go func() {
		onErr := func(err error) {
			out <- []shared.LogEntry{{Err: err}}
			go func() {
				for range _in {
				}
			}()
		}
		defer close(out)
		defer func() { shared.TamePanic(out) }()
		for entries := range _in {
			for i := range entries {
				err := ops.OnEntry(&entries[i])
				if err != nil && err != io.EOF {
					onErr(err)
					return
				}
			}
			err := ops.OnAfterEntriesSlice(entries, out)
			if err != nil && err != io.EOF {
				onErr(err)
				return
			}
		}
		err := ops.OnAfterEntries(out)
		if err != nil && err != io.EOF {
			onErr(err)
		}
	}()
	return out, nil
}

type GenericPlannerOps struct {
	OnEntry             func(*shared.LogEntry) error
	OnAfterEntriesSlice func([]shared.LogEntry, chan []shared.LogEntry) error
	OnAfterEntries      func(chan []shared.LogEntry) error
}
