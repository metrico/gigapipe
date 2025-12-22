package planner

import (
	"errors"
	"io"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

var errSkipEntry = errors.New("skip entry")

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
			skipMask := make([]uint64, (len(entries)+63)/64)
			for i := range entries {
				err := ops.OnEntry(&entries[i])
				if err == nil || errors.Is(err, io.EOF) {
					continue
				}
				if errors.Is(err, errSkipEntry) {
					skipMask[i/64] |= 1 << (i % 64)
					continue
				}
				onErr(err)
				return
			}

			shift := 0
			for i, m := range skipMask {
				var start, end int
				for m != 0 {
					for m&1 == 0 {
						m >>= 1
						start++
						end++
					}
					for m&1 != 0 {
						m >>= 1
						end++
					}
					copy(entries[i*64-shift+start:], entries[i*64-shift+end:])
					entries = entries[:len(entries)-end+start]
					shift += end - start
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
