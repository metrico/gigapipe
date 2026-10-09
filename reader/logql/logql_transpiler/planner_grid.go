package logql_transpiler

import (
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

// GridPlanner gives Main the request's evaluation grid and the read range
// (first-R-offset, last-offset], and keeps Main's non-zero points on the grid.
type GridPlanner struct {
	Main     shared.RequestProcessor
	Duration time.Duration
	Offset   time.Duration
}

func (g *GridPlanner) IsMatrix() bool {
	return true
}

func (g *GridPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	grid, err := shared.NewEvalGrid(ctx)
	if err != nil {
		return nil, err
	}
	gctx := *ctx
	gctx.Grid = &grid
	gctx.From = time.Unix(0, grid.FirstNs-g.Duration.Nanoseconds()-g.Offset.Nanoseconds())
	gctx.To = time.Unix(0, grid.LastNs()-g.Offset.Nanoseconds()+1)
	_in, err := g.Main.Process(&gctx, in)
	if err != nil {
		return nil, err
	}
	out := make(chan []shared.LogEntry)
	go func() {
		defer close(out)
		for entries := range _in {
			kept := entries[:0]
			for _, e := range entries {
				if e.Err != nil {
					kept = append(kept, e)
					continue
				}
				if _, ok := grid.Index(e.TimestampNS); ok && e.Value != 0 {
					kept = append(kept, e)
				}
			}
			if len(kept) > 0 {
				out <- kept
			}
		}
	}()
	return out, nil
}
