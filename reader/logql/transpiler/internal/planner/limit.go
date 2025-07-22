package planner

import "github.com/metrico/qryn/reader/logql/transpiler/shared"

type LimitPlanner struct {
	GenericPlanner
}

func (a *LimitPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	sent := 0
	limit := int(ctx.Limit)
	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			if sent >= limit {
				return nil
			}
			if sent+len(entries) < limit {
				c <- entries
				sent += len(entries)
				return nil
			}
			c <- entries[:limit-sent]
			if ctx.CancelCtx != nil {
				ctx.CancelCtx()
			}
			sent = limit
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}
