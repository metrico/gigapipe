package planner

import "github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"

type ResponseOptimizerPlanner struct {
	GenericPlanner
}

func (a *ResponseOptimizerPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	fpMap := make(map[uint64][]shared.LogEntry)
	size := 0
	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			fpMap[entry.Fingerprint] = append(fpMap[entry.Fingerprint], *entry)
			size++
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			if size < 3000 {
				return nil
			}
			for _, ents := range fpMap {
				c <- ents
			}
			fpMap = make(map[uint64][]shared.LogEntry)
			size = 0
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			if size == 0 {
				return nil
			}
			for _, ents := range fpMap {
				c <- ents
			}
			return nil
		},
	})
}
