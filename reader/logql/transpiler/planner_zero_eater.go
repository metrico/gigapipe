package transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/internal/planner"
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

type ZeroEaterPlanner struct {
	planner.GenericPlanner
}

func (m *ZeroEaterPlanner) IsMatrix() bool {
	return true
}
func (m *ZeroEaterPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	var _entries []shared.LogEntry
	return m.WrapProcess(ctx, in, planner.GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Value != 0 {
				_entries = append(_entries, *entry)
			}
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			if len(_entries) > 0 {
				c <- _entries
				_entries = nil
			}
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}
