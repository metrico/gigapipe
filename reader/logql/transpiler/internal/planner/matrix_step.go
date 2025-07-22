package planner

import (
	"time"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

type MatrixStepPlanner struct {
	GenericPlanner
	Duration time.Duration
}

func (m *MatrixStepPlanner) IsMatrix() bool {
	return true
}

func (m *MatrixStepPlanner) Process(ctx *shared.PlannerContext, in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	var previousEntry *shared.LogEntry
	var entries []shared.LogEntry
	/*var err error
	if ctx.Step.Nanoseconds() >= m.Duration.Nanoseconds() {
		in, err = m.Main.Process(ctx, in)
		if err != nil {
			return nil, err
		}
		return in, nil
	}*/
	return m.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if previousEntry == nil {
				previousEntry = entry
				return nil
			}
			i := previousEntry.TimestampNS
			for ; i <= previousEntry.TimestampNS+m.Duration.Nanoseconds() && i < ctx.To.UnixNano(); i += m.Duration.Nanoseconds() {
				newEntry := *previousEntry
				newEntry.TimestampNS = i
				entries = append(entries, newEntry)
			}
			previousEntry = entry
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			i := previousEntry.TimestampNS
			for ; i <= previousEntry.TimestampNS+m.Duration.Nanoseconds() && i < ctx.To.UnixNano(); i += m.Duration.Nanoseconds() {
				newEntry := *previousEntry
				newEntry.TimestampNS = i
				entries = append(entries, newEntry)
			}
			c <- entries
			entries = nil
			previousEntry = nil
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}
