package internal_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"strconv"
)

type UnwrapPlanner struct {
	GenericPlanner
	Label string
}

func (l *UnwrapPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return l.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil {
				return nil
			}
			var val string
			if l.Label == "_entry" {
				val = entry.Message
			} else {
				val = entry.Labels[l.Label]
			}
			if val != "" {
				fVal, err := strconv.ParseFloat(val, 64)
				if err != nil {
					return nil
				}
				entry.Value = fVal
			}
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			c <- entries
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}
