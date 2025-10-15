package planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type ByWithoutPlanner struct {
	GenericPlanner
	By     bool
	Labels []string

	labels map[string]bool
}

func (a *ByWithoutPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	a.labels = make(map[string]bool)
	for _, l := range a.Labels {
		a.labels[l] = true
	}

	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: a.cutLabels,
		OnAfterEntriesSlice: func(entries []shared.LogEntry, out chan []shared.LogEntry) error {
			out <- entries
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}

func (a *ByWithoutPlanner) cutLabels(e *shared.LogEntry) error {
	if e.Labels == nil {
		return nil
	}
	for k := range e.Labels {
		if (a.By && !a.labels[k]) || (!a.By && a.labels[k]) {
			delete(e.Labels, k)
		}
	}
	e.Fingerprint = fingerprint(e.Labels)
	return nil
}
