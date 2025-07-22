package planner

import "github.com/metrico/qryn/reader/logql/transpiler/shared"

type DropPlanner struct {
	GenericPlanner
	Labels []string
	Values []string
}

func (a *DropPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {

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

func (a *DropPlanner) cutLabels(e *shared.LogEntry) error {
	if e.Labels == nil {
		return nil
	}
	recountFP := false
	for k, v := range e.Labels {
		for i, l := range a.Labels {
			if k == l && (a.Values[i] == "" || v == a.Values[i]) {
				delete(e.Labels, k)
				recountFP = true
			}
		}
	}
	if recountFP {
		e.Fingerprint = fingerprint(e.Labels)
	}
	return nil
}
