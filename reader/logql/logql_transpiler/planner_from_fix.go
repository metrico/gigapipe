package logql_transpiler

import (
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
)

type FixPeriodPlanner struct {
	Main     shared.RequestProcessor
	Duration time.Duration
}

func (m *FixPeriodPlanner) IsMatrix() bool {
	return true
}
func (m *FixPeriodPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	_from := ctx.From.UnixNano()
	_to := ctx.To.UnixNano()
	ctx.From = ctx.From.Truncate(m.Duration)
	ctx.To = ctx.To.Truncate(m.Duration).Add(m.Duration)

	_in, err := m.Main.Process(ctx, in)
	if err != nil {
		return nil, err
	}

	res := make(chan []shared.LogEntry)

	var (
		values      []float64
		fingerprint uint64
		labels      map[string]string
	)

	exportEntries := func() {
		entries := make([]shared.LogEntry, 0, len(values))
		for i, v := range values {
			if v == 0 {
				continue
			}
			entries = append(entries, shared.LogEntry{
				TimestampNS: _from + int64(i)*ctx.Step.Nanoseconds(),
				Fingerprint: fingerprint,
				Labels:      labels,
				Message:     "",
				Value:       v,
				Err:         nil,
			})
		}
		if len(entries) > 0 {
			res <- entries
		}
	}

	go func() {
		defer close(res)
		for entries := range _in {
			for _, entry := range entries {
				if entry.Fingerprint != fingerprint {
					exportEntries()
					fingerprint = entry.Fingerprint
					values = make([]float64, (_to-_from)/ctx.Step.Nanoseconds()+1)
					labels = entry.Labels
				}
				idxFrom := ((entry.TimestampNS/m.Duration.Nanoseconds())*m.Duration.Nanoseconds() - _from) / ctx.Step.Nanoseconds()
				idxTo := ((entry.TimestampNS/m.Duration.Nanoseconds()+1)*m.Duration.Nanoseconds() - _from) / ctx.Step.Nanoseconds()

				if idxTo < 0 || idxFrom >= int64(len(values)) {
					continue
				}
				idxFrom = max(idxFrom, 0)
				idxTo = min(idxTo, int64(len(values)))
				idxTo = max(idxTo, idxFrom+1)
				fastFill(values[idxFrom:idxTo], entry.Value)
			}
		}
		exportEntries()
	}()
	return res, nil
}

func fastFill(v []float64, val float64) {
	v[0] = val
	l := 1
	for ; l < len(v); l *= 2 {
		copy(v[l:], v[:l])
	}
}
