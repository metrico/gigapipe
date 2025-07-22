package transpiler

import (
	"time"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

type MatrixStepPlanner struct {
	Main     shared.RequestProcessor
	Duration time.Duration
}

func (m *MatrixStepPlanner) IsMatrix() bool {
	return true
}
func (m *MatrixStepPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	_in, err := m.Main.Process(ctx, in)
	if err != nil {
		return nil, err
	}
	out := make(chan []shared.LogEntry)
	go func() {
		defer close(out)
		defer func() { shared.TamePanic(out) }()
		var (
			fp       uint64
			nextTsNs int64
		)

		for entries := range _in {
			var _entries []shared.LogEntry
			for _, entry := range entries {
				if entry.Fingerprint != fp {
					nextTsNs = 0
					fp = entry.Fingerprint
				}
				if entry.TimestampNS < nextTsNs {
					continue
				}
				start := entry.TimestampNS
				i := entry.TimestampNS
				for ; i < start+m.Duration.Nanoseconds() && i < ctx.To.UnixNano(); i += ctx.Step.Nanoseconds() {
					entry.TimestampNS = i
					_entries = append(_entries, entry)
				}
				nextTsNs = start + m.Duration.Nanoseconds()
			}
			out <- _entries
		}
	}()
	return out, nil
}
