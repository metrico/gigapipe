package clickhouse_planner

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/utils/zonetest"
)

func TestStoredDateBoundsInEveryZone(t *testing.T) {
	cases := []struct {
		at       string
		from, to string
	}{
		{"2026-03-10T00:10:00Z", "2026-03-09", "2026-03-11"},
		{"2026-03-10T02:00:00Z", "2026-03-09", "2026-03-11"},
		{"2026-03-10T12:00:00Z", "2026-03-09", "2026-03-11"},
		{"2026-03-10T23:30:00Z", "2026-03-09", "2026-03-11"},
		{"2026-01-01T00:00:00Z", "2025-12-31", "2026-01-02"},
	}
	zonetest.Run(t, zonetest.Zones, func(t *testing.T, loc *time.Location) {
		for _, c := range cases {
			at, err := time.Parse(time.RFC3339, c.at)
			if err != nil {
				t.Fatal(err)
			}
			for _, tm := range []time.Time{at, at.In(loc), time.Unix(0, at.UnixNano())} {
				if got := FormatFromDate(tm); got != c.from {
					t.Errorf("FormatFromDate(%s) = %s, want %s", tm, got, c.from)
				}
				if got := FormatToDate(tm); got != c.to {
					t.Errorf("FormatToDate(%s) = %s, want %s", tm, got, c.to)
				}
			}
		}
	})
}

func TestLogQLDateBoundsInEveryZone(t *testing.T) {
	from := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 11, 2, 0, 0, 0, time.UTC)
	stream := func() *StreamSelectPlanner {
		return &StreamSelectPlanner{LabelNames: []string{"pod", "job"}, Ops: []string{"=", "="}, Values: []string{"", "a"}}
	}
	ensureConfig()
	zonetest.Run(t, zonetest.Zones, func(t *testing.T, loc *time.Location) {
		planners := []struct {
			name         string
			p            shared.SQLRequestPlanner
			lower, upper int
		}{
			{"label names", &LabelNamesPlanner{}, 1, 1},
			{"detect labels", &DetectLabelsPlanner{NoStreamSelect: true}, 1, 1},
			{"series", &SeriesPlanner{FingerprintsPlanner: stream()}, 3, 1},
			{"values", &ValuesPlanner{FingerprintsPlanner: stream(), Key: "job"}, 3, 1},
			{"stream select", stream(), 2, 0},
			{"time series init", &TimeSeriesInitPlanner{}, 1, 0},
		}
		for _, p := range planners {
			ctx := &shared.PlannerContext{From: from.In(loc), To: to.In(loc),
				TimeSeriesTableName: "time_series", TimeSeriesDistTableName: "time_series",
				TimeSeriesGinTableName: "time_series_gin", TimeSeriesGinDistTableName: "time_series_gin"}
			sel, err := p.p.Process(ctx)
			if err != nil {
				t.Fatal(err)
			}
			lower, upper := zonetest.DateBounds(t, sel)
			if len(lower) != p.lower || !zonetest.AllEqual(lower, "2026-03-09") {
				t.Errorf("%s: lower bounds %v, want %d of 2026-03-09", p.name, lower, p.lower)
			}
			if len(upper) != p.upper || !zonetest.AllEqual(upper, "2026-03-12") {
				t.Errorf("%s: upper bounds %v, want %d of 2026-03-12", p.name, upper, p.upper)
			}
		}
	})
}
