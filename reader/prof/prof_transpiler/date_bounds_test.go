package prof_transpiler

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/prof/prof_parser"
	shared2 "github.com/metrico/qryn/v5/reader/prof/shared"
	v1 "github.com/metrico/qryn/v5/reader/prof/types/v1"
	"github.com/metrico/qryn/v5/reader/utils/zonetest"
)

// TestProfileDateBoundsInEveryZone checks every profiles_series read is bounded
// by a day either side of the UTC dates of the request, whatever time.Local is.
func TestProfileDateBoundsInEveryZone(t *testing.T) {
	from := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 11, 0, 10, 0, 0, time.UTC)
	script, err := prof_parser.Parse(`{service_name="a", pod="b"}`)
	if err != nil {
		t.Fatal(err)
	}
	tId := &shared2.TypeId{Tp: "process_cpu", SampleType: "cpu", SampleUnit: "nanoseconds",
		PeriodType: "cpu", PeriodUnit: "nanoseconds"}
	plan := map[string]func() (shared.SQLRequestPlanner, error){
		"label names":    func() (shared.SQLRequestPlanner, error) { return PlanLabelNames([]*prof_parser.Script{script}) },
		"label values":   func() (shared.SQLRequestPlanner, error) { return PlanLabelValues([]*prof_parser.Script{script}, "pod") },
		"series":         func() (shared.SQLRequestPlanner, error) { return PlanSeries([]*prof_parser.Script{script}, nil) },
		"all series":     func() (shared.SQLRequestPlanner, error) { return PlanSeries(nil, nil) },
		"merge profiles": func() (shared.SQLRequestPlanner, error) { return PlanMergeProfiles(script, tId) },
		"select series": func() (shared.SQLRequestPlanner, error) {
			return PlanSelectSeries(script, tId, []string{"pod"}, v1.TimeSeriesAggregationType_TIME_SERIES_AGGREGATION_TYPE_SUM, 15)
		},
	}
	zonetest.Run(t, zonetest.Zones, func(t *testing.T, loc *time.Location) {
		for name, p := range plan {
			planner, err := p()
			if err != nil {
				t.Fatal(err)
			}
			sel, err := planner.Process(&shared.PlannerContext{From: from.In(loc), To: to.In(loc),
				ProfilesSeriesTable: "profiles_series", ProfilesSeriesDistTable: "profiles_series",
				ProfilesSeriesGinTable: "profiles_series_gin", ProfilesSeriesGinDistTable: "profiles_series_gin",
				ProfilesTable: "profiles", ProfilesDistTable: "profiles"})
			if err != nil {
				t.Fatal(err)
			}
			lower, upper := zonetest.DateBounds(t, sel)
			sameCount := len(lower) > 0 && len(upper) == len(lower)
			if !sameCount || !zonetest.AllEqual(lower, "2026-03-09") || !zonetest.AllEqual(upper, "2026-03-12") {
				t.Errorf("%s: bounds %v and %v, want the same nonzero count of 2026-03-09 and 2026-03-12", name, lower, upper)
			}
		}
	})
}
