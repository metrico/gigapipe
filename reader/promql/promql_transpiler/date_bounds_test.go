package promql_transpiler

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/utils/zonetest"
)

// TestPromQLDateBoundsInEveryZone checks every time_series read the
// accelerated planners render is bounded below by the day before from's UTC
// date and not above, whatever time.Local is.
func TestPromQLDateBoundsInEveryZone(t *testing.T) {
	from := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 11, 2, 0, 0, 0, time.UTC)
	queries := []string{
		`sum by (a) (rate(http_requests_total{job="myjob"}[1m]))`,
		`rate(http_requests_total{job="myjob"}[1m])`,
	}
	zonetest.Run(t, zonetest.Zones, func(t *testing.T, loc *time.Location) {
		for _, query := range queries {
			script, err := promql_parser.Parse(query)
			if err != nil {
				t.Fatal(err)
			}
			if script, err = TranspileExpressionV2(script); err != nil {
				t.Fatal(err)
			}
			var lower, upper []string
			for _, v := range script.Substitutes {
				req, err := v.Request.Process(&shared.PlannerContext{
					From: from.In(loc), To: to.In(loc), Step: 15 * time.Second,
					TimeSeriesGinTableName: "time_series_gin", TimeSeriesGinDistTableName: "time_series_gin",
					SamplesTableName: "samples_v3", SamplesDistTableName: "samples_v3",
					TimeSeriesTableName: "time_series", TimeSeriesDistTableName: "time_series",
					Metrics15sTableName: "metrics_15s", Metrics15sDistTableName: "metrics_15s",
				})
				if err != nil {
					t.Fatal(err)
				}
				l, u := zonetest.DateBounds(t, req)
				lower, upper = append(lower, l...), append(upper, u...)
			}
			if len(lower) == 0 || !zonetest.AllEqual(lower, "2026-03-09") {
				t.Errorf("%s: lower bounds %v, want only 2026-03-09", query, lower)
			}
			if len(upper) != 0 {
				t.Errorf("%s: upper bounds %v, want none: PromQL reads bound dates only below", query, upper)
			}
		}
	})
}
