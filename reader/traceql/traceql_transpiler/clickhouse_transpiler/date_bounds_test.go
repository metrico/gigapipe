package clickhouse_transpiler

import (
	"slices"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/reader/utils/zonetest"
)

// TestAutocompleteDateBoundsInEveryZone pins the traces_kv date bounds of tag
// and value autocomplete: a day either side of the UTC dates of from and to.
func TestAutocompleteDateBoundsInEveryZone(t *testing.T) {
	cases := []struct {
		from, to             string
		wantLower, wantUpper string
	}{
		{"2026-03-09T23:00:00Z", "2026-03-10T00:10:00Z", "2026-03-08", "2026-03-11"},
		{"2026-03-10T01:00:00Z", "2026-03-10T12:00:00Z", "2026-03-09", "2026-03-11"},
		{"2026-03-10T20:00:00Z", "2026-03-10T23:50:00Z", "2026-03-09", "2026-03-11"},
	}
	planners := map[string]interface {
		Process(*shared.PlannerContext) (sql.ISelect, error)
	}{
		"tags":   &AllTagsRequestPlanner{},
		"values": &AllValuesRequestPlanner{Key: "service.name"},
	}
	zonetest.Run(t, zonetest.Zones, func(t *testing.T, loc *time.Location) {
		for _, c := range cases {
			from, err := time.Parse(time.RFC3339, c.from)
			if err != nil {
				t.Fatal(err)
			}
			to, err := time.Parse(time.RFC3339, c.to)
			if err != nil {
				t.Fatal(err)
			}
			for name, p := range planners {
				sel, err := p.Process(&shared.PlannerContext{
					From:              from.In(loc),
					To:                to.In(loc),
					TracesKVDistTable: "tempo_traces_kv_dist",
				})
				if err != nil {
					t.Fatal(err)
				}
				lower, upper := zonetest.DateBounds(t, sel)
				if !slices.Equal(lower, []string{c.wantLower}) || !slices.Equal(upper, []string{c.wantUpper}) {
					t.Errorf("%s %s..%s: date bounds %v..%v, want [%s]..[%s]",
						name, c.from, c.to, lower, upper, c.wantLower, c.wantUpper)
				}
			}
		}
	})
}
