package service

import (
	"context"
	"slices"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/zonetest"
)

// TestServiceDateBoundsInEveryZone checks the Prometheus label fetch and the
// profile types read bound date by a day either side of the request's UTC
// dates, whatever time.Local is.
func TestServiceDateBoundsInEveryZone(t *testing.T) {
	from := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 11, 2, 0, 0, 0, time.UTC)
	want := func(t *testing.T, what string, lower, upper []string) {
		t.Helper()
		if !slices.Equal(lower, []string{"2026-03-09"}) || !slices.Equal(upper, []string{"2026-03-12"}) {
			t.Errorf("%s: bounds %v and %v, want [2026-03-09] and [2026-03-12]", what, lower, upper)
		}
	}
	zonetest.Run(t, zonetest.Zones, func(t *testing.T, loc *time.Location) {
		conn := &model.DataDatabasesMap{Config: &clconfig.ClokiBaseDataBase{}}
		l := newLabelsGetter(from.In(loc), to.In(loc), conn, context.Background())
		l.Plan(42)
		lower, upper := zonetest.DateBounds(t, l.getFetchRequest(l.fingerprintToFetch))
		want(t, "labels getter", lower, upper)

		lower, upper = zonetest.DateBounds(t, profileTypesQuery("profiles_series", from.In(loc), to.In(loc)))
		want(t, "profile types", lower, upper)
	})
}
