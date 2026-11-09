package timerange

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The bounds depend on the instants only, a day either side of their UTC dates.
func TestSpanDateInEveryZone(t *testing.T) {
	cases := []struct {
		name         string
		from, to     time.Time
		lower, upper string
	}{
		{"late in the UTC day", time.Date(2026, 11, 6, 23, 30, 0, 0, time.UTC), time.Date(2026, 11, 6, 23, 40, 0, 0, time.UTC),
			"(date) >= ('2026-11-05')", "(date) <= ('2026-11-07')"},
		{"early in the UTC day", time.Date(2026, 11, 6, 0, 30, 0, 0, time.UTC), time.Date(2026, 11, 6, 0, 40, 0, 0, time.UTC),
			"(date) >= ('2026-11-05')", "(date) <= ('2026-11-07')"},
		{"across a UTC midnight", time.Date(2026, 11, 6, 23, 59, 0, 0, time.UTC), time.Date(2026, 11, 7, 0, 1, 0, 0, time.UTC),
			"(date) >= ('2026-11-05')", "(date) <= ('2026-11-08')"},
		{"at the epoch", time.Unix(0, 0), time.Unix(60, 0),
			"(date) >= ('1969-12-31')", "(date) <= ('1970-01-02')"},
	}
	for _, c := range cases {
		for _, hours := range []int{0, 9, -8, 14, -12} {
			zone := time.FixedZone("reader", hours*3600)
			lower, upper := SpanDate("date", c.from.In(zone), c.to.In(zone))
			require.Equal(t, c.lower, render(t, lower), "%s, offset %d", c.name, hours)
			require.Equal(t, c.upper, render(t, upper), "%s, offset %d", c.name, hours)
		}
	}
}
