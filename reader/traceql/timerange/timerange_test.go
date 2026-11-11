package timerange

import (
	"testing"
	"time"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/stretchr/testify/require"
)

func render(t *testing.T, c sql.SQLCondition) string {
	t.Helper()
	s, err := c.String(sql.DefaultCtx())
	require.NoError(t, err)
	return s
}

func TestSpanStart(t *testing.T) {
	lower, upper := SpanStart("traces_idx.timestamp_ns", 10, 20)
	require.Equal(t, "(traces_idx.timestamp_ns) > (10)", render(t, lower))
	require.Equal(t, "(traces_idx.timestamp_ns) <= (20)", render(t, upper))
}

func TestSpanDate(t *testing.T) {
	zone := time.FixedZone("reader", 8*3600)
	from := time.Date(2026, 11, 6, 17, 0, 0, 0, time.UTC).In(zone)
	lower, upper := SpanDate("date", from, from.Add(time.Minute))
	require.Equal(t, "(date) >= ('2026-11-05')", render(t, lower))
	require.Equal(t, "(date) <= ('2026-11-07')", render(t, upper))
}
