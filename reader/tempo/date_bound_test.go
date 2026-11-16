package tempo

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/stretchr/testify/require"
)

var dateBound = regexp.MustCompile(`\(date\) (>=|<=) \('[0-9-]+'\)`)

// inEachReaderZone runs check with the process in each of several time zones.
func inEachReaderZone(t *testing.T, check func(t *testing.T)) {
	t.Helper()
	local := time.Local
	t.Cleanup(func() { time.Local = local })
	for _, hours := range []int{0, 9, -8, 14, -12} {
		time.Local = time.FixedZone("reader", hours*3600)
		check(t)
	}
}

// requireDateBounds requires exactly a lower and an upper date bound per read.
func requireDateBounds(t *testing.T, res string, reads int, lower, upper string) {
	t.Helper()
	require.Len(t, dateBound.FindAllString(res, -1), 2*reads, res)
	require.Equal(t, reads, strings.Count(res, "(date) >= ('"+lower+"')"), res)
	require.Equal(t, reads, strings.Count(res, "(date) <= ('"+upper+"')"), res)
}

func TestByLabelDateBoundsDoNotDependOnTheReaderZone(t *testing.T) {
	from := time.Date(2026, 11, 6, 23, 30, 0, 0, time.UTC).UnixNano()
	req := &MetricsRequest{Fn: "rate", By: []string{"span.x"}, FromNS: from, ToNS: from + 60e9,
		StepNS: 60e9, AttrsTable: "attrs", Spans: testSpans()}
	inEachReaderZone(t, func(t *testing.T) {
		query, _, err := BuildMetricsInstantQuery(req)
		require.NoError(t, err)
		requireDateBounds(t, render(t, query), 1, "2026-11-05", "2026-11-07")
		query, _, err = BuildMetricsRangeQuery(req)
		require.NoError(t, err)
		requireDateBounds(t, render(t, query), 1, "2026-11-05", "2026-11-07")
	})
}

// The by-label pivot reads only the attributes of the requested range.
func TestByLabelPivotBoundsTheSpanStart(t *testing.T) {
	from := time.Date(2026, 11, 6, 23, 30, 0, 0, time.UTC).UnixNano()
	to := from + 60e9
	req := &MetricsRequest{Fn: "rate", By: []string{"span.x"}, FromNS: from, ToNS: to,
		StepNS: 60e9, AttrsTable: "attrs", Spans: testSpans()}
	instant, _, err := BuildMetricsInstantQuery(req)
	require.NoError(t, err)
	ranged, _, err := BuildMetricsRangeQuery(req)
	require.NoError(t, err)
	for _, query := range []sql.ISelect{instant, ranged} {
		res := render(t, query)
		_, pivot, found := strings.Cut(res, "by_attrs as (")
		require.True(t, found, res)
		pivot, _, found = strings.Cut(pivot, "GROUP BY")
		require.True(t, found, res)
		require.Contains(t, pivot, fmt.Sprintf("(timestamp_ns) > (%d)", from))
		require.Contains(t, pivot, fmt.Sprintf("(timestamp_ns) <= (%d)", to))
	}
}

// compare() bounds the attributes over the range spanning both windows, in either order.
func TestCompareDateBoundsDoNotDependOnTheReaderZone(t *testing.T) {
	early := time.Date(2026, 11, 4, 0, 30, 0, 0, time.UTC).UnixNano()
	late := time.Date(2026, 11, 6, 23, 30, 0, 0, time.UTC).UnixNano()
	baseline := sql.NewWith(sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewRawObject("b")), "baseline_spans")
	inEachReaderZone(t, func(t *testing.T) {
		for _, req := range []*CompareRequest{
			{Baseline: baseline, FromNS: late, ToNS: late + 60e9, BaselineFromNS: early, BaselineToNS: early + 60e9, AttrsTable: "attrs"},
			{Baseline: baseline, FromNS: early, ToNS: early + 60e9, BaselineFromNS: late, BaselineToNS: late + 60e9, AttrsTable: "attrs"},
		} {
			res := render(t, mustCompare(t, req))
			requireDateBounds(t, res, 1, "2026-11-03", "2026-11-07")
			require.Contains(t, res, fmt.Sprintf("(timestamp_ns) > (%d)", early))
			require.Contains(t, res, fmt.Sprintf("(timestamp_ns) <= (%d)", late+int64(60e9)))
		}
	})
}

func TestIndexQueryDateBoundsDoNotDependOnTheReaderZone(t *testing.T) {
	from := time.Date(2026, 11, 6, 23, 30, 0, 0, time.UTC).UnixNano()
	index := func(t *testing.T, fromNS, toNS int64) string {
		t.Helper()
		res, err := (&SQLIndexQuery{Tags: `x="a"`, FromNS: fromNS, ToNS: toNS, Database: "db", Ver: dbversion.VersionInfo{}}).
			String(&sql.Ctx{Params: map[string]sql.SQLObject{}})
		require.NoError(t, err)
		return res
	}
	inEachReaderZone(t, func(t *testing.T) {
		requireDateBounds(t, index(t, from, from+60e9), 1, "2026-11-05", "2026-11-07")

		lowerOnly := dateBound.FindAllString(index(t, from, 0), -1)
		require.Equal(t, []string{"(date) >= ('2026-11-05')"}, lowerOnly)
		upperOnly := dateBound.FindAllString(index(t, 0, from), -1)
		require.Equal(t, []string{"(date) <= ('2026-11-07')"}, upperOnly)
	})
}
