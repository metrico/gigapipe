package service

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/shared/chtest"
	"github.com/metrico/qryn/v5/shared/distconfig"
	"github.com/stretchr/testify/require"
)

const quantileHTTPEnv = "GIGAPIPE_TEST_CLICKHOUSE_HTTP"

// seedQuantileSpans inserts 40 spans, one a second from 1 ms after quantileStart.
// Span i lasts i+1 ms and is in group a when i is even and b when odd.
func seedQuantileSpans(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var rows []string
	for i := range 40 {
		group := "a"
		if i%2 == 1 {
			group = "b"
		}
		rows = append(rows, fmt.Sprintf("('%032x', '%016x', '', 's', 0, %d, %d, [('name','s'),('x','%s')])",
			i/4+1, i+1, quantileStart.Add(time.Duration(i)*time.Second+time.Millisecond).UnixNano(), (i+1)*1000000, group))
	}
	require.NoError(t, conn.Exec(context.Background(),
		"INSERT INTO traces_input (trace_id, span_id, parent_id, name, service_name, timestamp_ns, duration_ns, tags) VALUES "+
			strings.Join(rows, ",")))
}

// quantileService is a service over database name, on the given client options.
func quantileService(name string, opts *clickhouse.Options) *TempoService {
	opts.Auth.Database = name
	open := func() *sqlx.DB { return sqlx.NewDb(clickhouse.OpenDB(opts), "clickhouse") }
	return serviceOver(name, &dsn.StableSqlxDBWrapper{DB: open(), GetDB: open, Name: name})
}

func TestQuantileListsOverNativeAndHTTP(t *testing.T) {
	conn, name := chtest.Open(t)
	require.NoError(t, maintenance.Update(conn, name, "", maintenance.CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger))
	distconfig.Init()
	seedQuantileSpans(t, conn)
	t.Run("native", func(t *testing.T) {
		checkQuantileLists(t, quantileService(name, &clickhouse.Options{Addr: []string{os.Getenv(chtest.AddrEnv)}}))
	})
	t.Run("http", func(t *testing.T) {
		addr := os.Getenv(quantileHTTPEnv)
		if addr == "" {
			t.Skip(quantileHTTPEnv + " is not set")
		}
		checkQuantileLists(t, quantileService(name, &clickhouse.Options{Addr: []string{addr}, Protocol: clickhouse.HTTP}))
	})
}

func checkQuantileLists(t *testing.T, svc *TempoService) {
	t.Helper()
	const query = `{} | quantile_over_time(duration, .5, .95, .99) by (.x)`
	req := &model.MetricsQueryRequest{Query: query, From: quantileStart,
		To: quantileStart.Add(39500 * time.Millisecond), Step: 20 * time.Second}
	res, err := svc.MetricsQueryRange(context.Background(), req)
	require.NoError(t, err)
	got := make(map[string]model.MetricsTimeSeries, len(res.Series))
	for _, s := range res.Series {
		got[s.PromLabels] = s
	}
	// Linear interpolation over the span durations of each bucket and group, in ms.
	want := map[string][2]float64{
		`{.x="a", p="0.5"}`:  {10, 30},
		`{.x="a", p="0.95"}`: {18.1, 38.1},
		`{.x="a", p="0.99"}`: {18.82, 38.82},
		`{.x="b", p="0.5"}`:  {11, 31},
		`{.x="b", p="0.95"}`: {19.1, 39.1},
		`{.x="b", p="0.99"}`: {19.82, 39.82},
	}
	require.Len(t, got, len(want))
	exemplars := 0
	for labels, values := range want {
		require.Contains(t, got, labels)
		s := got[labels]
		require.Len(t, s.Samples, 2, labels)
		require.InDelta(t, values[0]/1e3, s.Samples[0].Value, 1e-9, labels)
		require.InDelta(t, values[1]/1e3, s.Samples[1].Value, 1e-9, labels)
		// Query-wide values are 20.5, 38.05 and 39.61 ms, so p=0.5 ends at the
		// midpoint 29.275 ms and p=0.95 at 38.83 ms.
		for _, seconds := range seriesExemplars(t, s) {
			exemplars++
			ms := seconds * 1e3
			switch {
			case strings.HasSuffix(labels, `p="0.5"}`):
				require.Less(t, ms, 29.275, labels)
			case strings.HasSuffix(labels, `p="0.95"}`):
				require.Greater(t, ms, 29.275, labels)
				require.Less(t, ms, 38.83, labels)
			default:
				require.Greater(t, ms, 38.83, labels)
			}
		}
	}
	// One span per bucket and group.
	require.Equal(t, 4, exemplars)

	instant, err := svc.MetricsQueryInstant(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, instant.Series, 6)
	instantWant := map[string]float64{"a0.5": 20, "a0.95": 37.1, "a0.99": 38.62, "b0.5": 21, "b0.95": 38.1, "b0.99": 39.62}
	for _, s := range instant.Series {
		require.Len(t, s.Labels, 2)
		key := fmt.Sprintf("%s%g", s.Labels[0].Value.StringValue, *s.Labels[1].Value.DoubleValue)
		require.InDelta(t, instantWant[key]/1e3, s.Value, 1e-9, key)
	}

	noBy, err := svc.MetricsQueryRange(context.Background(), &model.MetricsQueryRequest{
		Query: `{} | quantile_over_time(duration, .5, 1)`, From: req.From, To: req.To, Step: req.Step})
	require.NoError(t, err)
	require.Len(t, noBy.Series, 2)
	for _, s := range noBy.Series {
		require.Len(t, s.Labels, 1)
		require.Equal(t, "p", s.Labels[0].Key)
	}

	noSpans, err := svc.MetricsQueryRange(context.Background(), &model.MetricsQueryRequest{
		Query: `{.x = "none"} | quantile_over_time(duration, .5, 1) by (.x)`, From: req.From, To: req.To, Step: req.Step})
	require.NoError(t, err)
	require.Empty(t, noSpans.Series)
}
