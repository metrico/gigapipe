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
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/shared/chtest"
	"github.com/metrico/qryn/v5/shared/distconfig"
	"github.com/stretchr/testify/require"
)

// zoneRegistry serves one session.
type zoneRegistry struct{ db *model.DataDatabasesMap }

func (r zoneRegistry) GetDB(context.Context) (*model.DataDatabasesMap, error) { return r.db, nil }
func (r zoneRegistry) Run()                                                   {}
func (r zoneRegistry) Stop()                                                  {}
func (r zoneRegistry) Ping() error                                            { return nil }

// zoneWindow is a minute of spans whose attribute rows are stored under date.
type zoneWindow struct {
	name string
	at   time.Time
	date string
}

// zoneWindows are spans whose date in a reader's zone, or whose stored date,
// is not their UTC date.
var zoneWindows = []zoneWindow{
	{"late", time.Date(2026, 10, 1, 23, 30, 0, 0, time.UTC), "2026-10-01"},
	{"early", time.Date(2026, 10, 1, 0, 30, 0, 0, time.UTC), "2026-10-01"},
	{"stored_east", time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), "2026-10-02"},
	{"stored_west", time.Date(2026, 10, 1, 13, 0, 0, 0, time.UTC), "2026-09-30"},
}

// zoneTraces is the number of single-span traces in each window.
const zoneTraces = 8

// zoneService migrates a database, seeds zoneWindows and returns a service
// reading it, over the shards of the chtest.ClusterEnv cluster when cluster is set.
func zoneService(t *testing.T, cluster bool) *TempoService {
	t.Helper()
	var conn clickhouse.Conn
	var name, clusterName string
	mode := maintenance.CLUST_MODE_SINGLE
	traces, attrs := "tempo_traces", "tempo_traces_attrs_gin"
	if cluster {
		conn, name, clusterName = chtest.OpenCluster(t)
		mode |= maintenance.CLUST_MODE_DISTRIBUTED
		traces, attrs = "tempo_traces_dist", "tempo_traces_attrs_gin_dist"
	} else {
		conn, name = chtest.Open(t)
	}
	require.NoError(t, maintenance.Update(conn, name, clusterName, mode, 7, "", "", false, logger.Logger))
	distconfig.Init()

	ctx := context.Background()
	for i, w := range zoneWindows {
		ids := fmt.Sprintf(`unhex(leftPad(toString(%d + number), 32, '0'))::FixedString(16) AS trace_id,
			unhex(leftPad(toString(%d + number), 16, '0'))::FixedString(8) AS span_id,
			toInt64(%d) + number AS ts`, (i+1)*100, (i+1)*100, w.at.UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s
			(oid, trace_id, span_id, parent_id, name, timestamp_ns, duration_ns)
			SELECT '0', trace_id, span_id, '', 's', ts, 10 FROM (SELECT %s FROM numbers(%d))`, traces, ids, zoneTraces)))
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s
			(oid, date, key, val, trace_id, span_id, timestamp_ns, duration)
			SELECT '0', toDate('%s'), kv.1, kv.2, trace_id, span_id, ts, 10
			FROM (SELECT %s FROM numbers(%d))
			ARRAY JOIN [('name', 's'), ('x', '%s'), ('y_%s', '1')] AS kv`,
			attrs, w.date, ids, zoneTraces, w.name, w.name)))
	}
	if cluster {
		require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH DISTRIBUTED "+traces))
		require.NoError(t, conn.Exec(ctx, "SYSTEM FLUSH DISTRIBUTED "+attrs))
	}

	opts := &clickhouse.Options{Addr: []string{os.Getenv(chtest.AddrEnv)}}
	opts.Auth.Database = name
	var opened []*sqlx.DB
	open := func() *sqlx.DB {
		d := sqlx.NewDb(clickhouse.OpenDB(opts), "clickhouse")
		opened = append(opened, d)
		return d
	}
	t.Cleanup(func() {
		for _, d := range opened {
			_ = d.Close()
		}
	})
	session := &dsn.StableSqlxDBWrapper{DB: open(), GetDB: open, Name: name}
	return &TempoService{ServiceData: model.ServiceData{Session: zoneRegistry{&model.DataDatabasesMap{
		Config: &config.ClokiBaseDataBase{Name: name, ClusterName: clusterName}, Session: session}}}}
}

// labelValues are the string values of labels.
func labelValues(labels []model.MetricsKeyValue) []string {
	res := make([]string, len(labels))
	for i, l := range labels {
		res[i] = l.Value.StringValue
	}
	return res
}

func collect[T any](t *testing.T, ch chan T, err error) []T {
	t.Helper()
	require.NoError(t, err)
	res := []T{}
	for v := range ch {
		res = append(res, v)
	}
	return res
}

// checkWindow requires every traces read over the minute around w to see w's spans.
func checkWindow(t *testing.T, svc *TempoService, w zoneWindow, cluster bool) {
	ctx := context.Background()
	from, to := time.Unix(w.at.Unix()-60, 0), time.Unix(w.at.Unix()+60, 0)
	filter := fmt.Sprintf(`{.x = "%s"}`, w.name)
	req := func(query string) *model.MetricsQueryRequest {
		return &model.MetricsQueryRequest{Query: query, From: from, To: to, Step: 30 * time.Second}
	}

	t.Run("metrics instant by", func(t *testing.T) {
		res, err := svc.MetricsQueryInstant(ctx, req(`{} | count_over_time() by (span.x)`))
		require.NoError(t, err)
		require.Len(t, res.Series, 1)
		require.Contains(t, labelValues(res.Series[0].Labels), w.name)
		require.EqualValues(t, zoneTraces, res.Series[0].Value)
	})
	t.Run("metrics range by", func(t *testing.T) {
		res, err := svc.MetricsQueryRange(ctx, req(`{} | count_over_time() by (span.x)`))
		require.NoError(t, err)
		require.Len(t, res.Series, 1)
		require.Contains(t, labelValues(res.Series[0].Labels), w.name)
	})
	t.Run("compare", func(t *testing.T) {
		res, err := svc.MetricsQueryInstant(ctx, req(`{} | compare(`+filter+`)`))
		require.NoError(t, err)
		var values []string
		for _, s := range res.Series {
			values = append(values, labelValues(s.Labels)...)
		}
		require.Contains(t, values, w.name)
	})
	t.Run("search", func(t *testing.T) {
		ch, err := svc.SearchTraceQL(ctx, filter+` | select(.y_`+w.name+`)`, 100, 100, from, to)
		var found int
		for _, batch := range collect(t, ch, err) {
			found += len(batch)
			for _, trace := range batch {
				require.NotEmpty(t, trace.SpanSet.Spans)
				var keys []string
				for _, attr := range trace.SpanSet.Spans[0].Attributes {
					keys = append(keys, attr.Key)
				}
				require.Contains(t, keys, "y_"+w.name)
			}
		}
		require.Equal(t, zoneTraces, found)
	})
	t.Run("all tags", func(t *testing.T) {
		ch, err := svc.TagsV2(ctx, "", from, to, 100)
		require.Contains(t, collect(t, ch, err), "y_"+w.name)
	})
	t.Run("filtered tags", func(t *testing.T) {
		ch, err := svc.TagsV2(ctx, filter, from, to, 100)
		require.Contains(t, collect(t, ch, err), "y_"+w.name)
	})
	t.Run("all values", func(t *testing.T) {
		ch, err := svc.ValuesV2(ctx, "x", "", from, to, 100)
		require.Contains(t, collect(t, ch, err), w.name)
	})
	t.Run("filtered values", func(t *testing.T) {
		ch, err := svc.ValuesV2(ctx, "x", filter, from, to, 100)
		require.Contains(t, collect(t, ch, err), w.name)
	})
	t.Run("tag search", func(t *testing.T) {
		ch, err := svc.Search(ctx, fmt.Sprintf(`x="%s"`, w.name), 0, 0, 100, from.UnixNano(), to.UnixNano())
		res := collect(t, ch, err)
		// Tag search reads the attributes of the shard it connects to.
		if cluster {
			require.NotEmpty(t, res)
		} else {
			require.Len(t, res, zoneTraces)
		}
	})
}

func testResultsInReaderZones(t *testing.T, cluster bool) {
	svc := zoneService(t, cluster)
	local := time.Local
	t.Cleanup(func() { time.Local = local })
	for name, hours := range map[string]int{"utc": 0, "east": 9, "west": -8} {
		for _, w := range zoneWindows {
			t.Run(strings.Join([]string{name, w.name}, "/"), func(t *testing.T) {
				time.Local = time.FixedZone(name, hours*3600)
				checkWindow(t, svc, w, cluster)
			})
		}
	}
}

// A reader in any zone sees spans whose local or stored date is not their UTC date.
func TestResultsDoNotDependOnTheReaderZone(t *testing.T) { testResultsInReaderZones(t, false) }

func TestResultsDoNotDependOnTheReaderZoneOnCluster(t *testing.T) { testResultsInReaderZones(t, true) }
