package service

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
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

var clusterLevels = []float64{.5, .95, .99}

const clusterStep = 20 * time.Second

// clusterSpan is a seeded span, as the query sees it.
type clusterSpan struct {
	i, trace int
	tsNs     int64
	durNs    int64
	group    string
	spanHash uint64
}

// clusterStore is a two-shard database with a service on each protocol.
type clusterStore struct {
	conn    clickhouse.Conn
	name    string
	cluster string
	spans   []clusterSpan
	from    time.Time
	to      time.Time
}

// recordingSession keeps every statement a service ran and its error.
type recordingSession struct {
	model.ISqlxDB
	mu      sync.Mutex
	queries []string
	errs    []string
}

func (r *recordingSession) QueryCtx(ctx context.Context, q string, args ...any) (*stdsql.Rows, error) {
	rows, err := r.ISqlxDB.QueryCtx(ctx, q, args...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = append(r.queries, q)
	if err != nil {
		r.errs = append(r.errs, err.Error())
	}
	return rows, err
}

func (s *clusterStore) service(t *testing.T, opts *clickhouse.Options) (*TempoService, *recordingSession) {
	t.Helper()
	opts.Auth.Database = s.name
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
	rec := &recordingSession{ISqlxDB: &dsn.StableSqlxDBWrapper{DB: open(), GetDB: open, Name: s.name}}
	return &TempoService{ServiceData: model.ServiceData{Session: quantileRegistry{&model.DataDatabasesMap{
		Config: &config.ClokiBaseDataBase{Name: s.name, ClusterName: s.cluster}, Session: rec}}}}, rec
}

// openClusterStore seeds 120 spans, one a second from 1 ms after start, in
// traces of 3. Durations are distinct and out of order. Group is a or b by
// parity, and a alone in the last bucket.
func openClusterStore(t *testing.T, start time.Time) *clusterStore {
	t.Helper()
	conn, name, cluster := chtest.OpenCluster(t)
	require.NoError(t, maintenance.Update(conn, name, cluster,
		maintenance.CLUST_MODE_SINGLE|maintenance.CLUST_MODE_DISTRIBUTED, 7, "", "", false, logger.Logger))
	distconfig.Init()
	s := &clusterStore{conn: conn, name: name, cluster: cluster, from: start, to: start.Add(120 * time.Second)}
	var rows []string
	for i := range 120 {
		group := "a"
		if i%2 == 1 && i < 100 {
			group = "b"
		}
		sp := clusterSpan{i: i, trace: i/3 + 1, tsNs: start.Add(time.Duration(i)*time.Second + time.Millisecond).UnixNano(),
			durNs: int64((i*37)%211+1) * 1e6, group: group}
		s.spans = append(s.spans, sp)
		rows = append(rows, fmt.Sprintf("('%032x', '%016x', '', 's', %d, %d, [('name','s'),('x','%s'),('t','%d')])",
			sp.trace, i+1, sp.tsNs, sp.durNs, group, sp.trace))
	}
	ctx := context.Background()
	exec := func(q string) {
		t.Helper()
		require.NoError(t, conn.Exec(ctx, q))
	}
	const cols = "(trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, tags)"
	exec("CREATE TABLE seed_input AS traces_input ENGINE = Memory")
	exec("INSERT INTO seed_input " + cols + " VALUES " + strings.Join(rows, ","))
	exec(`INSERT INTO tempo_traces_dist (oid, trace_id, span_id, parent_id, name, timestamp_ns, duration_ns)
		SELECT oid, unhex(trace_id)::FixedString(16), unhex(span_id)::FixedString(8), unhex(parent_id), name,
		       timestamp_ns, duration_ns FROM seed_input`)
	exec(`INSERT INTO tempo_traces_attrs_gin_dist (oid, date, key, val, trace_id, span_id, timestamp_ns, duration)
		SELECT oid, toDate(toDateTime(intDiv(timestamp_ns, 1000000000), 'UTC')), tags.1, tags.2, unhex(trace_id)::FixedString(16),
		       unhex(span_id)::FixedString(8), timestamp_ns, duration_ns FROM seed_input ARRAY JOIN tags`)
	exec("SYSTEM FLUSH DISTRIBUTED tempo_traces_dist")
	exec("SYSTEM FLUSH DISTRIBUTED tempo_traces_attrs_gin_dist")

	r, err := conn.Query(ctx, "SELECT lower(hex(span_id)), cityHash64(span_id), shardNum() FROM tempo_traces_dist")
	require.NoError(t, err)
	defer r.Close()
	hashes, perShard := map[string]uint64{}, map[uint32]int{}
	for r.Next() {
		var id string
		var hash uint64
		var shard uint32
		require.NoError(t, r.Scan(&id, &hash, &shard))
		hashes[id] = hash
		perShard[shard]++
	}
	require.Len(t, hashes, len(s.spans))
	require.Len(t, perShard, 2, "spans on both shards")
	t.Logf("spans per shard: %v", perShard)
	for i := range s.spans {
		s.spans[i].spanHash = hashes[fmt.Sprintf("%016x", s.spans[i].i+1)]
	}
	return s
}

// exactQuantile is linear interpolation at q over values, which quantiles()
// returns below its reservoir size. Every seed here stays below that size.
func exactQuantile(values []float64, q float64) float64 {
	sorted := slices.Sorted(slices.Values(values))
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	if lo+1 >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	return sorted[lo] + (pos-float64(lo))*(sorted[lo+1]-sorted[lo])
}

func spanSeconds(spans []clusterSpan) []float64 {
	res := make([]float64, len(spans))
	for i, sp := range spans {
		res[i] = float64(sp.durNs) / 1e9
	}
	return res
}

func spanGroup(sp clusterSpan, by bool) string {
	if by {
		return sp.group
	}
	return ""
}

func wantLabels(group string, by bool, q float64) string {
	if by {
		return fmt.Sprintf(`{.x="%s", p="%g"}`, group, q)
	}
	return fmt.Sprintf(`{p="%g"}`, q)
}

// checkClusterRange compares a range query against spans, the ones it selects.
func checkClusterRange(t *testing.T, s *clusterStore, svc *TempoService, query string, spans []clusterSpan, by bool) {
	t.Helper()
	res, err := svc.MetricsQueryRange(context.Background(), &model.MetricsQueryRequest{
		Query: query, From: s.from, To: s.to, Step: clusterStep})
	require.NoError(t, err)

	stepMs := clusterStep.Milliseconds()
	cells := map[string]map[int64][]float64{}
	for _, sp := range spans {
		group := spanGroup(sp, by)
		if cells[group] == nil {
			cells[group] = map[int64][]float64{}
		}
		bucket := sp.tsNs / 1e6 / stepMs * stepMs
		cells[group][bucket] = append(cells[group][bucket], float64(sp.durNs)/1e9)
	}
	got := map[string]model.MetricsTimeSeries{}
	for _, series := range res.Series {
		require.NotContains(t, got, series.PromLabels)
		got[series.PromLabels] = series
	}
	require.Len(t, got, len(cells)*len(clusterLevels), "series: %v", seriesNames(got))

	queryWide := make([]float64, len(clusterLevels))
	all := spanSeconds(spans)
	for i, q := range clusterLevels {
		queryWide[i] = exactQuantile(all, q)
	}
	wantExemplars := 0
	for group, buckets := range cells {
		wantExemplars += len(buckets)
		for _, q := range clusterLevels {
			labels := wantLabels(group, by, q)
			require.Contains(t, got, labels)
			series := got[labels]
			wantLen := 1
			if by {
				wantLen = 2
				require.Equal(t, ".x", series.Labels[0].Key)
				require.Equal(t, group, series.Labels[0].Value.StringValue)
			}
			require.Len(t, series.Labels, wantLen, labels)
			last := series.Labels[wantLen-1]
			require.Equal(t, "p", last.Key, labels)
			require.NotNil(t, last.Value.DoubleValue, labels)
			require.Equal(t, q, *last.Value.DoubleValue, labels)

			// Every step of the range appears, and a step without spans is 0.
			fromMs := s.from.UnixMilli() / stepMs * stepMs
			wantSteps := int((s.to.UnixMilli()-fromMs)/stepMs) + 1
			require.Len(t, series.Samples, wantSteps, labels)
			for j, sample := range series.Samples {
				ms := fromMs + int64(j)*stepMs
				require.Equal(t, strconv.FormatInt(ms, 10), sample.TimestampMs, labels)
				want := 0.0
				if values, ok := buckets[ms]; ok {
					want = exactQuantile(values, q)
				}
				require.InDelta(t, want, sample.Value, 1e-9, "%s at %d", labels, ms)
			}
		}
	}

	seen := map[string]int{}
	for labels, series := range got {
		for _, raw := range series.Exemplars {
			b, err := json.Marshal(raw)
			require.NoError(t, err)
			var ex struct {
				Labels      []model.MetricsKeyValue `json:"labels"`
				Value       float64                 `json:"value"`
				TimestampMs string                  `json:"timestampMs"`
			}
			require.NoError(t, json.Unmarshal(b, &ex))
			i := slices.IndexFunc(spans, func(sp clusterSpan) bool {
				return math.Abs(float64(sp.durNs)/1e9-ex.Value) < 1e-9
			})
			require.GreaterOrEqual(t, i, 0, "%s: exemplar %v is no selected span", labels, ex.Value)
			sp := spans[i]
			require.Equal(t, strconv.FormatInt(sp.tsNs/1e6, 10), ex.TimestampMs, labels)
			group := spanGroup(sp, by)
			nearest := clusterLevels[nearestQuantile(queryWide, ex.Value)]
			require.Equal(t, wantLabels(group, by, nearest), labels, "exemplar %v of span %d, query-wide %v", ex.Value, sp.i, queryWide)
			names := make([]string, 0, len(ex.Labels))
			for _, l := range ex.Labels {
				names = append(names, l.Key)
				if l.Key == "trace:id" {
					require.EqualValues(t, fmt.Sprintf("%032x", sp.trace), strings.ToLower(l.Value.StringValue), labels)
				}
			}
			require.NotContains(t, names, "p", labels)
			seen[fmt.Sprintf("%s/%d", group, sp.tsNs/1e6/stepMs)]++
		}
	}
	require.Len(t, seen, wantExemplars, "one exemplar per bucket and group: %v", seen)
	for key, count := range seen {
		require.Equal(t, 1, count, key)
	}
}

func seriesNames(got map[string]model.MetricsTimeSeries) []string {
	names := make([]string, 0, len(got))
	for name := range got {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// checkClusterInstant compares an instant query against spans, the ones it selects.
func checkClusterInstant(t *testing.T, s *clusterStore, svc *TempoService, query string, spans []clusterSpan, by bool) {
	t.Helper()
	res, err := svc.MetricsQueryInstant(context.Background(), &model.MetricsQueryRequest{
		Query: query, From: s.from, To: s.to, Step: clusterStep})
	require.NoError(t, err)
	groups := map[string][]float64{}
	for _, sp := range spans {
		group := spanGroup(sp, by)
		groups[group] = append(groups[group], float64(sp.durNs)/1e9)
	}
	require.Len(t, res.Series, len(groups)*len(clusterLevels))
	seen := map[string]bool{}
	for _, series := range res.Series {
		wantLen, group := 1, ""
		if by {
			wantLen = 2
			require.Equal(t, ".x", series.Labels[0].Key)
			group = series.Labels[0].Value.StringValue
		}
		require.Len(t, series.Labels, wantLen)
		last := series.Labels[wantLen-1]
		require.Equal(t, "p", last.Key)
		require.NotNil(t, last.Value.DoubleValue)
		key := wantLabels(group, by, *last.Value.DoubleValue)
		require.False(t, seen[key], key)
		seen[key] = true
		require.Contains(t, groups, group)
		require.InDelta(t, exactQuantile(groups[group], *last.Value.DoubleValue), series.Value, 1e-9, key)
	}
}

func checkAllShapes(t *testing.T, s *clusterStore, svc *TempoService, rec *recordingSession) {
	t.Helper()
	const fn = `quantile_over_time(duration, .5, .95, .99)`
	all := s.spans
	for _, shape := range []struct {
		name  string
		query string
		spans []clusterSpan
		by    bool
	}{
		{"by", `{} | ` + fn + ` by (.x)`, all, true},
		{"no by", `{} | ` + fn, all, false},
		{"filter by", `{.x = "a"} | ` + fn + ` by (.x)`, filterSpans(all, func(sp clusterSpan) bool { return sp.group == "a" }), true},
		{"one trace, one shard empty", `{.t = "5"} | ` + fn + ` by (.x)`, filterSpans(all, func(sp clusterSpan) bool { return sp.trace == 5 }), true},
		{"one trace no by", `{.t = "5"} | ` + fn, filterSpans(all, func(sp clusterSpan) bool { return sp.trace == 5 }), false},
	} {
		t.Run(shape.name+"/range", func(t *testing.T) { checkClusterRange(t, s, svc, shape.query, shape.spans, shape.by) })
		t.Run(shape.name+"/instant", func(t *testing.T) { checkClusterInstant(t, s, svc, shape.query, shape.spans, shape.by) })
	}
	for _, f := range []string{"0.5", ".25"} {
		fraction, err := strconv.ParseFloat(f, 64)
		require.NoError(t, err)
		threshold := uint64(math.Ldexp(fraction, 64))
		kept := filterSpans(all, func(sp clusterSpan) bool { return sp.spanHash < threshold })
		require.Greater(t, len(kept), 5)
		require.Less(t, len(kept), len(all))
		hint := ` with(sample=` + f + `)`
		t.Run("sample "+f+"/range by", func(t *testing.T) { checkClusterRange(t, s, svc, `{} | `+fn+` by (.x)`+hint, kept, true) })
		t.Run("sample "+f+"/range", func(t *testing.T) { checkClusterRange(t, s, svc, `{} | `+fn+hint, kept, false) })
		t.Run("sample "+f+"/instant by", func(t *testing.T) { checkClusterInstant(t, s, svc, `{} | `+fn+` by (.x)`+hint, kept, true) })
		t.Run("sample "+f+"/instant", func(t *testing.T) { checkClusterInstant(t, s, svc, `{} | `+fn+hint, kept, false) })
	}
	t.Run("no span", func(t *testing.T) {
		res, err := svc.MetricsQueryRange(context.Background(), &model.MetricsQueryRequest{
			Query: `{.x = "none"} | ` + fn + ` by (.x)`, From: s.from, To: s.to, Step: clusterStep})
		require.NoError(t, err)
		require.Empty(t, res.Series)
	})
	require.Empty(t, rec.errs)
	joined := strings.Join(rec.queries, "\n")
	require.Contains(t, joined, "tempo_traces_dist")
	require.Contains(t, joined, "GROUPING SETS")
}

func filterSpans(spans []clusterSpan, keep func(clusterSpan) bool) []clusterSpan {
	return slices.DeleteFunc(slices.Clone(spans), func(sp clusterSpan) bool { return !keep(sp) })
}

func clusterProtocols(t *testing.T, run func(*testing.T, *clickhouse.Options)) {
	t.Helper()
	t.Run("native", func(t *testing.T) {
		run(t, &clickhouse.Options{Addr: []string{os.Getenv(chtest.AddrEnv)}})
	})
	t.Run("http", func(t *testing.T) {
		addr := os.Getenv(quantileHTTPEnv)
		if addr == "" {
			t.Skip(quantileHTTPEnv + " is not set")
		}
		run(t, &clickhouse.Options{Addr: []string{addr}, Protocol: clickhouse.HTTP})
	})
}

func TestQuantileListsOverNativeAndHTTPOnCluster(t *testing.T) {
	start := time.Now().UTC().Add(-2 * time.Hour).Truncate(clusterStep)
	s := openClusterStore(t, start)
	clusterProtocols(t, func(t *testing.T, opts *clickhouse.Options) {
		svc, rec := s.service(t, opts)
		checkAllShapes(t, s, svc, rec)
	})
	require.NoError(t, s.conn.Exec(context.Background(), "SYSTEM FLUSH LOGS"))
	r, err := s.conn.Query(context.Background(), `SELECT toUInt8(interface), count() FROM system.query_log
		WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
		  AND query LIKE '%quantiles(%' GROUP BY 1 ORDER BY 1`)
	require.NoError(t, err)
	defer r.Close()
	interfaces := map[uint8]uint64{}
	for r.Next() {
		var iface uint8
		var count uint64
		require.NoError(t, r.Scan(&iface, &count))
		interfaces[iface] = count
	}
	t.Logf("statements per interface (1 tcp, 2 http): %v", interfaces)
	require.NotZero(t, interfaces[1])
	if os.Getenv(quantileHTTPEnv) != "" {
		require.NotZero(t, interfaces[2])
	}
}

// A bucket at time 0 on both shards stays a sample beside the query-wide row.
func TestQuantileBucketAtTimeZeroOnCluster(t *testing.T) {
	s := openClusterStore(t, time.Unix(0, 0).UTC())
	clusterProtocols(t, func(t *testing.T, opts *clickhouse.Options) {
		svc, rec := s.service(t, opts)
		const fn = `quantile_over_time(duration, .5, .95, .99)`
		checkClusterRange(t, s, svc, `{} | `+fn+` by (.x)`, s.spans, true)
		checkClusterRange(t, s, svc, `{} | `+fn, s.spans, false)
		require.Empty(t, rec.errs)
	})
}
