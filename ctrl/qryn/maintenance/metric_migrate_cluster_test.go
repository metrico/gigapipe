package maintenance

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/cloki-config/config"
)

const testCluster = "c1"

// newClusterRun is newRun on the cluster testCluster.
func newClusterRun(floor string) (*fakeExec, MetricMigrationOptions) {
	f, opts := newRun(floor)
	opts.Cluster = testCluster
	return f, opts
}

// clusterLimited are limitedStatements without the cluster check's reads, which run before the lease.
func clusterLimited(f *fakeExec) []Statement {
	var out []Statement
	for _, s := range limitedStatements(f) {
		if s.Op != opDistTables && s.Op != opMacros && s.Op != opClusters {
			out = append(out, s)
		}
	}
	return out
}

// clusterRawGrowth answers each unit's first raw count through metric_samples_dist with before,
// its second with after, every tier count with buckets, and any other statement with next.
func clusterRawGrowth(before, after, buckets uint64, next func(context.Context, Statement) (any, error)) func(context.Context, Statement) (any, error) {
	var mu sync.Mutex
	seen := map[string]int{}
	return func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opCount && s.Table == "metric_samples_dist" {
			mu.Lock()
			defer mu.Unlock()
			seen[s.QueryID]++
			if seen[s.QueryID] == 1 {
				return before, nil
			}
			return after, nil
		}
		if s.Op == opCount {
			return buckets, nil
		}
		if next != nil {
			return next(ctx, s)
		}
		return nil, nil
	}
}

func TestAClusterUnitReadsAndWritesTheDistTablesAndDeletesTheLocalTiers(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	f.on = clusterRawGrowth(10, 17, 3, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	name := rawRecord("2026-10-02 09:00")
	sameStrings(t, "steps", unitSteps(f, "2026-10-02 09:00"), []string{
		"record " + name + "=started",
		"count metric_samples_dist",
		"copy metric_samples_dist",
		"count metric_samples_dist",
		"count metrics_5m_dist",
		"delete metrics_5m",
		"rebuild metrics_5m_dist",
		"count metrics_1h_dist",
		"delete metrics_1h",
		"rebuild metrics_1h_dist",
		"check metrics_5m_dist",
		"check metrics_1h_dist",
		"record " + name + "=done",
	})
	for _, s := range f.statements() {
		switch s.Op {
		case opCopy:
			if !strings.Contains(s.SQL, "INSERT INTO metric_samples_dist ") || !strings.Contains(s.SQL, "FROM samples_v3_dist ") {
				t.Errorf("copy = %s", s.SQL)
			}
		case opRebuild:
			if !strings.Contains(s.SQL, "FROM metric_samples_dist FINAL PREWHERE timestamp > ") || !strings.Contains(s.SQL, "INSERT INTO "+s.Table+" ") {
				t.Errorf("rebuild = %s", s.SQL)
			}
		case opCount:
			if s.Table == "metric_samples_dist" && !strings.HasPrefix(s.SQL, "SELECT count(), countIf(") ||
				s.Table == "metric_samples_dist" && !strings.Contains(s.SQL, "FROM metric_samples_dist FINAL PREWHERE timestamp > ") {
				t.Errorf("raw count = %s", s.SQL)
			}
		case opCheck:
			if !strings.HasPrefix(s.SQL, "SELECT sum(count) FROM "+s.Table+" WHERE bucket > ") {
				t.Errorf("check = %s", s.SQL)
			}
		case opDelete:
			if !strings.Contains(s.SQL, "DELETE FROM "+s.Table+" ON CLUSTER `c1` WHERE") {
				t.Errorf("delete = %s", s.SQL)
			}
		}
	}
}

func TestAClusterRunReadsTheLegacyDistTablesAndWritesTheSeriesAndMetadataThroughDist(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opExists {
			return uint8(1), nil
		}
		return nil, nil
	}
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		opExists:   "metrics_15s_dist",
		op15sHours: "metrics_15s_dist",
		opSeries:   "metric_series_dist",
		opMetadata: "metric_metadata_dist",
	}
	for _, s := range f.statements() {
		if table, ok := want[s.Op]; ok && s.Table != table {
			t.Errorf("%s reads %s, want %s", s.Op, s.Table, table)
		}
		switch s.Op {
		case opSeries:
			if !strings.Contains(s.SQL, "FROM time_series_dist ") || !strings.Contains(s.SQL, "FROM metrics_15s_dist ") {
				t.Errorf("series = %s", s.SQL)
			}
		case opMetadata:
			if !strings.Contains(s.SQL, "FROM time_series_dist ") {
				t.Errorf("metadata = %s", s.SQL)
			}
		}
	}
}

func TestEveryClusterStatementWaitsForItsShardsAndReadsEveryShard(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	f.on = clusterRawGrowth(10, 17, 3, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	ss := clusterLimited(f)
	seen := map[string]bool{}
	for _, s := range ss {
		seen[step(s)] = true
	}
	for _, want := range []string{"copy metric_samples_dist", "delete metrics_5m", "rebuild metrics_1h_dist", "series metric_series_dist", "metadata metric_metadata_dist"} {
		if !seen[want] {
			t.Fatalf("no %s", want)
		}
	}
	for _, s := range ss {
		if s.Settings["insert_distributed_sync"] != 1 || s.Settings["skip_unavailable_shards"] != 0 || s.Settings["load_balancing"] != "in_order" || s.Settings["use_hedged_requests"] != 0 {
			t.Errorf("%s: settings %v", step(s), s.Settings)
		}
		if s.Op == opDelete && s.Settings["distributed_ddl_task_timeout"] != wholeSeconds(5*time.Minute) {
			t.Errorf("delete: settings %v", s.Settings)
		}
	}
}

func TestClusterRunStateGoesThroughSettingsDistWaitingForItsShard(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	puts := 0
	for _, s := range f.statements() {
		if s.Op != opRecord {
			continue
		}
		puts++
		if s.Table != "settings_dist" || !strings.Contains(s.SQL, "INSERT INTO settings_dist ") || s.Settings["insert_distributed_sync"] != 1 {
			t.Errorf("%s: table %s, settings %v, sql %s", step(s), s.Table, s.Settings, s.SQL)
		}
	}
	if puts == 0 {
		t.Fatal("no record written")
	}
	capture := &clusterCapture{}
	records := &settingsRecords{exec: capture, tables: tablesFor(testCluster)}
	if _, err := records.all(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := records.age(context.Background(), leaseRecord); err != nil {
		t.Fatal(err)
	}
	if len(capture.log) != 2 {
		t.Fatalf("%d reads", len(capture.log))
	}
	for _, s := range capture.log {
		if s.Table != "settings_dist" || !strings.Contains(s.SQL, "FROM settings_dist ") || s.Settings["skip_unavailable_shards"] != 0 {
			t.Errorf("%s: table %s, settings %v, sql %s", s.Op, s.Table, s.Settings, s.SQL)
		}
	}
}

func TestASingleNodeRunSendsNoClusterSetting(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	f.on = recoveryRawGrowth(10, 17, 3, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.statements() {
		for _, k := range []string{"insert_distributed_sync", "skip_unavailable_shards", "distributed_ddl_task_timeout", "load_balancing", "use_hedged_requests"} {
			if _, ok := s.Settings[k]; ok {
				t.Errorf("%s carries %s", step(s), k)
			}
		}
		if strings.Contains(s.SQL, "_dist") || strings.Contains(s.SQL, "ON CLUSTER") || strings.Contains(s.SQL, "clusterAllReplicas") {
			t.Errorf("%s: %s", step(s), s.SQL)
		}
	}
}

// clusterCapture logs each statement and answers none.
type clusterCapture struct{ log []Statement }

func (c *clusterCapture) Exec(_ context.Context, s Statement) (uint64, error) {
	c.log = append(c.log, s)
	return 0, nil
}

func (c *clusterCapture) Scalar(_ context.Context, s Statement, _ ...any) error {
	c.log = append(c.log, s)
	return nil
}

func (c *clusterCapture) Rows(_ context.Context, s Statement, _ func(scan func(dest ...any) error) error) error {
	c.log = append(c.log, s)
	return nil
}

func TestACancelledClusterUnitIsKilledLocallyThenOnEveryNode(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var killCtxErrs []error
	f.on = func(qctx context.Context, s Statement) (any, error) {
		switch s.Op {
		case opCopy:
			cancel()
			return nil, qctx.Err()
		case opKill:
			killCtxErrs = append(killCtxErrs, qctx.Err())
		}
		return nil, nil
	}
	if _, err := RunMetricMigration(ctx, f, opts); err == nil {
		t.Fatal("no error")
	}
	u := migrationUnit{kind: unitRaw, from: at("2026-10-02 08:00"), to: at("2026-10-02 09:00")}
	id := "'" + unitQueryID("cloki", u) + "'"
	var kills []string
	for _, s := range f.statements() {
		if s.Op == opKill {
			kills = append(kills, s.SQL)
			if len(s.Settings) != 0 {
				t.Errorf("kill carries settings %v", s.Settings)
			}
		}
	}
	sameStrings(t, "kills", kills, []string{
		"KILL QUERY WHERE query_id = " + id + " SYNC",
		"KILL QUERY ON CLUSTER `c1` WHERE initial_query_id = " + id + " SYNC",
	})
	for _, err := range killCtxErrs {
		if err != nil {
			t.Errorf("kill ctx err %v", err)
		}
	}
	if got := f.value(u.name()); got != unitStarted {
		t.Errorf("unit = %q, want started", got)
	}
}

func TestTheExternalSortRatioIsSentOnAClusterOnlyWhenEveryNodeListsIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hosts [][]any
		sent  bool
	}{
		{"every node", [][]any{{"a", uint64(1)}, {"b", uint64(1)}}, true},
		{"one node lacks it", [][]any{{"a", uint64(1)}, {"b", uint64(0)}}, false},
		{"no node answers", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, opts := newClusterRun("2026-10-02 08:00")
			probes := 0
			f.on = clusterRawGrowth(10, 17, 3, func(_ context.Context, s Statement) (any, error) {
				if s.Op == opSetting {
					probes++
					if !strings.Contains(s.SQL, "clusterAllReplicas('c1', system.settings)") || !strings.Contains(s.SQL, "countIf(") {
						t.Errorf("probe = %s", s.SQL)
					}
					return tc.hosts, nil
				}
				return nil, nil
			})
			if _, err := migrate(t, f, opts); err != nil {
				t.Fatal(err)
			}
			if probes != 1 {
				t.Errorf("%d probes", probes)
			}
			copies := 0
			for _, s := range clusterLimited(f) {
				if s.Op == opSetting {
					continue
				}
				copies++
				if _, ok := s.Settings[externalSortRatio]; ok != tc.sent {
					t.Errorf("%s sends the ratio: %v", step(s), ok)
				}
			}
			if copies == 0 {
				t.Fatal("no statement")
			}
		})
	}
}

// clusterGateSQLReadsEveryNode fails t unless every system table sql reads is read on every
// replica of c1.
func clusterGateSQLReadsEveryNode(t *testing.T, sql string, tables ...string) {
	t.Helper()
	if strings.Contains(sql, "FROM system.") {
		t.Errorf("gate reads one node: %s", sql)
	}
	for _, table := range tables {
		if !strings.Contains(sql, "clusterAllReplicas('c1', system."+table+")") {
			t.Errorf("gate does not read %s on every node: %s", table, sql)
		}
	}
}

func TestTheClusterGateHoldsWhileAnyOneNodeIsOverItsThreshold(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	r.opts.Cluster = testCluster
	polls, lookups := 0, 0
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		switch s.Op {
		case opGateTable:
			lookups++
			clusterGateSQLReadsEveryNode(t, s.SQL, "parts")
			return []any{"cloki.busy", 200.0}, nil
		case opGate:
			polls++
			clusterGateSQLReadsEveryNode(t, s.SQL, "asynchronous_metrics", "merge_tree_settings", "merges",
				"mutations", "metrics", "server_settings")
			rows := append(gateRows("", 0, 0), gateRows("", 0, 0)...)
			if polls <= 2 {
				rows[len(rows)-1] = []any{"memory", 9e9, 10e9, "", ""}
				rows[0] = []any{"parts", 200.0, 300.0, "", ""}
			}
			return rows, nil
		}
		return nil, nil
	}
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	if got := r.gateSleeps(); got != 2 {
		t.Errorf("gate polls slept %d times, want 2", got)
	}
	if lookups != 1 {
		t.Errorf("parts table looked up %d times", lookups)
	}
	if got := r.log.matching("back-off on parts", "table cloki.busy"); len(got) != 1 {
		t.Errorf("start lines = %q", got)
	}
}

// clusterCatalog answers the refusal check's reads: the legacy _dist tables with their engines,
// the node's macros and its clusters.
func clusterCatalog(engines map[string]string, macros map[string]string, clusters ...string) func(context.Context, Statement) (any, error) {
	return func(_ context.Context, s Statement) (any, error) {
		var rows [][]any
		switch s.Op {
		case opDistTables:
			for _, name := range slices.Sorted(maps.Keys(engines)) {
				if strings.Contains(s.SQL, "'"+name+"'") {
					rows = append(rows, []any{name, engines[name]})
				}
			}
		case opMacros:
			for k, v := range macros {
				rows = append(rows, []any{k, v})
			}
		case opClusters:
			for _, c := range clusters {
				rows = append(rows, []any{c})
			}
		default:
			return nil, nil
		}
		return rows, nil
	}
}

func distEngine(cluster, table string) string {
	return "Distributed('" + cluster + "', 'cloki', '" + table + "', fingerprint)"
}

func TestAClusterRunIsRefusedWhenALegacyDistTableNamesAnUndefinedCluster(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	f.on = clusterCatalog(map[string]string{
		"samples_v3_dist":  distEngine("c1", "samples_v3"),
		"time_series_dist": distEngine("old", "time_series"),
	}, nil, "c1")
	_, err := checkThenMigrate(t, f, opts)
	var refused *MigrationRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("err = %v", err)
	}
	for _, want := range []string{"time_series_dist", "'old'", "system.clusters"} {
		if !strings.Contains(refused.Reason, want) {
			t.Errorf("reason %q lacks %q", refused.Reason, want)
		}
	}
	for _, s := range f.statements() {
		if s.Op != opDistTables && s.Op != opMacros && s.Op != opClusters {
			t.Errorf("ran %s", step(s))
		}
	}
	if f.value(leaseRecord) != "" {
		t.Error("lease taken")
	}
}

func TestAClusterRunIsRefusedWithoutSamplesOrSeriesDistAndPassesWithoutRollupDist(t *testing.T) {
	all := map[string]string{
		"samples_v3_dist":  distEngine("c1", "samples_v3"),
		"time_series_dist": distEngine("c1", "time_series"),
		"metrics_15s_dist": distEngine("c1", "metrics_15s"),
	}
	for _, missing := range []string{"samples_v3_dist", "time_series_dist"} {
		t.Run(missing, func(t *testing.T) {
			engines := maps.Clone(all)
			delete(engines, missing)
			f, opts := newClusterRun("2026-10-02 08:00")
			f.on = clusterCatalog(engines, nil, "c1")
			_, err := checkThenMigrate(t, f, opts)
			var refused *MigrationRefusedError
			want := "database cloki: " + missing + " does not exist; a run on cluster 'c1' reads it"
			if !errors.As(err, &refused) || refused.Reason != want {
				t.Fatalf("err = %v, want reason %q", err, want)
			}
			if f.value(leaseRecord) != "" {
				t.Error("lease taken")
			}
		})
	}
	t.Run("metrics_15s_dist", func(t *testing.T) {
		engines := maps.Clone(all)
		delete(engines, "metrics_15s_dist")
		f, opts := newClusterRun("2026-10-02 08:00")
		f.on = clusterCatalog(engines, nil, "c1")
		if err := CheckMetricMigration(context.Background(), f, opts); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAClusterRunIsRefusedWhenMetrics15sIsVisibleButMetrics15sDistIsNot(t *testing.T) {
	engines := map[string]string{
		"samples_v3_dist":  distEngine("c1", "samples_v3"),
		"time_series_dist": distEngine("c1", "time_series"),
		"metrics_15s":      "ReplicatedMergeTree('/clickhouse/{shard}/metrics_15s', '{replica}')",
	}
	f, opts := newClusterRun("2026-10-02 08:00")
	f.on = clusterCatalog(engines, nil, "c1")
	_, err := checkThenMigrate(t, f, opts)
	var refused *MigrationRefusedError
	want := "database cloki: metrics_15s_dist does not exist or is not visible to the migrator user; " +
		"a run on cluster 'c1' reads metrics_15s through it"
	if !errors.As(err, &refused) || refused.Reason != want {
		t.Fatalf("err = %v, want reason %q", err, want)
	}
	if f.value(leaseRecord) != "" {
		t.Error("lease taken")
	}

	engines["metrics_15s_dist"] = distEngine("c1", "metrics_15s")
	f, opts = newClusterRun("2026-10-02 08:00")
	f.on = clusterCatalog(engines, nil, "c1")
	if err := CheckMetricMigration(context.Background(), f, opts); err != nil {
		t.Fatal(err)
	}
}

func TestAClusterMacroInALegacyDistTableIsExpandedBeforeTheCheck(t *testing.T) {
	engines := map[string]string{
		"samples_v3_dist":  distEngine("{cluster}", "samples_v3"),
		"time_series_dist": distEngine("{cluster}", "time_series"),
		"metrics_15s_dist": distEngine("{cluster}", "metrics_15s"),
	}
	t.Run("defined", func(t *testing.T) {
		f, opts := newClusterRun("2026-10-02 08:00")
		f.on = clusterCatalog(engines, map[string]string{"cluster": "c1", "shard": "01"}, "c1", "other")
		if _, err := checkThenMigrate(t, f, opts); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("expanded to an undefined cluster", func(t *testing.T) {
		f, opts := newClusterRun("2026-10-02 08:00")
		f.on = clusterCatalog(engines, map[string]string{"cluster": "gone"}, "c1")
		_, err := checkThenMigrate(t, f, opts)
		var refused *MigrationRefusedError
		if !errors.As(err, &refused) || !strings.Contains(refused.Reason, "metrics_15s_dist") || !strings.Contains(refused.Reason, "gone") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unresolvable", func(t *testing.T) {
		f, opts := newClusterRun("2026-10-02 08:00")
		f.on = clusterCatalog(engines, map[string]string{"shard": "01"}, "c1")
		_, err := checkThenMigrate(t, f, opts)
		var refused *MigrationRefusedError
		if !errors.As(err, &refused) || !strings.Contains(refused.Reason, "metrics_15s_dist") || !strings.Contains(refused.Reason, "{cluster}") ||
			!strings.Contains(refused.Reason, "system.macros") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestAClusterCheckReadsTheLegacyDistTablesAndASingleNodeCheckOnlyProbesMetrics15s(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	f.on = clusterCatalog(map[string]string{
		"samples_v3_dist":  distEngine("c1", "samples_v3"),
		"time_series_dist": distEngine("c1", "time_series"),
	}, nil, "c1")
	if err := CheckMetricMigration(context.Background(), f, opts); err != nil {
		t.Fatal(err)
	}
	ss := f.statements()
	if len(ss) != 4 || ss[0].Op != opDistTables || ss[1].Op != opMacros || ss[2].Op != opClusters || ss[3].SQL != "EXISTS TABLE metrics_15s_dist" {
		t.Fatalf("statements = %v", ss)
	}
	if !strings.Contains(ss[0].SQL, "samples_v3_dist") || !strings.Contains(ss[0].SQL, "time_series_dist") || !strings.Contains(ss[0].SQL, "metrics_15s_dist") {
		t.Errorf("tables read = %s", ss[0].SQL)
	}
	f, opts = newRun("2026-10-02 08:00")
	if err := CheckMetricMigration(context.Background(), f, opts); err != nil || len(f.statements()) != 1 || f.statements()[0].SQL != "EXISTS TABLE metrics_15s" {
		t.Fatalf("err = %v, statements = %v", err, f.statements())
	}
}

func TestARunIsRefusedWhenItsUserMayNotSeeMetrics15s(t *testing.T) {
	denied := func(_ context.Context, s Statement) (any, error) {
		if s.Op == opExists {
			return nil, &clickhouse.Exception{Code: 497, Message: "Not enough privileges"}
		}
		return nil, nil
	}
	f, opts := newRun("2026-10-02 08:00")
	f.on = denied
	_, err := checkThenMigrate(t, f, opts)
	var refused *MigrationRefusedError
	want := "database cloki: metrics_15s is not visible to the migrator user; grant it SELECT on metrics_15s"
	if !errors.As(err, &refused) || refused.Reason != want {
		t.Fatalf("err = %v, want reason %q", err, want)
	}
	if len(f.statements()) != 1 || f.value(leaseRecord) != "" {
		t.Errorf("statements = %v, lease %q", f.statements(), f.value(leaseRecord))
	}

	f, opts = newClusterRun("2026-10-02 08:00")
	catalog := clusterCatalog(map[string]string{
		"samples_v3_dist":  distEngine("c1", "samples_v3"),
		"time_series_dist": distEngine("c1", "time_series"),
	}, nil, "c1")
	f.on = func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opExists {
			return denied(ctx, s)
		}
		return catalog(ctx, s)
	}
	_, err = checkThenMigrate(t, f, opts)
	want = "database cloki: metrics_15s_dist is not visible to the migrator user; grant it SELECT on metrics_15s_dist"
	if !errors.As(err, &refused) || refused.Reason != want {
		t.Fatalf("err = %v, want reason %q", err, want)
	}
}

func TestEveryClusterDatabaseIsCheckedBeforeAnyIsMigrated(t *testing.T) {
	ok, _ := newClusterRun("2026-10-02 08:00")
	ok.on = clusterCatalog(map[string]string{
		"samples_v3_dist":  distEngine("c1", "samples_v3"),
		"time_series_dist": distEngine("c1", "time_series"),
	}, nil, "c1")
	bad, _ := newClusterRun("2026-10-02 08:00")
	bad.on = clusterCatalog(map[string]string{
		"samples_v3_dist":  distEngine("old", "samples_v3"),
		"time_series_dist": distEngine("c1", "time_series"),
	}, nil, "c1")
	_, opts := newClusterRun("2026-10-02 08:00")
	execs := map[string]*fakeExec{"first": ok, "second": bad}
	closed := 0
	m := &MetricMigration{
		base: []config.ClokiBaseDataBase{
			{Name: "first", ClusterName: testCluster},
			{Name: "second", ClusterName: testCluster},
		},
		rawDays:    []int{7, 7},
		rollupDays: []int{7, 7},
		opts:       opts,
		open: func(db *config.ClokiBaseDataBase, _ MetricMigrationOptions) (Executor, func() error, error) {
			return execs[db.Name], func() error { closed++; return nil }, nil
		},
	}
	reports, err := m.Run(context.Background(), opts.Logger)
	var refused *MigrationRefusedError
	want := "database second: samples_v3_dist names cluster 'old', which is not in system.clusters"
	if !errors.As(err, &refused) || refused.Reason != want || len(reports) != 0 {
		t.Fatalf("reports %v, err = %v, want reason %q", reports, err, want)
	}
	for _, s := range ok.statements() {
		if s.Op != opDistTables && s.Op != opMacros && s.Op != opClusters && s.Op != opExists {
			t.Errorf("first database ran %s", step(s))
		}
	}
	if closed != 2 {
		t.Errorf("closed %d connections, want 2", closed)
	}
}

func TestTheClusterArgumentIsReadFromTheDistributedEngine(t *testing.T) {
	for engine, want := range map[string]string{
		"Distributed('c1', 'cloki', 'samples_v3', fingerprint)":                            "c1",
		"Distributed('{cluster}','cloki', 'samples_v3')":                                   "{cluster}",
		"Distributed(c1, cloki, samples_v3, fingerprint)":                                  "c1",
		`Distributed('it\'s', 'cloki', 'samples_v3') SETTINGS skip_unavailable_shards = 1`: "it's",
	} {
		if got, ok := distributedCluster(engine); !ok || got != want {
			t.Errorf("%s: %q, %v", engine, got, ok)
		}
	}
	if _, ok := distributedCluster("MergeTree ORDER BY x"); ok {
		t.Error("parsed a MergeTree engine")
	}
}

func TestAPartlyWrittenDistCopyLeavesTheUnitStartedAndTheNextRunRecoversIt(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	name := rawRecord("2026-10-02 09:00")
	f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCopy && s.Table == "metric_samples_dist" {
			return 40, errors.New("code: 210, connection refused by shard 2")
		}
		return nil, nil
	}
	_, err := migrate(t, f, opts)
	var failed *MigrationFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("err = %v", err)
	}
	if got := f.value(name); got != unitStarted {
		t.Fatalf("unit = %q, want started", got)
	}
	f.log = nil
	// The failed copy reached one shard: both runs' counts see the same 40 samples.
	f.on = clusterRawGrowth(40, 40, 3, nil)
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "steps", unitSteps(f, "2026-10-02 09:00"), []string{
		"record " + name + "=started",
		"count metric_samples_dist",
		"copy metric_samples_dist",
		"count metric_samples_dist",
		"count metrics_5m_dist",
		"delete metrics_5m",
		"rebuild metrics_5m_dist",
		"count metrics_1h_dist",
		"delete metrics_1h",
		"rebuild metrics_1h_dist",
		"check metrics_5m_dist",
		"check metrics_1h_dist",
		"record " + name + "=done",
	})
	if report.Recovered != 1 {
		t.Errorf("report = %+v", report)
	}
}

// checkThenMigrate checks the run as MetricMigration.Run does, then migrates.
func checkThenMigrate(t *testing.T, f *fakeExec, opts MetricMigrationOptions) (MetricMigrationReport, error) {
	t.Helper()
	if err := CheckMetricMigration(context.Background(), f, opts); err != nil {
		return MetricMigrationReport{}, err
	}
	return migrate(t, f, opts)
}

// finalCounts answers the FINAL counts of each table in turn and each series or metadata insert with 999 rows written.
func finalCounts(counts map[string][]uint64) func(context.Context, Statement) (any, error) {
	var mu sync.Mutex
	return func(_ context.Context, s Statement) (any, error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case s.Op == opCount && strings.HasSuffix(s.SQL, " FINAL"):
			n := counts[s.Table][0]
			counts[s.Table] = counts[s.Table][1:]
			return n, nil
		case s.Op == opSeries || s.Op == opMetadata:
			return 999, nil
		}
		return nil, nil
	}
}

func TestAClusterRunReportsSeriesAndMetadataAsTheChangeInTheirDistCounts(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	f.on = finalCounts(map[string][]uint64{
		"metric_series_dist":   {100, 130},
		"metric_metadata_dist": {5, 8},
	})
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	if report.Series != 30 || report.Metadata != 3 {
		t.Errorf("series = %d, metadata = %d, want 30 and 3", report.Series, report.Metadata)
	}
	var steps []string
	for _, s := range f.statements() {
		if s.QueryID == runQueryID("cloki", opSeries) || s.QueryID == runQueryID("cloki", opMetadata) {
			steps = append(steps, s.Op+" "+s.Table)
		}
	}
	sameStrings(t, "steps", steps, []string{
		"count metric_series_dist", "series metric_series_dist", "count metric_series_dist",
		"count metric_metadata_dist", "metadata metric_metadata_dist", "count metric_metadata_dist",
	})
}

func TestASingleNodeRunReportsSeriesAndMetadataAsTheRowsWritten(t *testing.T) {
	f, opts := newRun("2026-10-02 08:00")
	f.on = finalCounts(nil)
	report, err := migrate(t, f, opts)
	if err != nil {
		t.Fatal(err)
	}
	if report.Series != 999 || report.Metadata != 999 {
		t.Errorf("series = %d, metadata = %d, want 999 each", report.Series, report.Metadata)
	}
}

// clusterGateRow is the row of signal in gateClusterSQL.
func clusterGateRow(signal string) string {
	sql := gateClusterSQL(testCluster)
	row := sql[strings.Index(sql, "SELECT '"+signal+"'"):]
	if i := strings.Index(row, "UNION ALL"); i >= 0 {
		row = row[:i]
	}
	return row
}

func TestEveryClusterGateRowHasFiveColumns(t *testing.T) {
	for _, signal := range gateSignals {
		row := clusterGateRow(signal)
		head := row[:strings.Index(row, "\n  FROM")]
		if n := len(selectColumns(head)); n != 5 {
			t.Errorf("%s row has %d columns: %s", signal, n, head)
		}
	}
}

// selectColumns splits the column list of a SELECT head at its top-level commas.
func selectColumns(head string) []string {
	var cols []string
	depth, quoted, start := 0, false, len("SELECT ")
	for i := start; i < len(head); i++ {
		switch c := head[i]; {
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			cols = append(cols, head[start:i])
			start = i + 1
		}
	}
	return append(cols, head[start:])
}

func TestAFailingMutationOnAnyNodeDoesNotHoldTheClusterGate(t *testing.T) {
	row := clusterGateRow("mutations")
	for _, want := range []string{"clusterAllReplicas('c1', system.mutations)", "NOT is_done", "latest_fail_reason = ''",
		"argMin(concat(database, '.', table), create_time)", "argMin(mutation_id, create_time)"} {
		if !strings.Contains(row, want) {
			t.Errorf("mutations signal %q lacks %q", row, want)
		}
	}
}

func TestAClusterBackOffOnMutationsNamesTheMutation(t *testing.T) {
	r := newGateRun("2026-10-02 08:00")
	r.opts.Cluster = testCluster
	polls := 0
	r.f.on = func(_ context.Context, s Statement) (any, error) {
		if s.Op != opGate {
			return nil, nil
		}
		polls++
		if polls <= 1 {
			return gateRows("mutations", 1, 0), nil
		}
		return gateRows("", 0, 0), nil
	}
	if _, err := migrate(t, r.f, r.opts); err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "back-off lines", r.log.matching("back-off on"), []string{
		"metric migration: back-off on mutations: 1 at threshold 0, table cloki.busy, mutation 0000000042",
	})
}

func TestAClusterPartsTableHoldingFewerPartsThanTheSignalIsNotVisible(t *testing.T) {
	for _, answer := range [][]any{{"cloki.small", 3.0}, {"", 0.0}} {
		t.Run(fmt.Sprint(answer), func(t *testing.T) {
			r := newGateRun("2026-10-02 08:00")
			r.opts.Cluster = testCluster
			polls := 0
			r.f.on = func(_ context.Context, s Statement) (any, error) {
				if s.Op == opGateTable {
					clusterGateSQLReadsEveryNode(t, s.SQL, "parts")
					for _, want := range []string{"hostName() AS host", "toFloat64(max(n))", "WHERE host = 'h1'"} {
						if !strings.Contains(s.SQL, want) {
							t.Errorf("parts lookup lacks %q: %s", want, s.SQL)
						}
					}
					return answer, nil
				}
				if s.Op != opGate {
					return nil, nil
				}
				polls++
				if polls <= 1 {
					rows := gateRows("parts", 150, 300)
					rows[0][3] = "h1"
					return rows, nil
				}
				return gateRows("", 0, 0), nil
			}
			if _, err := migrate(t, r.f, r.opts); err != nil {
				t.Fatal(err)
			}
			sameStrings(t, "back-off lines", r.log.matching("back-off on"), []string{
				"metric migration: back-off on parts: 150 at threshold 150, table not visible to this user",
			})
		})
	}
}

func TestTheClusterPartsLineComparesTheLargestOverNodeWithItsOwnPartitions(t *testing.T) {
	if row := clusterGateRow("parts"); !strings.Contains(row, "m.host AS tbl") {
		t.Errorf("parts signal does not carry its host: %s", row)
	}
	for _, order := range [][]string{{"h1", "h2"}, {"h2", "h1"}} {
		t.Run(strings.Join(order, ","), func(t *testing.T) {
			r := newGateRun("2026-10-02 08:00")
			r.opts.Cluster = testCluster
			values := map[string]float64{"h1": 160, "h2": 200}
			polls := 0
			r.f.on = func(_ context.Context, s Statement) (any, error) {
				if s.Op == opGateTable {
					if strings.Contains(s.SQL, "WHERE host = 'h2'") {
						return []any{"cloki.h2", 200.0}, nil
					}
					return []any{"cloki.h1", 900.0}, nil
				}
				if s.Op != opGate {
					return nil, nil
				}
				polls++
				if polls > 1 {
					return gateRows("", 0, 0), nil
				}
				rows := gateRows("", 0, 0)
				var parts [][]any
				for _, host := range order {
					parts = append(parts, []any{"parts", values[host], 300.0, host, ""})
				}
				return append(parts, rows[1:]...), nil
			}
			if _, err := migrate(t, r.f, r.opts); err != nil {
				t.Fatal(err)
			}
			sameStrings(t, "back-off lines", r.log.matching("back-off on"), []string{
				"metric migration: back-off on parts: 200 at threshold 150, table cloki.h2",
			})
		})
	}
}

func TestAClusterSplitUnitCopiesAndRebuildsEachShareThroughDist(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	opts.Split = 2
	f.on = clusterRawGrowth(10, 17, 3, nil)
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	sameStrings(t, "steps", splitSteps(f, "2026-10-02 09:00"), []string{
		"count metric_samples_dist",
		"copy metric_samples_dist 0/2",
		"copy metric_samples_dist 1/2",
		"count metric_samples_dist",
		"count metrics_5m_dist",
		"delete metrics_5m",
		"rebuild metrics_5m_dist 0/2",
		"rebuild metrics_5m_dist 1/2",
		"count metrics_1h_dist",
		"delete metrics_1h",
		"rebuild metrics_1h_dist 0/2",
		"rebuild metrics_1h_dist 1/2",
		"check metrics_5m_dist",
		"check metrics_1h_dist",
	})
	for _, s := range f.statements() {
		if s.Parts < 2 {
			continue
		}
		share := splitWhere(s.Parts, s.Part)
		switch s.Op {
		case opCopy:
			if !strings.Contains(s.SQL, "FROM samples_v3_dist WHERE ") || !strings.HasSuffix(s.SQL, share) {
				t.Errorf("copy part %d = %s", s.Part, s.SQL)
			}
		case opRebuild:
			if !strings.Contains(s.SQL, "FROM metric_samples_dist FINAL PREWHERE timestamp > ") || !strings.Contains(s.SQL, share+"))") {
				t.Errorf("rebuild part %d = %s", s.Part, s.SQL)
			}
		}
	}
}

func TestAClusterUnitWithATierShortOfItsSamplesFailsLeavingItStarted(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 07:00")
	f.on = clusterRawGrowthLive(10, 17, 15, 3, func(_ context.Context, s Statement) (any, error) {
		if s.Op == opCheck {
			return map[string]uint64{"metrics_5m_dist": 15, "metrics_1h_dist": 14}[s.Table], nil
		}
		return nil, nil
	})
	_, err := migrate(t, f, opts)
	want := "metric migration of cloki failed at unit 1/2 raw (2026-10-02T08:00:00Z, 2026-10-02T09:00:00Z]: " +
		"metrics_1h holds 14 samples, raw holds 15; the hour stays started and the next run rebuilds it"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v\nwant %s", err, want)
	}
	if got := f.value(rawRecord("2026-10-02 09:00")); got != unitStarted {
		t.Errorf("unit = %q, want started", got)
	}
}

// clusterRawGrowthLive is clusterRawGrowth whose second raw count also reads live non-stale samples.
func clusterRawGrowthLive(before, after, live, buckets uint64, next func(context.Context, Statement) (any, error)) func(context.Context, Statement) (any, error) {
	var mu sync.Mutex
	seen := map[string]int{}
	return func(ctx context.Context, s Statement) (any, error) {
		if s.Op == opCount && s.Table == "metric_samples_dist" {
			mu.Lock()
			defer mu.Unlock()
			seen[s.QueryID]++
			if seen[s.QueryID] == 1 {
				return before, nil
			}
			return []any{after, live}, nil
		}
		if s.Op == opCount {
			return buckets, nil
		}
		return next(ctx, s)
	}
}

func TestA15sClusterUnitCountsAndRebuildsThroughDist(t *testing.T) {
	f, opts := newClusterRun("2026-10-02 08:00")
	with15s(f, 240, "2026-10-02 08:00", "2026-10-02 06:00")
	opts.RollupDays = 30
	if _, err := migrate(t, f, opts); err != nil {
		t.Fatal(err)
	}
	rec := rollupRecord("2026-10-02 06:00")
	sameStrings(t, "15s steps", steps15sOf(f, unit15s, "2026-10-02 06:00"), []string{
		"record " + rec + "=started",
		"count metrics_15s_dist",
		"count metrics_5m_dist",
		"delete metrics_5m",
		"rebuild metrics_5m_dist",
		"count metrics_1h_dist",
		"delete metrics_1h",
		"rebuild metrics_1h_dist",
		"check metrics_5m_dist",
		"check metrics_1h_dist",
		"record " + rec + "=done",
	})
	id := unitQueryID("cloki", migrationUnit{kind: unit15s, to: at("2026-10-02 06:00")})
	for _, s := range f.statements() {
		if s.QueryID != id {
			continue
		}
		if strings.Contains(s.SQL, "metric_samples") {
			t.Errorf("15s unit touches raw: %s", s.SQL)
		}
		switch s.Op {
		case opCount:
			if s.Table == "metrics_15s_dist" && !strings.Contains(s.SQL, "FROM metrics_15s_dist WHERE ") {
				t.Errorf("15s count = %s", s.SQL)
			}
		case opRebuild:
			if !strings.Contains(s.SQL, "INSERT INTO "+s.Table+" ") || !strings.Contains(s.SQL, "FROM metrics_15s_dist WHERE ") {
				t.Errorf("15s rebuild = %s", s.SQL)
			}
		case opDelete:
			if !strings.HasPrefix(s.SQL, "DELETE FROM "+s.Table+" ON CLUSTER `c1` WHERE") {
				t.Errorf("15s delete = %s", s.SQL)
			}
		}
	}
}

func TestAClusterRunNamesEachLegacyClusterThatDiffersFromClusterNameOnce(t *testing.T) {
	for _, c := range []struct {
		name    string
		engines map[string]string
		lines   []string
	}{
		{name: "same cluster", engines: map[string]string{
			"samples_v3_dist":  distEngine("{cluster}", "samples_v3"),
			"time_series_dist": distEngine("c1", "time_series"),
			"metrics_15s_dist": distEngine("c1", "metrics_15s"),
		}},
		{name: "one other cluster", engines: map[string]string{
			"samples_v3_dist":  distEngine("{legacy}", "samples_v3"),
			"time_series_dist": distEngine("old", "time_series"),
			"metrics_15s_dist": distEngine("old", "metrics_15s"),
		}, lines: []string{"metric migration: legacy cluster: 'old' (CLUSTER_NAME 'c1')"}},
		{name: "two other clusters", engines: map[string]string{
			"samples_v3_dist":  distEngine("old", "samples_v3"),
			"time_series_dist": distEngine("c1", "time_series"),
			"metrics_15s_dist": distEngine("older", "metrics_15s"),
		}, lines: []string{
			"metric migration: legacy cluster: 'old' (CLUSTER_NAME 'c1')",
			"metric migration: legacy cluster: 'older' (CLUSTER_NAME 'c1')",
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, opts := newClusterRun("2026-10-02 08:00")
			log := &gateLogger{}
			opts.Logger = log
			f.on = clusterCatalog(c.engines, map[string]string{"cluster": "c1", "legacy": "old"}, "c1", "old", "older")
			if _, err := checkThenMigrate(t, f, opts); err != nil {
				t.Fatal(err)
			}
			sameStrings(t, "legacy cluster lines", log.matching("legacy cluster"), c.lines)
		})
	}
}
