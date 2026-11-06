package service

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// reader is s served to user, the admin when empty, over HTTP when http is set,
// with SkipUnavailableShards set to skip.
func (s *metricsStore) reader(t *testing.T, user string, skip, http bool) *metricsStore {
	t.Helper()
	opts := &clickhouse.Options{
		Addr: []string{os.Getenv(chtest.AddrEnv)},
		Auth: clickhouse.Auth{Database: s.name, Username: user},
	}
	if http {
		opts.Addr, opts.Protocol = []string{os.Getenv(chtest.HTTPAddrEnv)}, clickhouse.HTTP
	}
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
	// The version cache is keyed by Name, so each session reads as its own user.
	session := &dsn.StableSqlxDBWrapper{DB: open(), GetDB: open, Name: fmt.Sprintf("%s_%s_%t", s.name, user, http)}
	res := *s
	res.svc = &TempoService{ServiceData: model.ServiceData{Session: testRegistry{&model.DataDatabasesMap{
		Config:  &config.ClokiBaseDataBase{Name: s.name, ClusterName: s.cluster, SkipUnavailableShards: skip},
		Session: session,
	}}}}
	return &res
}

// results lists each query on the range and instant endpoints, and its exemplars.
func (s *metricsStore) results(t *testing.T, queries, exemplars []string) map[string][]string {
	t.Helper()
	res := map[string][]string{}
	for _, q := range queries {
		res["range | "+q] = s.rangeResult(t, q)
		res["instant | "+q] = s.instantResult(t, q)
	}
	for _, q := range exemplars {
		res["exemplars | "+q] = s.exemplarResult(t, q)
	}
	return res
}

func assertSameResults(t *testing.T, got, want map[string][]string) {
	t.Helper()
	for key, w := range want {
		if isReaderError(w) {
			t.Errorf("%s: admin got %.300q", key, w)
		} else if !reflect.DeepEqual(got[key], w) {
			t.Errorf("%s:\ngot  %.300q\nwant %.300q", key, got[key], w)
		}
	}
}

// assertReadonlyError wants every result to be a code 164 error naming setting.
func assertReadonlyError(t *testing.T, got map[string][]string, setting string) {
	t.Helper()
	for key, g := range got {
		if !isReaderError(g) || !strings.Contains(g[0], "code: 164") || !strings.Contains(g[0], setting) {
			t.Errorf("%s: got %.300q, want code 164 naming %s", key, g, setting)
		}
	}
}

func isReaderError(lines []string) bool {
	return len(lines) == 1 && strings.HasPrefix(lines[0], "error: ")
}

// readerMetrics are plain and structural metrics queries.
var readerMetrics = []string{
	`{} | rate()`,
	`{.a = "1"} | count_over_time() by (name)`,
	`{.a = "1"} > {.b = "1"} | count_over_time()`,
	`{.a = "1"} >> {.b = "1"} | rate()`,
	`{.a = "1"} >> {.b = "1"} | quantile_over_time(duration, 0.5)`,
	`{.a = "1"} >> {} | count_over_time() with(sample=0.5)`,
}

// readerExemplars are plain and structural exemplar queries.
var readerExemplars = []string{`{} | rate()`, `{.a = "1"} >> {.b = "1"} | rate()`}

// readerPlainMetrics are metrics queries with no structural operator.
var readerPlainMetrics = []string{`{} | rate()`, `{.a = "1"} | count_over_time() by (name)`}

// readerPlainCompares are compare() queries with no structural operator.
func readerPlainCompares() []string {
	from, to := metricsStart.UnixNano(), metricsStart.Add(10*time.Second).UnixNano()
	return []string{
		`{} | compare({.b = "1"})`,
		`{.a = "1"} | compare({})`,
		fmt.Sprintf(`{} | compare({.a = "1"}, 10, %d, %d)`, from, to),
	}
}

// readerStructuralMetrics are the structural metrics and compare() queries.
func readerStructuralMetrics() []string {
	from, to := metricsStart.UnixNano(), metricsStart.Add(10*time.Second).UnixNano()
	return []string{
		`{.a = "1"} > {.b = "1"} | count_over_time()`,
		`{.a = "1"} >> {.b = "1"} | rate()`,
		`{.a = "1"} >> {.b = "1"} | compare({.a = "1"})`,
		`{.b = "1"} &<< {.a = "1"} | compare({name = "s2"}, 3)`,
		fmt.Sprintf(`{.a = "1"} >> {} | compare({.b = "1"}, 10, %d, %d)`, from, to),
	}
}

// A read-only user configured as documented gets the admin's results. skip is
// the reader's SkipUnavailableShards.
func TestReadonlyReaderMatchesAdminOnCluster(t *testing.T) {
	admin := openMetricsStore(t, true, false)
	admin.seed(t, metricsTraces())
	queries := append(append([]string{}, readerMetrics...), compareQueries()...)
	for _, c := range []struct {
		name, settings string
		skip           bool
	}{
		{"in profile", "readonly = 1, prefer_localhost_replica = 0", false},
		{"in profile with skip", "readonly = 1, prefer_localhost_replica = 0, skip_unavailable_shards = 1", true},
		{"changeable", "readonly = 1, prefer_localhost_replica CHANGEABLE_IN_READONLY, skip_unavailable_shards CHANGEABLE_IN_READONLY", true},
		{"readonly 2", "readonly = 2", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			user := chtest.ReaderUser(t, admin.conn, admin.name, c.settings)
			want := admin.reader(t, "", c.skip, false).results(t, queries, readerExemplars)
			assertSameResults(t, admin.reader(t, user, c.skip, false).results(t, queries, readerExemplars), want)
		})
	}
}

// HTTP takes the same settings as native.
func TestReadonlyReaderMatchesAdminOverHTTPOnCluster(t *testing.T) {
	if os.Getenv(chtest.HTTPAddrEnv) == "" {
		t.Skipf("%s is not set", chtest.HTTPAddrEnv)
	}
	admin := openMetricsStore(t, true, false)
	admin.seed(t, metricsTraces())
	user := chtest.ReaderUser(t, admin.conn, admin.name, "readonly = 1, prefer_localhost_replica = 0")
	queries := []string{`{.a = "1"} >> {.b = "1"} | rate()`, `{.a = "1"} >> {.b = "1"} | compare({.a = "1"})`, `{} | compare({.b = "1"})`}
	assertSameResults(t, admin.reader(t, user, false, true).results(t, queries, nil),
		admin.reader(t, "", false, false).results(t, queries, nil))
}

// A bare readonly=1 user fails structural metrics and compare() on the setting
// their ids statement changes, and reads plain ones as the admin does.
func TestReadonlyReaderWithoutSettingFailsOnCluster(t *testing.T) {
	admin := openMetricsStore(t, true, false)
	admin.seed(t, metricsTraces())
	user := chtest.ReaderUser(t, admin.conn, admin.name, "readonly = 1")
	got := admin.reader(t, user, false, false).results(t, readerStructuralMetrics(), nil)
	assertReadonlyError(t, got, "prefer_localhost_replica")
	plain := append(append([]string{}, readerPlainMetrics...), readerPlainCompares()...)
	assertSameResults(t, admin.reader(t, user, false, false).results(t, plain, readerPlainMetrics[:1]),
		admin.reader(t, "", false, false).results(t, plain, readerPlainMetrics[:1]))
}

// With skip_unavailable_shards on the read tables, a user without that setting
// fails structural metrics and every compare(), and reads plain metrics as the
// admin does.
func TestReadonlyReaderWithoutSkipFailsOnCluster(t *testing.T) {
	admin := openMetricsStore(t, true, false)
	admin.seed(t, metricsTraces())
	user := chtest.ReaderUser(t, admin.conn, admin.name, "readonly = 1, prefer_localhost_replica = 0")
	queries := append(readerStructuralMetrics(), compareQueries()...)
	assertReadonlyError(t, admin.reader(t, user, true, false).results(t, queries, nil), "skip_unavailable_shards")
	assertSameResults(t, admin.reader(t, user, true, false).results(t, readerPlainMetrics, readerPlainMetrics[:1]),
		admin.reader(t, "", true, false).results(t, readerPlainMetrics, readerPlainMetrics[:1]))
}
