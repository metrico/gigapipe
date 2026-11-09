package traceql_transpiler

import (
	"context"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/chtest"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// asReader is s read as a new user with settings in its profile, over HTTP when
// http is set, with the reader's SkipUnavailableShards set to skip.
func (s *searchStore) asReader(t *testing.T, settings string, skip, http bool) *searchStore {
	t.Helper()
	user := chtest.ReaderUser(t, s.conn, s.name, settings)
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
	res := *s
	res.db = &dsn.StableSqlxDBWrapper{DB: open(), GetDB: open}
	res.skipUnavailable = skip
	res.user = user
	return &res
}

// readerSuggestions lists every autocomplete of queries, by kind and query.
func readerSuggestions(t *testing.T, s *searchStore, queries []string) map[string][]string {
	t.Helper()
	res := map[string][]string{}
	for _, query := range queries {
		for _, kind := range autocompleteKinds {
			res[kind+" | "+query] = s.suggest(t, kind, query, searchRun{limit: searchLimit})
		}
	}
	return res
}

// readerSearches lists the result of each query.
func readerSearches(t *testing.T, s *searchStore, queries []string) map[string][]string {
	t.Helper()
	res := map[string][]string{}
	for _, query := range queries {
		res[query] = s.result(t, query, searchRun{limit: searchLimit, boundary: 3})
	}
	return res
}

// assertSameResults compares a reader's results with the admin's, which must
// not be errors.
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

// readerSettings are the configurations docs/configuration.md gives a read-only
// reader. skip is the reader's SkipUnavailableShards.
var readerSettings = []struct {
	name, settings string
	skip           bool
}{
	{"in profile", "readonly = 1, prefer_localhost_replica = 0", false},
	{"in profile with skip", "readonly = 1, prefer_localhost_replica = 0, skip_unavailable_shards = 1", true},
	{"changeable", "readonly = 1, prefer_localhost_replica CHANGEABLE_IN_READONLY, skip_unavailable_shards CHANGEABLE_IN_READONLY", true},
	{"readonly 2", "readonly = 2", true},
}

// readerStructural are structural queries over each shape of statement: a
// one-hop, a recursive and a nested-set operand, a chain and a select.
var readerStructural = []string{
	`{.a = "1"} > {.b = "1"}`,
	`{.a = "1"} >> {.b = "1"}`,
	`{.b = "1"} ~ {.a = "1"}`,
	`{.a = "1"} !>> {.b = "1"}`,
	`{.a = "1"} >> {.b = "1"} >> {}`,
	`({nestedSetParent < 0 && .a = "1"} &>> {.b = "1"}) || ({nestedSetParent < 0 && .a = "1"}) | select(name, nestedSetParent)`,
	`{.a = "1"} >> {.b = "1"} | select(.a)`,
}

// A read-only user configured as documented reads what the admin reads.
func TestReadonlyReaderMatchesAdminOnCluster(t *testing.T) {
	admin := openSearchStore(t, true, false)
	admin.seedStructural(t)
	searches := slices.Concat(readerStructural, plainSearches)
	suggestions := slices.Concat(readerStructural, plainAutocompletes, allSpansAutocompletes)
	for _, c := range readerSettings {
		t.Run(c.name, func(t *testing.T) {
			base := *admin
			base.skipUnavailable = c.skip
			user := admin.asReader(t, c.settings, c.skip, false)
			assertSameResults(t, readerSearches(t, user, searches), readerSearches(t, &base, searches))
			assertSameResults(t, readerSuggestions(t, user, suggestions), readerSuggestions(t, &base, suggestions))
		})
	}
}

// HTTP takes the same settings as native.
func TestReadonlyReaderMatchesAdminOverHTTPOnCluster(t *testing.T) {
	if os.Getenv(chtest.HTTPAddrEnv) == "" {
		t.Skipf("%s is not set", chtest.HTTPAddrEnv)
	}
	admin := openSearchStore(t, true, false)
	admin.seedStructural(t)
	user := admin.asReader(t, readerSettings[0].settings, false, true)
	queries := []string{`{.a = "1"} >> {.b = "1"}`, `{.a = "1"}`}
	assertSameResults(t, readerSearches(t, user, queries), readerSearches(t, admin, queries))
	assertSameResults(t, readerSuggestions(t, user, queries), readerSuggestions(t, admin, queries))
}

// A bare readonly=1 user fails structural autocomplete on the setting the
// statement changes, and reads the rest as the admin does.
func TestReadonlyReaderWithoutSettingFailsOnCluster(t *testing.T) {
	admin := openSearchStore(t, true, false)
	admin.seedStructural(t)
	user := admin.asReader(t, "readonly = 1", false, false)
	assertReadonlyError(t, readerSuggestions(t, user, readerStructural), "prefer_localhost_replica")
	searches := slices.Concat(readerStructural, plainSearches)
	assertSameResults(t, readerSearches(t, user, searches), readerSearches(t, admin, searches))
	suggestions := slices.Concat(plainAutocompletes, allSpansAutocompletes)
	assertSameResults(t, readerSuggestions(t, user, suggestions), readerSuggestions(t, admin, suggestions))
}

// A bare readonly=1 user walks several slices, extent read included, and stops
// early as the admin does.
func TestReadonlyReaderWalksOnCluster(t *testing.T) {
	admin := openSearchStore(t, true, false)
	admin.seedStarts(t, []span{
		{1, 1, 0, []string{"a"}}, {1, 2, 1, []string{"b"}},
		{2, 1, 0, []string{"a"}}, {3, 1, 0, []string{"b"}},
	}, func(i int) int64 { return beforeEnd(time.Duration(i+1) * 40 * time.Second) })
	user := admin.asReader(t, "readonly = 1", false, false)
	from := walkEnd.Add(-10 * time.Minute)
	for _, query := range []string{`{.a = "1"}`, `{}`, `{.a = "1"} >> {.b = "1"}`, `{.a = "1"} && {.b = "1"}`, `{.a = "1"} | count() > 0`} {
		for _, limit := range []int{1, 0} {
			walk, _ := planWalk(t, query)
			ctx := user.walkContext(from, walkEnd, limit)
			got := user.walkLines(t, walk, query, ctx)
			if want := admin.wholeLines(t, query, from, walkEnd, limit); !reflect.DeepEqual(got, want) {
				t.Errorf("%s limit %d:\ngot  %.300q\nwant %.300q", query, limit, got, want)
			}
			if limit == 0 && ctx.Portions < 2 {
				t.Errorf("%s: %d slices", query, ctx.Portions)
			}
		}
	}
}

// With skip_unavailable_shards on the read tables, a user without that setting
// fails structural reads and reads the rest as the admin does.
func TestReadonlyReaderWithoutSkipFailsOnCluster(t *testing.T) {
	admin := openSearchStore(t, true, false)
	admin.seedStructural(t)
	user := admin.asReader(t, readerSettings[0].settings, true, false)
	assertReadonlyError(t, readerSearches(t, user, readerStructural), "skip_unavailable_shards")
	assertReadonlyError(t, readerSuggestions(t, user, readerStructural), "skip_unavailable_shards")
	base := *admin
	base.skipUnavailable = true
	assertSameResults(t, readerSearches(t, user, plainSearches), readerSearches(t, &base, plainSearches))
	suggestions := slices.Concat(plainAutocompletes, allSpansAutocompletes)
	assertSameResults(t, readerSuggestions(t, user, suggestions), readerSuggestions(t, &base, suggestions))
}

// probeCycles runs the unresolved-trace probe of a structural query as s's user.
func (s *searchStore) probeCycles(t *testing.T) ([]unresolvedFound, error) {
	t.Helper()
	q, err := traceql_ir.Parse(`{.a = "1"} >> {.b = "1"}`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &shared.PlannerContext{
		Ctx: context.Background(), CHDb: s.db, IsCluster: true,
		From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Minute), AncestryRounds: 20,
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	distconfig.Init()
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
		Name: s.name, ClusterName: s.cluster,
	}})
	return probeUnresolved(context.Background(), q, ctx)
}

// The probe behind the unresolved-trace warning fails for a bare readonly=1 user.
func TestReadonlyReaderWithoutSettingProbeFailsOnCluster(t *testing.T) {
	admin := openSearchStore(t, true, false)
	admin.seed(t, []span{{41, 1, 1, []string{"a", "b"}}})
	_, err := admin.asReader(t, "readonly = 1", false, false).probeCycles(t)
	if err == nil || !strings.Contains(err.Error(), "code: 164") || !strings.Contains(err.Error(), "prefer_localhost_replica") {
		t.Errorf("got %v, want code 164 naming prefer_localhost_replica", err)
	}
}

// The probe behind the unresolved-trace warning runs for a user configured as
// documented.
func TestReadonlyReaderProbeFindsCyclesOnCluster(t *testing.T) {
	admin := openSearchStore(t, true, false)
	admin.seed(t, []span{{41, 1, 1, []string{"a", "b"}}})
	user := admin.asReader(t, readerSettings[0].settings, false, false)
	found, err := user.probeCycles(t)
	if err != nil {
		t.Fatal(err)
	}
	const id41 = "00000000000000000000000000000029"
	if n, ids := mergeUnresolved(found); n != 1 || !slices.Equal(ids, []string{id41}) {
		t.Errorf("n = %d ids = %q, want 1 %q", n, ids, id41)
	}
}

// The profile's prefer_localhost_replica reaches the connected node's shard: it
// gets the statement that reads the spans table as a remote query.
func TestReadonlyReaderReadsLocalShardRemotelyOnCluster(t *testing.T) {
	admin := openSearchStore(t, true, false)
	admin.seedStructural(t)
	user := admin.asReader(t, readerSettings[0].settings, false, false)
	assertSameResults(t, readerSuggestions(t, user, readerStructural[:1]), readerSuggestions(t, admin, readerStructural[:1]))
	execSQL(t, admin.conn, "SYSTEM FLUSH LOGS")
	var shardQueries uint64
	err := admin.conn.QueryRow(context.Background(), `SELECT count() FROM system.query_log
		WHERE initial_user = $1 AND NOT is_initial_query AND type = 'QueryFinish'
		AND Settings['prefer_localhost_replica'] = '0' AND has(tables, $2)`,
		user.user, admin.name+".tempo_traces").Scan(&shardQueries)
	if err != nil {
		t.Fatal(err)
	}
	if shardQueries == 0 {
		t.Error("no shard query on the connected node")
	}
}
