package service

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// onePassReader is s served to user, the admin when empty.
func onePassReader(t *testing.T, s *metricsStore, user string, skip bool) *metricsStore {
	t.Helper()
	opts := &clickhouse.Options{
		Addr: []string{os.Getenv(chtest.AddrEnv)},
		Auth: clickhouse.Auth{Database: s.name, Username: user},
	}
	db := sqlx.NewDb(clickhouse.OpenDB(opts), "clickhouse")
	t.Cleanup(func() { _ = db.Close() })
	res := *s
	res.svc = &TempoService{ServiceData: model.ServiceData{Session: testRegistry{&model.DataDatabasesMap{
		Config: &config.ClokiBaseDataBase{Name: s.name, ClusterName: s.cluster, SkipUnavailableShards: skip},
		Session: &dsn.StableSqlxDBWrapper{
			DB: db, GetDB: func() *sqlx.DB { return db }, Name: fmt.Sprintf("%s_%s_%t", s.name, user, skip),
		},
	}}}}
	return &res
}

// A readonly=1 user with no setting of its own gets the admin's results on metrics
// that need no whole trace, with or without skipped shards.
func TestReadonlyUserRunsOnePassMetricsOnCluster(t *testing.T) {
	admin := openMetricsStore(t, true, false)
	admin.seed(t, metricsTraces())
	user := chtest.ReaderUser(t, admin.conn, admin.name, "readonly = 1")
	for _, skip := range []bool{false, true} {
		queries := []string{
			`{} | rate()`,
			`{.a = "1"} | rate()`,
			`{.a = "1"} | {.b = "1"} | rate()`,
			`{.a = "1"} | select(.b) | rate()`,
			`{nestedSetParent < 0} | rate()`,
			`{.a = "1"} | rate() by (name)`,
		}
		if !skip {
			queries = append(queries, `{} | compare({.b = "1"})`, `{.a = "1"} | {.b = "1"} | compare({.c = "1"})`,
				`{.a = "1"} | select(.b) | compare({.c = "1"})`)
		}
		want, got := onePassReader(t, admin, "", skip), onePassReader(t, admin, user, skip)
		for _, q := range queries {
			t.Run(fmt.Sprintf("%s skip=%t", q, skip), func(t *testing.T) {
				for name, result := range map[string]func(*metricsStore, *testing.T, string) []string{
					"range":     (*metricsStore).rangeResult,
					"instant":   (*metricsStore).instantResult,
					"exemplars": (*metricsStore).exemplarResult,
				} {
					w, g := result(want, t, q), result(got, t, q)
					if len(w) == 0 || strings.HasPrefix(w[0], "error: ") {
						t.Errorf("%s: admin got %.300q", name, w)
					}
					if !reflect.DeepEqual(g, w) {
						t.Errorf("%s:\ngot  %.300q\nwant %.300q", name, g, w)
					}
				}
			})
		}
	}
}
