package logql_transpiler

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// startCountCutoffs are start_count cutoffs before, inside and after the
// grid requests' read ranges: a fresh install's, a cell boundary off every
// grid, the instant point, and a day after the lines.
var startCountCutoffs = map[string]time.Time{
	"0":        time.Unix(0, 0),
	"3h22m45s": gridD0.Add(3*time.Hour + 22*time.Minute + 45*time.Second),
	"4h":       gridD0.Add(4 * time.Hour),
	"1d":       gridD0.Add(24 * time.Hour),
}

// withStartCountCutoffs adds every grid request at every start_count cutoff.
func withStartCountCutoffs(reqs map[string]request) {
	for name, req := range gridRequests {
		for cn, at := range startCountCutoffs {
			req.startCount = &at
			reqs[name+"/sc="+cn] = req
		}
	}
}

// TestShortcutEvaluatesOnTheGridOnClickHouse runs every shortcut query on
// ClickHouse, on and off the 15s lattice, without metrics_15s and at every
// start_count cutoff, and checks it against (T-offset-R, T] and the route it
// took.
func TestShortcutEvaluatesOnTheGridOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := gridLines()
	reqs := maps.Clone(gridRequests)
	maps.Copy(reqs, offLatticeRequests)
	for name, req := range gridRequests {
		req.noMetrics15s = true
		reqs[name+"/nom15s"] = req
	}
	withStartCountCutoffs(reqs)
	for _, cluster := range chModes(t, base) {
		for _, c := range shortcutCases {
			for name, req := range reqs {
				req.cluster = cluster
				t.Run(fmt.Sprintf("%s@%s/cluster=%v", c.query, name, cluster), func(t *testing.T) {
					db := newChDB(base)
					defer db.Close()
					p := serveRequest(t, c.query, req, db)
					if want := onLattice(req) && !req.noMetrics15s; readsMetrics15s(p) != want {
						t.Errorf("shortcut %v, want %v", !want, want)
					}
					want := reference(lines, c.ref, refGrid(req))
					if d := diffSeries(gotSeries(p), want); len(d) > 0 {
						if len(d) > 6 {
							d = append(d[:6], fmt.Sprintf("... %d more", len(d)-6))
						}
						t.Errorf("%s\n%s", c.query, strings.Join(d, "\n"))
					}
				})
			}
		}
	}
}

// TestShortcutBinariesOnClickHouse checks binaries with a shortcut operand
// point by point: series and points on one side only are dropped, and the
// operands may take different routes.
func TestShortcutBinariesOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := gridLines()
	var linesA []logLine
	for _, l := range lines {
		if l.labels["l"] == "a" {
			linesA = append(linesA, l)
		}
	}
	count := refQuery{fn: "count_over_time", r: 5 * time.Minute}
	shifted := refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 5 * time.Minute}
	off7s := refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Second}
	for _, c := range []struct {
		query       string
		left, right refQuery
		rightLines  []logLine
	}{
		{`count_over_time({job="g"} [5m]) - count_over_time({job="g", l="a"} [5m] offset 5m)`, count, shifted, linesA},
		{`count_over_time({job="g"} [5m]) - count_over_time(` + rawSel + ` [5m] offset 5m)`, count, shifted, lines},
		{`count_over_time({job="g"} [5m] offset 7s) - count_over_time({job="g"} [5m])`, off7s, count, lines},
	} {
		reqs := maps.Clone(gridRequests)
		withStartCountCutoffs(reqs)
		for _, cluster := range chModes(t, base) {
			for name, req := range reqs {
				req.cluster = cluster
				t.Run(fmt.Sprintf("%s@%s/cluster=%v", c.query, name, cluster), func(t *testing.T) {
					db := newChDB(base)
					defer db.Close()
					p := serveRequest(t, c.query, req, db)
					times := refGrid(req)
					want := joinSeries(reference(lines, c.left, times), reference(c.rightLines, c.right, times),
						func(x, y float64) float64 { return x - y })
					if d := diffSeries(gotSeries(p), want); len(d) > 0 {
						t.Errorf("%v", d[:min(len(d), 6)])
					}
				})
			}
		}
	}
}

// TestShortcutLoadsStartCountCutoffOnClickHouse: the version info carries the
// largest start_count cutoff recorded in settings, whichever row is newest,
// and the shortcut at that cutoff matches the reference.
func TestShortcutLoadsStartCountCutoffOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	distconfig.Init()
	lines := gridLines()
	mid := gridD0.Add(4 * time.Hour)
	// An older row with a later cutoff than the seed's 0.
	for _, db := range append([]string{chLogDB}, chShardDBs[:]...) {
		if err := chExec(base, db, fmt.Sprintf(`INSERT INTO settings SELECT fingerprint, type, name, '%d',
			inserted_at - 3600 FROM settings WHERE name = '%s'`, mid.Unix(), dbversion.MarkerMetrics15sStartCount)); err != nil {
			t.Fatal(err)
		}
	}
	q := `sum by (l) (count_over_time({job="g"} [5m]))`
	ref := refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum"}
	for _, cluster := range chModes(t, base) {
		db, err := newChSQLDB(base, fmt.Sprintf("start-count-cluster=%v", cluster))
		if err != nil {
			t.Fatal(err)
		}
		v, err := dbversion.GetVersionInfo(context.Background(), cluster, db)
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got := v.Metrics15sStartCountFrom(); got != mid.UnixNano() {
			t.Fatalf("cluster=%v: start_count cutoff %d, want %d", cluster, got, mid.UnixNano())
		}
		req := gridRequests["aligned/60s"]
		req.cluster = cluster
		at := time.Unix(0, v.Metrics15sStartCountFrom())
		req.startCount = &at
		ch := newChDB(base)
		p := serveRequest(t, q, req, ch)
		ch.Close()
		if !strings.Contains(p.sql[0], fmt.Sprintf("samples.timestamp_ns >= %d", mid.UnixNano())) {
			t.Errorf("cluster=%v: no start_count read from %d", cluster, mid.UnixNano())
		}
		if d := diffSeries(gotSeries(p), reference(lines, ref, refGrid(req))); len(d) > 0 {
			t.Errorf("cluster=%v: %v", cluster, d[:min(len(d), 6)])
		}
	}
}

// chSQLDB is a model.ISqlxDB on chLogDB over clickhouse-go's database/sql
// driver, for queries of any shape.
type chSQLDB struct {
	name string
	db   *sql.DB
}

func newChSQLDB(base, name string) (*chSQLDB, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	pass, _ := u.User.Password()
	return &chSQLDB{name: name, db: clickhouse.OpenDB(&clickhouse.Options{
		Addr:     []string{u.Host},
		Protocol: clickhouse.HTTP,
		Auth:     clickhouse.Auth{Database: chLogDB, Username: u.User.Username(), Password: pass},
	})}, nil
}

func (c *chSQLDB) GetName() string { return c.name }
func (c *chSQLDB) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}
func (c *chSQLDB) ExecCtx(ctx context.Context, query string, args ...any) error {
	_, err := c.db.ExecContext(ctx, query, args...)
	return err
}
func (c *chSQLDB) Conn(ctx context.Context) (*sql.Conn, error) { return c.db.Conn(ctx) }
func (c *chSQLDB) Begin() (*sql.Tx, error)                     { return c.db.Begin() }
func (c *chSQLDB) Close()                                      { c.db.Close() }
