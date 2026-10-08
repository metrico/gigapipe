package logql_transpiler

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The tests in this file run the planned SQL on a ClickHouse server given by
// GRID_TEST_CLICKHOUSE (an HTTP URL with credentials), in databases they
// recreate, and are skipped without it. Cluster runs need a cluster
// grid_two_shards whose shards use the databases chShardDBs; they run in
// chLogDB, whose *_dist tables read the shards.
const (
	chLogDB   = "logql_grid_test"
	chCluster = "grid_two_shards"
)

var chShardDBs = [2]string{"logql_grid_s1", "logql_grid_s2"}

var chLogSeed struct {
	once sync.Once
	err  error
}

func chLogURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("GRID_TEST_CLICKHOUSE")
	if u == "" {
		t.Skip("GRID_TEST_CLICKHOUSE is not set")
	}
	chLogSeed.once.Do(func() { chLogSeed.err = seedLogClickHouse(u, append(gridLines(), nonFiniteLines()...)) })
	if chLogSeed.err != nil {
		t.Fatal(chLogSeed.err)
	}
	return u
}

// chPost runs query in db and returns the response body.
func chPost(base, db, query string) (io.ReadCloser, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	if db != "" {
		q.Set("database", db)
	}
	u.RawQuery = q.Encode()
	resp, err := http.Post(u.String(), "text/plain", strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("%s: %s", resp.Status, b)
	}
	return resp.Body, nil
}

func chExec(base, db, query string) error {
	body, err := chPost(base, db, query)
	if err != nil {
		return err
	}
	return body.Close()
}

// seedLogClickHouse writes lines into chLogDB, and split by fingerprint into
// the shard databases.
func seedLogClickHouse(base string, lines []logLine) error {
	tables := []string{
		`samples_v3 (fingerprint UInt64, timestamp_ns Int64, value Float64, string String, type UInt8)
			ENGINE = MergeTree ORDER BY (fingerprint, timestamp_ns)`,
		`time_series (date Date, fingerprint UInt64, labels String, name String, type UInt8)
			ENGINE = ReplacingMergeTree ORDER BY (fingerprint, type)`,
		`time_series_gin (date Date, key String, val String, fingerprint UInt64, type UInt8)
			ENGINE = ReplacingMergeTree ORDER BY (key, val, fingerprint, type)`,
	}
	dbs := append([]string{chLogDB}, chShardDBs[:]...)
	for _, db := range dbs {
		for _, s := range []string{"DROP DATABASE IF EXISTS " + db, "CREATE DATABASE " + db} {
			if err := chExec(base, "", s); err != nil {
				return err
			}
		}
		for _, t := range tables {
			if err := chExec(base, db, "CREATE TABLE "+t); err != nil {
				return err
			}
		}
	}
	type rows struct{ series, gin, samples strings.Builder }
	all, shards := &rows{}, [len(chShardDBs)]*rows{{}, {}}
	seen := map[uint64]bool{}
	for _, l := range lines {
		for _, r := range []*rows{all, shards[l.fp%uint64(len(chShardDBs))]} {
			fmt.Fprintf(&r.samples, "(%d, %d, 0, '%s', 1),", l.fp, l.ts, l.line)
			if seen[l.fp] {
				continue
			}
			lbls, _ := json.Marshal(l.labels)
			for _, day := range []time.Time{gridD0.AddDate(0, 0, -1), gridD0} {
				d := day.Format("2006-01-02")
				fmt.Fprintf(&r.series, "('%s', %d, '%s', '', 1),", d, l.fp, lbls)
				for k, v := range l.labels {
					fmt.Fprintf(&r.gin, "('%s', '%s', '%s', %d, 1),", d, k, v, l.fp)
				}
			}
		}
		seen[l.fp] = true
	}
	insert := func(db string, r *rows) error {
		for _, s := range []string{
			"INSERT INTO time_series VALUES " + r.series.String(),
			"INSERT INTO time_series_gin VALUES " + r.gin.String(),
			"INSERT INTO samples_v3 VALUES " + r.samples.String(),
		} {
			if err := chExec(base, db, strings.TrimSuffix(s, ",")); err != nil {
				return err
			}
		}
		return nil
	}
	if err := insert(chLogDB, all); err != nil {
		return err
	}
	for i, db := range chShardDBs {
		if err := insert(db, shards[i]); err != nil {
			return err
		}
	}
	for _, t := range []string{"samples_v3", "time_series", "time_series_gin"} {
		q := fmt.Sprintf("CREATE TABLE %s_dist AS %[1]s ENGINE = Distributed(%s, '', %[1]s)", t, chCluster)
		if err := chExec(base, chLogDB, q); err != nil {
			return err
		}
	}
	return nil
}

// chModes returns the request modes to run: single node, and the cluster
// when the server defines chCluster.
func chModes(t *testing.T, base string) []bool {
	body, err := chPost(base, "", fmt.Sprintf("SELECT count() FROM system.clusters WHERE cluster = '%s'", chCluster))
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	b, _ := io.ReadAll(body)
	if strings.TrimSpace(string(b)) != fmt.Sprint(len(chShardDBs)) {
		t.Logf("no cluster %s: single node only", chCluster)
		return []bool{false}
	}
	return []bool{false, true}
}

// chDB is a model.ISqlxDB that runs matrix queries on ClickHouse over HTTP.
type chDB struct {
	base string
	db   *sql.DB
	mu   sync.Mutex
	sql  []string
}

func newChDB(base string) *chDB {
	c := &chDB{base: base}
	c.db = sql.OpenDB(chConnector{c})
	return c
}

func (c *chDB) GetName() string { return "clickhouse" }

func (c *chDB) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	c.mu.Lock()
	c.sql = append(c.sql, query)
	c.mu.Unlock()
	return c.db.QueryContext(ctx, query, args...)
}

func (c *chDB) ExecCtx(ctx context.Context, query string, args ...any) error {
	_, err := c.db.ExecContext(ctx, query, args...)
	return err
}

func (c *chDB) Conn(ctx context.Context) (*sql.Conn, error) { return c.db.Conn(ctx) }
func (c *chDB) Begin() (*sql.Tx, error)                     { return c.db.Begin() }
func (c *chDB) Close()                                      { c.db.Close() }

func (c *chDB) statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.sql)
}

type chConnector struct{ c *chDB }

func (k chConnector) Connect(context.Context) (driver.Conn, error) { return chConn(k), nil }
func (k chConnector) Driver() driver.Driver                        { return fakeDriver{} }

type chConn struct{ c *chDB }

func (c chConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c chConn) Close() error                        { return nil }
func (c chConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

// QueryContext reads the four result columns: fingerprint, labels, the value
// or line, and timestamp_ns.
func (c chConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	body, err := chPost(c.c.base, chLogDB, query+" FORMAT JSONCompactEachRowWithNamesAndTypes SETTINGS "+
		"output_format_json_quote_64bit_integers = 0, output_format_json_quote_denormals = 1")
	if err != nil {
		return nil, err
	}
	defer body.Close()
	rows := &chRows{}
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	var types []string
	for i := 0; sc.Scan(); i++ {
		switch i {
		case 0:
			continue
		case 1:
			if err := json.Unmarshal(sc.Bytes(), &types); err != nil {
				return nil, err
			}
			continue
		}
		var raw []json.RawMessage
		if err := json.Unmarshal(sc.Bytes(), &raw); err != nil {
			return nil, err
		}
		var (
			fp          uint64
			labels      map[string]string
			valueOrLine any
			ts          int64
		)
		if err := json.Unmarshal(raw[0], &fp); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw[1], &labels); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw[2], &valueOrLine); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw[3], &ts); err != nil {
			return nil, err
		}
		if str, ok := valueOrLine.(string); ok && types[2] == "Float64" {
			if valueOrLine, err = strconv.ParseFloat(str, 64); err != nil {
				return nil, err
			}
		}
		rows.rows = append(rows.rows, []driver.Value{fp, labels, valueOrLine, ts})
	}
	return rows, sc.Err()
}

type chRows struct {
	rows [][]driver.Value
	i    int
}

func (r *chRows) Columns() []string {
	return []string{"fingerprint", "labels", "value", "timestamp_ns"}
}
func (r *chRows) Close() error { return nil }
func (r *chRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

// TestRawSQLEvaluatesOnTheGridOnClickHouse runs every raw SQL query on
// ClickHouse and checks it against (T-offset-R, T] on every grid.
func TestRawSQLEvaluatesOnTheGridOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := gridLines()
	for _, cluster := range chModes(t, base) {
		for _, c := range rawSQLCases {
			for name, req := range gridRequests {
				req.cluster = cluster
				t.Run(fmt.Sprintf("%s@%s/cluster=%v", c.query, name, cluster), func(t *testing.T) {
					db := newChDB(base)
					defer db.Close()
					p := serveRequest(t, c.query, req, db)
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

// nonFiniteLines is a stream of finite, infinite and NaN values, one every
// two minutes.
func nonFiniteLines() []logLine {
	vals := []string{"1", "2", "inf", "3", "4", "5", "-inf", "6", "7", "8", "nan", "9", "10", "11", "inf", "-inf", "12"}
	var lines []logLine
	for i, v := range vals {
		lines = append(lines, logLine{fp: 99, labels: map[string]string{"job": "nf"},
			ts: gridD0.Add(time.Duration(2*i+1) * time.Minute).UnixNano(), line: "v=" + v})
	}
	return lines
}

// TestRawSQLNonFiniteSumsOnClickHouse: an Inf or NaN counts only in the
// windows that hold it.
func TestRawSQLNonFiniteSumsOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := nonFiniteLines()
	for _, c := range []struct {
		fn  string
		agg func(sum, n float64) float64
	}{
		{"sum_over_time", func(sum, _ float64) float64 { return sum }},
		{"avg_over_time", func(sum, n float64) float64 { return sum / n }},
		{"rate", func(sum, _ float64) float64 { return sum / 300 }},
	} {
		q := c.fn + `({job="nf"} | regexp "v=(?P<v>[a-z0-9-]+)" | unwrap v [5m]) by (job)`
		for _, req := range []request{
			rangeReq(gridD0, gridD0.Add(40*time.Minute), time.Minute),
			instantReq(gridD0.Add(9 * time.Minute)),
		} {
			db := newChDB(base)
			p := serveRequest(t, q, req, db)
			db.Close()
			got := map[int64]float64{}
			for _, e := range p.out {
				got[e.TimestampNS] = e.Value
			}
			for _, T := range refGrid(req) {
				var sum, n float64
				for _, l := range lines {
					if l.ts > T-(5*time.Minute).Nanoseconds() && l.ts <= T {
						v, _ := strconv.ParseFloat(strings.TrimPrefix(l.line, "v="), 64)
						sum += v
						n++
					}
				}
				g, ok := got[T]
				if n == 0 {
					if ok {
						t.Errorf("%s at %d: got %g, want nothing", q, T, g)
					}
					continue
				}
				w := c.agg(sum, n)
				if same := g == w || math.IsNaN(g) && math.IsNaN(w) || math.Abs(g-w) < 1e-9; !ok || !same {
					t.Errorf("%s at %d: got %g (%v), want %g", q, T, g, ok, w)
				}
			}
		}
	}
}

// TestRawSQLTopKOnClickHouse checks that topk ranks the series at each T.
func TestRawSQLTopKOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := gridLines()
	ref := refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum"}
	for name, req := range gridRequests {
		db := newChDB(base)
		p := serveRequest(t, `topk(1, sum by (l) (`+rawCount5m+`))`, req, db)
		db.Close()
		all := reference(lines, ref, refGrid(req))
		want := map[string]map[int64]float64{}
		for _, ts := range refGrid(req) {
			best, bestV := "", math.Inf(-1)
			for k, pts := range all {
				if v, ok := pts[ts]; ok && v > bestV {
					best, bestV = k, v
				}
			}
			if best != "" {
				if want[best] == nil {
					want[best] = map[int64]float64{}
				}
				want[best][ts] = bestV
			}
		}
		if d := diffSeries(gotSeries(p), want); len(d) > 0 {
			t.Errorf("%s: %v", name, d[:min(len(d), 6)])
		}
	}
}

// TestRawSQLBinariesOnClickHouse checks SQL and mixed Go/SQL binaries with a
// raw SQL operand point by point.
func TestRawSQLBinariesOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := gridLines()
	count := refQuery{fn: "count_over_time", r: 5 * time.Minute}
	shifted := refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 5 * time.Minute}
	join := func(a, b map[string]map[int64]float64, op func(x, y float64) float64) map[string]map[int64]float64 {
		out := map[string]map[int64]float64{}
		for k, pts := range a {
			for ts, v := range pts {
				w, ok := b[k][ts]
				if !ok || op(v, w) == 0 {
					continue
				}
				if out[k] == nil {
					out[k] = map[int64]float64{}
				}
				out[k][ts] = op(v, w)
			}
		}
		return out
	}
	for _, c := range []struct {
		query string
		op    func(x, y float64) float64
	}{
		{rawCount5m + ` * count_over_time(` + rawSel + ` [5m] offset 5m)`, func(x, y float64) float64 { return x * y }},
		{goCount + ` - count_over_time(` + rawSel + ` [5m] offset 5m)`, func(x, y float64) float64 { return x - y }},
		{`count_over_time(` + rawSel + ` [5m] offset 5m) - ` + goCount, func(x, y float64) float64 { return y - x }},
	} {
		for _, cluster := range chModes(t, base) {
			for name, req := range gridRequests {
				req.cluster = cluster
				t.Run(fmt.Sprintf("%s@%s/cluster=%v", c.query, name, cluster), func(t *testing.T) {
					db := newChDB(base)
					defer db.Close()
					p := serveRequest(t, c.query, req, db)
					times := refGrid(req)
					want := join(reference(lines, count, times), reference(lines, shifted, times), c.op)
					if d := diffSeries(gotSeries(p), want); len(d) > 0 {
						t.Errorf("%v", d[:min(len(d), 6)])
					}
				})
			}
		}
	}
}
