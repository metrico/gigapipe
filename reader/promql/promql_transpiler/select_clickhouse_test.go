package promql_transpiler

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// The tests in this file run the planned SQL on a ClickHouse server given by
// GRID_TEST_CLICKHOUSE (an HTTP URL with credentials), in a database they
// recreate, and are skipped without it.
const chTestDB = "promql_grid_test"

var chSeed struct {
	once sync.Once
	err  error
}

// chURL returns the server URL, skipping the test when none is set.
func chURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("GRID_TEST_CLICKHOUSE")
	if u == "" {
		t.Skip("GRID_TEST_CLICKHOUSE is not set")
	}
	return u
}

// chQuery runs query in db and returns its TSV rows.
func chQuery(base, db, query string) ([][]string, error) {
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s: %s", resp.Status, b)
	}
	var rows [][]string
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		rows = append(rows, strings.Split(sc.Text(), "\t"))
	}
	return rows, sc.Err()
}

// chFixture is the data every ClickHouse test reads: gridFixture, plus
// counters sampled at most once per 15s cell, some exactly on the lattice and
// some just after it, with uneven and zero increments and a reset.
func chFixture() []rawSeries {
	out := gridFixture()
	jitter := []int64{0, 7_000, 20_000, 250, 290_000, 61_000, 1_234, 14_999}
	base := selectStartMs - 3*3_600_000
	for li, l := range []string{"a", "b"} {
		var samples []model.Sample
		v := 0.0
		for i := int64(0); i < 12*14; i++ {
			inc := float64((i*i*13+i*5+int64(li)*3)%17) + float64(li)*0.5
			v += inc
			if i == 100 {
				v = inc
			}
			if i >= 12*5 && i < 12*7 {
				continue
			}
			ts := base + i*300_000 + jitter[(i+int64(li)*3)%int64(len(jitter))]
			samples = append(samples, model.Sample{TimestampMs: ts, Value: v})
		}
		out = append(out, rawSeries{labels.FromStrings("__name__", "gc", "l", l), samples})
	}
	for _, at := range cellOffsets {
		samples := []model.Sample{{TimestampMs: cellT - 600_000, Value: 3}, {TimestampMs: cellT - 300_000, Value: 5}}
		if at > -300_000 {
			samples = append(samples, model.Sample{TimestampMs: cellT + at, Value: 11})
		}
		out = append(out, rawSeries{labels.FromStrings("__name__", "cell", "at", strconv.FormatInt(at, 10)), samples})
	}
	return out
}

// cellT is the evaluation point the cell series are placed around: each has
// samples at cellT-10m and cellT-5m, and one at cellT plus its offset.
const cellT = selectStartMs + 3_600_000

var cellOffsets = []int64{7_000, 1, 14_999, 0, -1, -300_000}

// seedClickHouse recreates chTestDB with the tables the planners read and
// writes chFixture into them, the samples in several inserts so metrics_15s
// holds unmerged cells.
func seedClickHouse(base string) error {
	for _, s := range []string{"DROP DATABASE IF EXISTS " + chTestDB, "CREATE DATABASE " + chTestDB} {
		if _, err := chQuery(base, "", s); err != nil {
			return err
		}
	}
	stmts := []string{
		`CREATE TABLE samples_v3 (fingerprint UInt64, timestamp_ns Int64, value Float64, string String, type UInt8)
			ENGINE = MergeTree ORDER BY (fingerprint, timestamp_ns)`,
		`CREATE TABLE metrics_15s (fingerprint UInt64, timestamp_ns Int64,
			last AggregateFunction(argMax, Float64, Int64), max SimpleAggregateFunction(max, Float64),
			min SimpleAggregateFunction(min, Float64), count AggregateFunction(count),
			sum SimpleAggregateFunction(sum, Float64), bytes SimpleAggregateFunction(sum, Float64), type UInt8)
			ENGINE = AggregatingMergeTree ORDER BY (fingerprint, timestamp_ns, type)`,
		`CREATE MATERIALIZED VIEW metrics_15s_mv TO metrics_15s AS SELECT fingerprint,
			intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
			argMaxState(value, samples.timestamp_ns) as last, maxSimpleState(value) as max,
			minSimpleState(value) as min, countState() as count, sumSimpleState(value) as sum,
			sumSimpleState(length(string)) as bytes, type
			FROM samples_v3 as samples GROUP BY fingerprint, timestamp_ns, type`,
		`CREATE TABLE time_series (date Date, fingerprint UInt64, labels String, name String, type UInt8)
			ENGINE = ReplacingMergeTree ORDER BY (fingerprint, type)`,
		`CREATE TABLE time_series_gin (date Date, key String, val String, fingerprint UInt64, type UInt8)
			ENGINE = ReplacingMergeTree ORDER BY (key, val, fingerprint, type)`,
	}
	for _, s := range stmts {
		if _, err := chQuery(base, chTestDB, s); err != nil {
			return err
		}
	}
	const parts = 3
	var series, gin strings.Builder
	samples := make([]strings.Builder, parts)
	for i, s := range chFixture() {
		fp := i + 1
		lbls, _ := json.Marshal(s.labels.Map())
		fmt.Fprintf(&series, "('2023-11-30', %d, '%s', '%s', 0),", fp, lbls, s.labels.Get("__name__"))
		s.labels.Range(func(l labels.Label) {
			fmt.Fprintf(&gin, "('2023-11-30', '%s', '%s', %d, 0),", l.Name, l.Value, fp)
		})
		for j, smp := range s.samples {
			fmt.Fprintf(&samples[j%parts], "(%d, %d, %v, '', 0),", fp, smp.TimestampMs*1_000_000, smp.Value)
		}
	}
	inserts := []string{
		"INSERT INTO time_series VALUES " + series.String(),
		"INSERT INTO time_series_gin VALUES " + gin.String(),
	}
	for i := range samples {
		inserts = append(inserts, "INSERT INTO samples_v3 VALUES "+samples[i].String())
	}
	for _, s := range inserts {
		if _, err := chQuery(base, chTestDB, strings.TrimSuffix(s, ",")); err != nil {
			return err
		}
	}
	return nil
}

// chQuerier serves every read by running its planned SQL on ClickHouse and
// building the series as CLokiQuerier.Select does.
type chQuerier struct {
	t           *testing.T
	url         string
	base        shared.PlannerContext
	substitutes map[string]*promql_parser.Substitute
	versionInfo dbversion.VersionInfo
	routes      map[Route]int
}

func (q *chQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.SeriesSet {
	resp, err := TranspileSelect(q.base, SelectRequest{
		Hints: hints, Matchers: matchers, Substitutes: q.substitutes, VersionInfo: q.versionInfo,
	})
	if err != nil {
		q.t.Fatal(err)
	}
	str, err := resp.Query.String(&sql.Ctx{Params: map[string]sql.SQLObject{}})
	if err != nil {
		q.t.Fatal(err)
	}
	q.routes[resp.Route]++
	rows, err := chQuery(q.url, chTestDB, str)
	if err != nil {
		q.t.Fatalf("%v\n%s", err, str)
	}
	filled := resp.Route == RouteSubstitute
	prolong := !filled && (hints.Func == "" || hints.Func == "deriv" || hints.Func == "rate" ||
		hints.Func == "delta") && hints.Step != 0
	byFp := map[string]*model.SeriesV2{}
	var order []string
	lbls := map[string]labels.Labels{}
	for _, r := range rows {
		if r[0] == "2" {
			var m map[string]string
			if err := json.Unmarshal([]byte(strings.ReplaceAll(r[4], `\'`, `'`)), &m); err != nil {
				q.t.Fatalf("labels %q: %v", r[4], err)
			}
			lbls[r[1]] = labels.FromMap(m)
			continue
		}
		ts, _ := strconv.ParseInt(r[2], 10, 64)
		v, err := strconv.ParseFloat(r[3], 64)
		if err != nil {
			q.t.Fatalf("value %q: %v", r[3], err)
		}
		s := byFp[r[1]]
		if s == nil {
			s = &model.SeriesV2{Prolong: prolong, StepMs: hints.Step}
			byFp[r[1]] = s
			order = append(order, r[1])
		}
		s.Samples = append(s.Samples, model.Sample{TimestampMs: ts, Value: v})
	}
	set := &model.SeriesSet{}
	slices.SortFunc(order, func(a, b string) int { return labels.Compare(lbls[a], lbls[b]) })
	for _, fp := range order {
		s := byFp[fp]
		s.LabelsGetter = fixedLabels{lbls[fp]}
		if filled && resp.OnGrid {
			s.Samples = model.StaleAfterGaps(s.Samples, hints.Step, hints.End)
		}
		set.Series = append(set.Series, s)
	}
	set.Reset()
	return set
}

func (*chQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*chQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*chQuerier) Close() error { return nil }

// chFillPaths returns the fill paths the server supports: the arrayJoin fill,
// then WITH FILL STALENESS.
func chFillPaths(base string) []bool {
	if _, err := chQuery(base, "", "SELECT 1 AS x ORDER BY x WITH FILL STALENESS 1"); err != nil {
		return []bool{false}
	}
	return []bool{false, true}
}

var reSampleValue = regexp.MustCompile(`(\S+) @\[`)

// roundValues rounds every sample value of a printed result to 12
// significant digits, and sorts the series of a vector.
func roundValues(s string) string {
	s = reSampleValue.ReplaceAllStringFunc(s, func(m string) string {
		v, err := strconv.ParseFloat(strings.TrimSuffix(m, " @["), 64)
		if err != nil {
			return m
		}
		return strconv.FormatFloat(v, 'g', 12, 64) + " @["
	})
	if strings.Contains(s, "=>\n") {
		return s
	}
	lines := strings.Split(s, "\n")
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// evalClickHouse evaluates query on eval with the optimizers on, every read
// served by ClickHouse. staleness selects the WITH FILL STALENESS fill.
func evalClickHouse(t *testing.T, base, query string, eval EvalGrid, staleness bool, routes map[Route]int) string {
	t.Helper()
	chSeed.once.Do(func() { chSeed.err = seedClickHouse(base) })
	if chSeed.err != nil {
		t.Fatal(chSeed.err)
	}
	v := evalQuery(t, query, eval, planOpts{tag: true, metrics15s: true}, func(pc shared.PlannerContext,
		subs map[string]*promql_parser.Substitute, vi dbversion.VersionInfo) storage.Querier {
		vi = dbversion.VersionInfo{dbversion.CapMetrics15s: 1}
		if staleness {
			vi[dbversion.CapStaleness] = 1
		}
		return &chQuerier{t: t, url: base, base: pc, substitutes: subs, versionInfo: vi, routes: routes}
	})
	return roundValues(v.String())
}

var substituteQueries = []string{
	"rate(gc[15m])",
	"rate(gc[1h])",
	"increase(gc[15m])",
	"increase(gc[1h] offset 7m)",
	"delta(gx[15m])",
	"resets(gc[1h])",
	"changes(gx[15m])",
	"sum(rate(gc[15m]))",
	"sum by (l) (gx)",
	"sum by (l) (gd)",
	"avg(gd)",
	"max by (l) (gx offset 7m)",
	"avg_over_time(gd[1h])",
	"max_over_time(gd[5m])",
	"min_over_time(gx[15m])",
	"count_over_time(gd[15m])",
	"sum_over_time(gx[1h])",
	"present_over_time(gx[15m])",
	"max_over_time(gx[5m] @ 1700000100)",
	"max_over_time(rate(gc[15m])[1h:5m])",
}

// A substitute on the lattice returns what the engine computes over every
// raw sample, on each fill path the server supports.
func TestTranspileSelectSubstituteMatchesRawSamplesClickHouse(t *testing.T) {
	base := chURL(t)
	data := chFixture()
	routes := map[Route]int{}
	paths := chFillPaths(base)
	for _, staleness := range paths {
		for _, g := range latticeGrids() {
			for _, query := range substituteQueries {
				t.Run(fmt.Sprintf("staleness=%v %s %s", staleness, g.name, query), func(t *testing.T) {
					want := roundValues(evalGrid(t, query, g.eval, data, true, nil))
					if got := evalClickHouse(t, base, query, g.eval, staleness, routes); got != want {
						t.Fatalf("got\n%s\nwant\n%s", got, want)
					}
				})
			}
		}
	}
	if routes[RouteSubstitute] < 250*len(paths) {
		t.Fatalf("only %d substitute reads (%v)", routes[RouteSubstitute], routes)
	}
}

// The selector reads served from ClickHouse return what the engine computes
// over every raw sample. last_over_time is left out: its substitute drops
// __name__.
func TestTranspileSelectDownsampleMatchesRawSamplesClickHouse(t *testing.T) {
	base := chURL(t)
	data := chFixture()
	paths := chFillPaths(base)
	staleness := paths[len(paths)-1]
	for _, g := range latticeGrids() {
		for _, query := range gridQueries {
			if strings.HasPrefix(query, "last_over_time") {
				continue
			}
			t.Run(g.name+" "+query, func(t *testing.T) {
				want := roundValues(evalGrid(t, query, g.eval, data, true, nil))
				if got := evalClickHouse(t, base, query, g.eval, staleness, map[Route]int{}); got != want {
					t.Fatalf("got\n%s\nwant\n%s", got, want)
				}
			})
		}
	}
}

// A substitute sees a sample exactly at T at T, and none inside the cell
// [T, T+15s).
func TestTranspileSelectSubstituteExcludesTheCellAfterTClickHouse(t *testing.T) {
	base := chURL(t)
	data := chFixture()
	evals := []EvalGrid{
		{StartMs: cellT, EndMs: cellT},
		{StartMs: cellT - 3_600_000, EndMs: cellT + 3_600_000, StepMs: 300_000},
		{StartMs: cellT, EndMs: cellT + 3_600_000, StepMs: 3_600_000},
	}
	queries := []string{"max_over_time(cell[5m])", "count_over_time(cell[15m])", "sum by (at) (cell)",
		"rate(cell[15m])", "changes(cell[15m])", "sum(increase(cell[15m]))"}
	for _, staleness := range chFillPaths(base) {
		for _, eval := range evals {
			for _, query := range queries {
				t.Run(fmt.Sprintf("staleness=%v %+v %s", staleness, eval, query), func(t *testing.T) {
					routes := map[Route]int{}
					want := roundValues(evalGrid(t, query, eval, data, true, nil))
					if got := evalClickHouse(t, base, query, eval, staleness, routes); got != want {
						t.Fatalf("got\n%s\nwant\n%s", got, want)
					}
					if routes[RouteSubstitute] == 0 {
						t.Fatalf("no substitute read: %v", routes)
					}
				})
			}
		}
	}
}
