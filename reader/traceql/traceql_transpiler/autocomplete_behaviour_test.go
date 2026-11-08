package traceql_transpiler

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// updateAutocompleteGolden rewrites autocompleteGoldenFile from the results of this run.
var updateAutocompleteGolden = flag.Bool("update-autocomplete-golden", false,
	"rewrite "+autocompleteGoldenFile+"; run it on the commit whose results the file pins")

// autocompleteGoldenFile holds the expected tag keys and values, by test,
// step, kind, query and ancestry rounds.
const autocompleteGoldenFile = "testdata/autocomplete_golden.json"

// autocompleteLimit is a suggestion limit that cuts no result.
const autocompleteLimit = 100

// autocompleteGolden holds the golden results; autocompleteGoldenRun those
// written in this run under -update-autocomplete-golden.
var (
	autocompleteGolden    map[string][]string
	autocompleteGoldenRun = map[string][]string{}
)

func loadAutocompleteGolden(t *testing.T) {
	t.Helper()
	goldenMu.Lock()
	defer goldenMu.Unlock()
	if autocompleteGolden != nil {
		return
	}
	autocompleteGolden = map[string][]string{}
	b, err := os.ReadFile(autocompleteGoldenFile)
	if err != nil {
		if !*updateAutocompleteGolden {
			t.Fatal(err)
		}
		return
	}
	if err := json.Unmarshal(b, &autocompleteGolden); err != nil {
		t.Fatal(err)
	}
}

// autocompleteKinds are the suggestions a query is checked for: tag keys,
// values of name (one per span) and values of a.
var autocompleteKinds = []string{"tags", "values name", "values a"}

// suggest runs kind for query: its suggestions, sorted when the query selects
// every span, or the error.
func (s *searchStore) suggest(t *testing.T, kind, query string, run searchRun) []string {
	t.Helper()
	var q *traceql_ir.Query
	if query != "" {
		var err error
		if q, err = traceql_ir.Parse(query); err != nil {
			t.Fatal(err)
		}
	}
	var (
		planner shared.GenericTraceRequestProcessor[string]
		err     error
	)
	if key, ok := strings.CutPrefix(kind, "values "); ok {
		planner, err = PlanValuesV2(q, key)
	} else {
		planner, err = PlanTagsV2(q)
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx := &shared.PlannerContext{
		Ctx: context.Background(), CHDb: s.db, IsCluster: s.cluster != "",
		// To an hour on reaches the seeded date in tempo_traces_kv's date bounds.
		From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Hour),
		Limit: int64(run.limit), AncestryRounds: run.rounds,
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	distconfig.Init()
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
		Name: s.name, ClusterName: s.cluster, SkipUnavailableShards: s.skipUnavailable,
	}})
	var ch chan []string
	if run.portions == 0 {
		ch, err = planner.Process(ctx)
	} else {
		portioned := planner.(interface {
			ProcessComplexReq(*shared.PlannerContext, int64) (chan []string, error)
		})
		ch, err = portioned.ProcessComplexReq(ctx, int64(run.portions)*traceqlconfig.PortionSpans())
	}
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	res := []string{}
	for part := range ch {
		res = append(res, part...)
	}
	if clickhouse_transpiler.AllSpans(q) {
		slices.Sort(res)
	}
	return res
}

// assertSuggestions runs every kind for each query, which must not fail, and
// compares it with the golden result of step. A result under a limit
// below autocompleteLimit is the first limit suggestions of the unlimited one.
func (s *searchStore) assertSuggestions(t *testing.T, step string, queries []string, run searchRun) {
	t.Helper()
	loadAutocompleteGolden(t)
	checkPrefix := run.limit > 0
	if run.limit == 0 {
		run.limit = autocompleteLimit
	}
	test := strings.SplitN(t.Name(), "/", 2)[0]
	test = strings.TrimSuffix(strings.TrimSuffix(test, "OnCluster"), "OverHTTP")
	for _, query := range queries {
		for _, kind := range autocompleteKinds {
			key := fmt.Sprintf("%s/%s/%s/%s/%d", test, step, kind, query, run.rounds)
			got := s.suggest(t, kind, query, run)
			isErr := len(got) == 1 && strings.HasPrefix(got[0], "error: ")
			if isErr {
				t.Errorf("%s: got %.300q", key, got)
			}
			if !isErr && !slices.IsSorted(got) {
				t.Errorf("%s: not sorted: %q", key, got)
			}
			if !isErr && checkPrefix {
				full := s.suggest(t, kind, query, searchRun{rounds: run.rounds, limit: autocompleteLimit})
				if want := full[:min(run.limit, len(full))]; !slices.Equal(got, want) {
					t.Errorf("%s: %q, want the first %d of %q", key, got, run.limit, full)
				}
			}
			if *updateAutocompleteGolden {
				goldenMu.Lock()
				if prev, ok := autocompleteGoldenRun[key]; ok && !slices.Equal(prev, got) {
					t.Errorf("%s: %q, and %q earlier in this run", key, got, prev)
				}
				autocompleteGoldenRun[key] = got
				goldenMu.Unlock()
				continue
			}
			if want, ok := autocompleteGolden[key]; !ok {
				t.Errorf("%s: no golden result", key)
			} else if !slices.Equal(got, want) {
				t.Errorf("%s:\ngot:  %q\nwant: %q", key, got, want)
			}
		}
	}
}

// writeAutocompleteGolden merges this run's results into
// autocompleteGoldenFile under -update-autocomplete-golden.
func writeAutocompleteGolden(t *testing.T) {
	t.Helper()
	if !*updateAutocompleteGolden {
		return
	}
	goldenMu.Lock()
	defer goldenMu.Unlock()
	all := map[string][]string{}
	if b, err := os.ReadFile(autocompleteGoldenFile); err == nil {
		if err := json.Unmarshal(b, &all); err != nil {
			t.Fatal(err)
		}
	}
	for k, v := range autocompleteGoldenRun {
		all[k] = v
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(all); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(autocompleteGoldenFile, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// plainAutocompletes are the plainSearches that read a search result.
var plainAutocompletes = slices.DeleteFunc(slices.Clone(plainSearches), func(q string) bool { return q == `{}` })

// allSpansAutocompletes select every span, so they read no search result.
var allSpansAutocompletes = []string{``, `{}`, `({})`}

// portionedAutocompletes run in complexity portions.
var portionedAutocompletes = []string{
	`{.a = "1"}`,
	`{.a = "1"} >> {.b = "1"} | select(.a)`,
	`{.b = "1"} | by(.a) | select(name)`,
	`({nestedSetParent < 0 && .a = "1"} &>> {.b = "1"}) || ({nestedSetParent < 0 && .a = "1"}) | select(name, nestedSetParent)`,
}

func testAutocomplete(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	s.traceID = spreadTraceID
	defer writeAutocompleteGolden(t)
	s.seedStructural(t)
	s.assertSuggestions(t, "plain", plainAutocompletes, searchRun{})
	s.assertSuggestions(t, "all-spans", allSpansAutocompletes, searchRun{})
	s.assertSuggestions(t, "structural", structuralSearches(), searchRun{})
	// 6 rounds resolve 64 deep; 5 rounds leave the 40-deep chain unresolved.
	s.assertSuggestions(t, "deep", []string{`{.a = "1"} >> {.b = "1"}`, `{.a = "1"} > {.b = "1"}`}, searchRun{rounds: 6})
	s.assertSuggestions(t, "shallow", []string{`{.a = "1"} >> {.b = "1"}`, `{.b = "1"} &<< {.a = "1"}`}, searchRun{rounds: 5})
	s.assertSuggestions(t, "limit-3", []string{`{.a = "1"}`, `{.a = "1"} >> {.b = "1"}`, `{} >> {}`}, searchRun{limit: 3})
	for _, portions := range []int{2, 3} {
		s.assertSuggestions(t, fmt.Sprintf("portions-%d", portions), portionedAutocompletes,
			searchRun{portions: portions})
		s.assertSuggestions(t, fmt.Sprintf("portions-%d-limit-3", portions), portionedAutocompletes,
			searchRun{portions: portions, limit: 3})
	}
}

// testAutocompleteOverHTTP runs some of testAutocomplete's queries over HTTP.
func testAutocompleteOverHTTP(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, true)
	s.traceID = spreadTraceID
	s.seedStructural(t)
	s.assertSuggestions(t, "plain", []string{`{.a = "1"}`, `{.b = "1"} | by(.a) | select(name)`}, searchRun{})
	s.assertSuggestions(t, "all-spans", allSpansAutocompletes[:2], searchRun{})
	s.assertSuggestions(t, "structural", []string{
		`{.a = "1"} >> {.b = "1"}`,
		`({nestedSetParent < 0 && .a = "1"} &>> {.b = "1"}) || ({nestedSetParent < 0 && .a = "1"}) | select(name, nestedSetParent)`,
	}, searchRun{})
	s.assertSuggestions(t, "shallow", []string{`{.a = "1"} >> {.b = "1"}`}, searchRun{rounds: 5})
	s.assertSuggestions(t, "portions-2", portionedAutocompletes[:2], searchRun{portions: 2})
}

// Tag keys and values match the golden results, single node and on a two-shard
// cluster.
func TestAutocomplete(t *testing.T) { testAutocomplete(t, false) }

func TestAutocompleteOnCluster(t *testing.T) { testAutocomplete(t, true) }

// Tag keys and values over HTTP are the golden results.
func TestAutocompleteOverHTTP(t *testing.T) { testAutocompleteOverHTTP(t, false) }

func TestAutocompleteOverHTTPOnCluster(t *testing.T) { testAutocompleteOverHTTP(t, true) }

// statementRecorder records the statements a session runs.
type statementRecorder struct {
	model.ISqlxDB
	mu         sync.Mutex
	statements []string
}

func (r *statementRecorder) QueryCtx(ctx context.Context, query string, args ...any) (*stdsql.Rows, error) {
	r.mu.Lock()
	r.statements = append(r.statements, query)
	r.mu.Unlock()
	return r.ISqlxDB.QueryCtx(ctx, query, args...)
}

// take counts the recorded statements: all, those that list span ids and
// those that read SearchResultTable, and forgets them.
func (r *statementRecorder) take() (all, ids, data int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	all = len(r.statements)
	for _, q := range r.statements {
		switch {
		case strings.Contains(q, "groupArray(span_id)"):
			ids++
		case strings.Contains(q, "FROM "+clickhouse_transpiler.SearchResultTable):
			data++
		}
	}
	r.statements = nil
	return all, ids, data
}

// A request lists its span ids once per complexity portion and reads them
// once; without ids it reads nothing more. A query that selects every span
// runs one statement and no complexity estimate.
func TestAutocompleteListsIdsOncePerPortion(t *testing.T) {
	s := openSearchStore(t, false, false)
	s.traceID = spreadTraceID
	s.seedStructural(t)
	rec := &statementRecorder{ISqlxDB: s.db}
	s.db = rec
	for _, c := range []struct {
		query                      string
		portions                   int
		wantAll, wantIds, wantData int
	}{
		{`{.a = "1"} >> {.b = "1"}`, 0, 3, 1, 1},
		{`{.a = "1"} >> {.b = "1"}`, 3, 6, 3, 3},
		{`{.c = "1"}`, 0, 2, 1, 0},
		{`{.c = "1"}`, 2, 2, 2, 0},
		{``, 0, 1, 0, 0},
		{`{}`, 0, 1, 0, 0},
	} {
		for _, kind := range autocompleteKinds {
			got := s.suggest(t, kind, c.query, searchRun{limit: autocompleteLimit, portions: c.portions})
			all, ids, data := rec.take()
			if all != c.wantAll || ids != c.wantIds || data != c.wantData {
				t.Errorf("%s %s in %d portions: %d statements, %d ids and %d data, want %d, %d and %d",
					kind, c.query, c.portions, all, ids, data, c.wantAll, c.wantIds, c.wantData)
			}
			if len(got) == 1 && strings.HasPrefix(got[0], "error: ") {
				t.Errorf("%s %s: %s", kind, c.query, got[0])
			}
		}
	}
}

// spreadTraceID spreads trace numbers over the id space: odd ones as 64-bit
// ids with a zero high half, even ones as 128-bit ids.
func spreadTraceID(trace int) string {
	mixed := uint64(trace) * 0x9e3779b97f4a7c15
	if trace%2 == 1 {
		return fmt.Sprintf("%016x%016x", 0, mixed)
	}
	return fmt.Sprintf("%016x%016x", mixed, trace)
}

// takeStatements returns the recorded statements and forgets them.
func (r *statementRecorder) takeStatements() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := r.statements
	r.statements = nil
	return res
}

// portionedScalarAndSpanOnly are the scalar filters, including the keyless
// pivot's read of tempo_traces, and a span-only filter, each matching seeded spans.
var portionedScalarAndSpanOnly = []string{
	`{.b = "1"} | count() > 1`,
	`{.b = "1"} | avg(.a) > 0`,
	`{.b = "1"} | max(duration) > 0ns`,
	`{duration > 1ns}`,
}

// Portioned suggestions equal the unportioned ones, and each portion's ids and
// data statements read a trace_id range.
func testAutocompletePortionsEqualTheWhole(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, false)
	s.traceID = spreadTraceID
	s.seedStructural(t)
	rec := &statementRecorder{ISqlxDB: s.db}
	s.db = rec
	queries := append(append(structuralSearches(), portionedAutocompletes...), portionedScalarAndSpanOnly...)
	for _, query := range queries {
		for _, kind := range autocompleteKinds {
			for _, run := range []searchRun{{limit: autocompleteLimit, portions: 2}, {limit: autocompleteLimit, portions: 5}, {limit: 3, portions: 3}} {
				whole := s.suggest(t, kind, query, searchRun{limit: run.limit})
				rec.takeStatements()
				if len(whole) == 0 && slices.Contains(portionedScalarAndSpanOnly, query) {
					t.Errorf("%s %s: no suggestions to compare", kind, query)
				}
				got := s.suggest(t, kind, query, run)
				if len(got) == 1 && strings.HasPrefix(got[0], "error: ") || !slices.Equal(got, whole) {
					t.Errorf("%s %s limit %d in %d portions:\ngot:  %q\nwant: %q", kind, query, run.limit, run.portions, got, whole)
				}
				for _, q := range rec.takeStatements() {
					portioned := strings.Contains(q, "groupArray(span_id)") ||
						strings.Contains(q, "FROM "+clickhouse_transpiler.SearchResultTable)
					if portioned && (!strings.Contains(q, "trace_id >= unhex(") || strings.Contains(q, "cityHash64(trace_id)")) {
						t.Errorf("%s %s in %d portions: statement without a trace_id range:\n%.400s", kind, query, run.portions, q)
					}
				}
			}
		}
	}
}

func TestAutocompletePortionsEqualTheWhole(t *testing.T) {
	testAutocompletePortionsEqualTheWhole(t, false)
}

func TestAutocompletePortionsEqualTheWholeOnCluster(t *testing.T) {
	testAutocompletePortionsEqualTheWhole(t, true)
}
