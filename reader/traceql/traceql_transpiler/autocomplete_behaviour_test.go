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
		Name: s.name, ClusterName: s.cluster,
	}})
	var ch chan []string
	if run.portions == 0 {
		ch, err = planner.Process(ctx)
	} else {
		portioned := planner.(interface {
			ProcessComplexReq(*shared.PlannerContext, int64) (chan []string, error)
		})
		ch, err = portioned.ProcessComplexReq(ctx, int64(run.portions)*COMPLEXITY_THRESHOLD)
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

// assertSuggestions runs every kind for each query and compares it with the
// golden result of step, which says whether it fails. A result under a limit
// below autocompleteLimit is the first limit suggestions of the unlimited one.
func (s *searchStore) assertSuggestions(t *testing.T, step string, queries []string, run searchRun, wantError bool) {
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
			if isErr != wantError {
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
	defer writeAutocompleteGolden(t)
	s.seedStructural(t)
	s.assertSuggestions(t, "plain", plainAutocompletes, searchRun{}, false)
	s.assertSuggestions(t, "all-spans", allSpansAutocompletes, searchRun{}, false)
	s.assertSuggestions(t, "structural", structuralSearches(), searchRun{}, false)
	// 6 rounds resolve 64 deep; 5 rounds fail on the 40-deep chain.
	s.assertSuggestions(t, "deep", []string{`{.a = "1"} >> {.b = "1"}`, `{.a = "1"} > {.b = "1"}`}, searchRun{rounds: 6}, false)
	s.assertSuggestions(t, "shallow", []string{`{.a = "1"} >> {.b = "1"}`, `{.b = "1"} &<< {.a = "1"}`}, searchRun{rounds: 5}, true)
	s.assertSuggestions(t, "limit-3", []string{`{.a = "1"}`, `{.a = "1"} >> {.b = "1"}`, `{} >> {}`}, searchRun{limit: 3}, false)
	for _, portions := range []int{2, 3} {
		s.assertSuggestions(t, fmt.Sprintf("portions-%d", portions), portionedAutocompletes,
			searchRun{portions: portions}, false)
		s.assertSuggestions(t, fmt.Sprintf("portions-%d-limit-3", portions), portionedAutocompletes,
			searchRun{portions: portions, limit: 3}, false)
	}
}

// testAutocompleteOverHTTP runs some of testAutocomplete's queries over HTTP.
func testAutocompleteOverHTTP(t *testing.T, cluster bool) {
	s := openSearchStore(t, cluster, true)
	s.seedStructural(t)
	s.assertSuggestions(t, "plain", []string{`{.a = "1"}`, `{.b = "1"} | by(.a) | select(name)`}, searchRun{}, false)
	s.assertSuggestions(t, "all-spans", allSpansAutocompletes[:2], searchRun{}, false)
	s.assertSuggestions(t, "structural", []string{
		`{.a = "1"} >> {.b = "1"}`,
		`({nestedSetParent < 0 && .a = "1"} &>> {.b = "1"}) || ({nestedSetParent < 0 && .a = "1"}) | select(name, nestedSetParent)`,
	}, searchRun{}, false)
	s.assertSuggestions(t, "shallow", []string{`{.a = "1"} >> {.b = "1"}`}, searchRun{rounds: 5}, true)
	s.assertSuggestions(t, "portions-2", portionedAutocompletes[:2], searchRun{portions: 2}, false)
}

// Tag keys and values match the golden results, single node and with traces
// split across shards.
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
