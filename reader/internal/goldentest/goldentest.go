// Package goldentest is the shared golden-SQL fixture infrastructure used by
// every query head's conformance corpus (PromQL, LogQL, TraceQL): a TXTAR
// fixture format, a loader, a comparison-with-diff, an -update regeneration
// mode, and a doc-comment-mandated naming convention so the corpora and CI
// stay wired together. See recordingdb.go for the recording fake DB used to
// drive the real PromQL engine, and parserpin.go for the parser-pin helper.
//
// # Fixture format
//
// Each fixture is one .txtar file (golang.org/x/tools/txtar) under a head's
// own testdata/golden directory, e.g.
// reader/promql/promql_transpiler/testdata/golden/rate_5m.txtar. A txtar file
// is a free-text Comment followed by named sections marked by a "-- name --"
// line. This package recognizes exactly these section names:
//
//   - "query" (required): the raw query text, exactly as fed to the parser.
//     Trailing newline is stripped on load.
//
//   - "meta" (optional): "key: value" lines of per-fixture metadata that a
//     head defines for itself — e.g. step/range for PromQL, the simple-vs-
//     complex routing decision for TraceQL. goldentest does not interpret
//     the keys; it only splits each line on the first ':' into Fixture.Meta.
//     Blank lines and lines starting with '#' are ignored.
//
//   - "sql-0", "sql-1", ... (at least one, unless "error" is present): the
//     golden SQL text for each statement a head's harness produces, in
//     order. Sections are always the zero-indexed "sql-N" form — even a
//     single-statement fixture uses "sql-0" — so a regenerating writer never
//     has to choose between a bare "sql" and "sql-0" spelling.
//
//   - "error" (optional): an expected error substring, checked with
//     strings.Contains against the produced error's Error() text. A fixture
//     may carry "error" instead of any "sql-N" section (e.g. an
//     unsupported-function fallback that must fail to plan), or, for a head
//     where that is meaningful, alongside them.
//
// Any other section name is a load error, to catch typos early rather than
// silently ignoring a fixture author's intent.
//
// Example fixture, reader/promql/promql_transpiler/testdata/golden/rate_5m.txtar:
//
//	-- meta --
//	step: 15s
//	range: 5m
//	route: downsampled
//	-- query --
//	sum(rate(http_requests_total{job="api"}[5m]))
//	-- sql-0 --
//	SELECT sum(...) FROM metrics_15s ...
//
// # -update mode
//
// Update is a package-level flag, `var Update = flag.Bool("update", false,
// ...)`, registered as a side effect of importing this package. Because it
// is declared in an ordinary (non-_test.go) source file, its var initializer
// runs — and so the flag is registered on flag.CommandLine — for any test
// binary that transitively imports goldentest, before testing.Main parses
// os.Args. No TestMain plumbing is required in a consuming head's test
// package; it is enough to import goldentest and call Fixture.CheckSQL /
// Fixture.CheckError from a test function. Concretely:
//
//	func TestGolden(t *testing.T) {
//	    fixtures, err := goldentest.LoadDir("testdata/golden")
//	    if err != nil {
//	        t.Fatal(err)
//	    }
//	    for _, f := range fixtures {
//	        f := f
//	        t.Run(f.Name, func(t *testing.T) {
//	            got := renderSQL(t, f.Query, f.Meta) // head-specific rendering
//	            f.CheckSQL(t, got...)
//	        })
//	    }
//	}
//
// Run `go test ./reader/<head>/... -run TestGolden -update` (or
// `just update-golden <head>`) to regenerate every fixture's golden section
// from the head's current output. Regenerating with no code change must
// produce a byte-identical file (see writeFile): re-run it twice and diff.
//
// # TestGolden naming convention
//
// Every head's golden-driving test function MUST be named exactly TestGolden
// in its package (t.Run subtests per fixture are expected and encouraged).
// The Justfile's `update-golden` recipe runs
// `go test ./reader/<head>/... -run TestGolden -update`, and the merge
// gate's corpus lanes key off the same name; a differently-named test
// silently will not be regenerated or gated.
package goldentest

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/txtar"
)

// Update enables golden-fixture regeneration instead of comparison. See the
// package doc comment for exactly how a consuming test wires it up.
var Update = flag.Bool("update", false, "regenerate golden corpus fixtures from current output")

const (
	sectionQuery = "query"
	sectionMeta  = "meta"
	sectionError = "error"
	sqlPrefix    = "sql-"
)

// Fixture is one loaded .txtar golden fixture.
type Fixture struct {
	// Name is the fixture's base file name without the .txtar extension.
	// Use it as the t.Run subtest name so a failure names the fixture
	// directly (e.g. in `go test -run TestGolden/rate_5m`).
	Name string
	// Query is the trimmed contents of the "query" section.
	Query string
	// Meta holds the "key: value" pairs parsed from the optional "meta"
	// section. Never nil; empty if the fixture has no meta section.
	Meta map[string]string
	// SQL holds the trimmed contents of the "sql-0", "sql-1", ... sections,
	// in order. Empty if the fixture only pins an expected error.
	SQL []string
	// WantErr is the trimmed contents of the optional "error" section.
	// Empty means the fixture does not expect an error.
	WantErr string

	path    string
	comment []byte
}

// LoadDir loads every *.txtar fixture directly inside dir (non-recursive),
// sorted by file name for deterministic test/subtest ordering.
func LoadDir(dir string) ([]*Fixture, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("goldentest: read %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".txtar") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	fixtures := make([]*Fixture, 0, len(names))
	for _, name := range names {
		path := filepath.Join(dir, name)
		f, err := loadFixture(path)
		if err != nil {
			return nil, fmt.Errorf("goldentest: %s: %w", path, err)
		}
		fixtures = append(fixtures, f)
	}
	return fixtures, nil
}

func loadFixture(path string) (*Fixture, error) {
	ar, err := txtar.ParseFile(path)
	if err != nil {
		return nil, err
	}
	f := &Fixture{
		Name:    strings.TrimSuffix(filepath.Base(path), ".txtar"),
		Meta:    map[string]string{},
		path:    path,
		comment: ar.Comment,
	}
	sqlSections := map[int]string{}
	for _, file := range ar.Files {
		content := strings.TrimRight(string(file.Data), "\n")
		switch {
		case file.Name == sectionQuery:
			f.Query = content
		case file.Name == sectionMeta:
			f.Meta = parseMeta(content)
		case file.Name == sectionError:
			f.WantErr = content
		case strings.HasPrefix(file.Name, sqlPrefix):
			idxStr := strings.TrimPrefix(file.Name, sqlPrefix)
			idx, convErr := strconv.Atoi(idxStr)
			if convErr != nil {
				return nil, fmt.Errorf("section %q: expected sql-N, got non-numeric suffix %q", file.Name, idxStr)
			}
			sqlSections[idx] = content
		default:
			return nil, fmt.Errorf("unrecognized section %q (want one of: query, meta, error, sql-N)", file.Name)
		}
	}
	if f.Query == "" {
		return nil, fmt.Errorf("missing required %q section", sectionQuery)
	}
	if len(sqlSections) == 0 && f.WantErr == "" {
		return nil, fmt.Errorf("fixture has neither a sql-N section nor an %q section", sectionError)
	}
	f.SQL = make([]string, len(sqlSections))
	for idx, content := range sqlSections {
		if idx < 0 || idx >= len(sqlSections) {
			return nil, fmt.Errorf("sql-N sections must be zero-indexed and contiguous; got sql-%d with %d total", idx, len(sqlSections))
		}
		f.SQL[idx] = content
	}
	return f, nil
}

func parseMeta(content string) map[string]string {
	meta := map[string]string{}
	if content == "" {
		return meta
	}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		meta[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return meta
}

// TestingT is the subset of *testing.T (or *testing.B) that Check methods
// need. It exists only so callers are not forced to depend on *testing.T
// specifically.
type TestingT interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// CheckSQL compares got against the fixture's golden "sql-N" sections and
// reports a diff via t.Errorf on any mismatch (statement count or content).
// In -update mode it instead overwrites the fixture's golden sections with
// got and rewrites the .txtar file on disk.
//
// Call it from inside a t.Run(fixture.Name, ...) subtest so a failure names
// the fixture in `go test -run`.
func (f *Fixture) CheckSQL(t TestingT, got ...string) {
	t.Helper()
	if *Update {
		f.SQL = append([]string(nil), got...)
		if err := f.writeFile(); err != nil {
			t.Fatalf("%s: rewrite golden: %v", f.Name, err)
		}
		return
	}
	if len(got) != len(f.SQL) {
		t.Errorf("%s: got %d SQL statement(s), fixture pins %d:\n%s", f.Name, len(got), len(f.SQL), diffStatements(f.SQL, got))
		return
	}
	for i := range got {
		want := strings.TrimRight(f.SQL[i], "\n")
		have := strings.TrimRight(got[i], "\n")
		if want != have {
			t.Errorf("%s: sql-%d mismatch:\n%s", f.Name, i, diffLines(want, have))
		}
	}
}

// CheckError compares err against the fixture's golden "error" section: err
// must be non-nil and its Error() text must contain the golden substring. A
// fixture with no "error" section instead asserts err is nil. In -update
// mode it rewrites the "error" section to err's message (or clears it, if
// err is nil) and rewrites the .txtar file on disk.
func (f *Fixture) CheckError(t TestingT, err error) {
	t.Helper()
	if *Update {
		if err != nil {
			f.WantErr = err.Error()
		} else {
			f.WantErr = ""
		}
		if werr := f.writeFile(); werr != nil {
			t.Fatalf("%s: rewrite golden: %v", f.Name, werr)
		}
		return
	}
	if f.WantErr == "" {
		if err != nil {
			t.Errorf("%s: unexpected error: %v", f.Name, err)
		}
		return
	}
	if err == nil {
		t.Errorf("%s: expected an error containing %q, got nil", f.Name, f.WantErr)
		return
	}
	if !strings.Contains(err.Error(), f.WantErr) {
		t.Errorf("%s: error %q does not contain expected substring %q", f.Name, err.Error(), f.WantErr)
	}
}

// writeFile rebuilds the fixture's .txtar file from its current in-memory
// state, in a fixed canonical section order (meta, query, sql-N..., error),
// with meta keys sorted alphabetically. Rebuilding from scratch on every
// write — rather than patching the previously parsed archive in place — is
// what makes `just update-golden <head>` deterministic: two consecutive
// regenerations with no code change always produce byte-identical files.
func (f *Fixture) writeFile() error {
	ar := &txtar.Archive{Comment: f.comment}
	if len(f.Meta) > 0 {
		ar.Files = append(ar.Files, txtar.File{Name: sectionMeta, Data: []byte(formatMeta(f.Meta))})
	}
	ar.Files = append(ar.Files, txtar.File{Name: sectionQuery, Data: []byte(f.Query + "\n")})
	for i, s := range f.SQL {
		ar.Files = append(ar.Files, txtar.File{Name: fmt.Sprintf("%s%d", sqlPrefix, i), Data: []byte(s + "\n")})
	}
	if f.WantErr != "" {
		ar.Files = append(ar.Files, txtar.File{Name: sectionError, Data: []byte(f.WantErr + "\n")})
	}
	return os.WriteFile(f.path, txtar.Format(ar), 0o644)
}

func formatMeta(meta map[string]string) string {
	keys := make([]string, 0, len(meta))
	for k := range meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s: %s\n", k, meta[k])
	}
	return b.String()
}

// diffLines renders a line-by-line diff of two single golden statements,
// naming the first mismatching line rather than dumping both in full.
func diffLines(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	n := len(wantLines)
	if len(gotLines) > n {
		n = len(gotLines)
	}
	var b strings.Builder
	for i := 0; i < n; i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w == g {
			continue
		}
		fmt.Fprintf(&b, "  line %d:\n    - want: %s\n    + got:  %s\n", i+1, w, g)
	}
	return b.String()
}

// diffStatements renders both statement lists when their counts differ, so a
// missing/extra statement is obvious without hunting through per-line diffs.
func diffStatements(want, got []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  want (%d):\n", len(want))
	for i, s := range want {
		fmt.Fprintf(&b, "    [%d] %s\n", i, s)
	}
	fmt.Fprintf(&b, "  got (%d):\n", len(got))
	for i, s := range got {
		fmt.Fprintf(&b, "    [%d] %s\n", i, s)
	}
	return b.String()
}
