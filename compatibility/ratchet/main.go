// Command ratchet compares a parity run with the committed baseline.
//
// It reads the `go test -json` output of a parity test (TestPromQLParity by
// default; -test names another) and the baseline file, which records every
// query's state (agree or differ) and, for queries that differ, why. A parity
// test names each query sent to gigapipe as the subtest <test>/gigapipe/<query>
// and loads its data in <test>/gigapipe/_setup. Usage:
//
//	ratchet report   -results out/parity.json
//	ratchet check    -results out/parity.json -baseline baseline.json
//	ratchet baseline -results out/parity.json -baseline baseline.json
//
// report prints the counts. check fails when any query's state changed since
// the baseline: a query that agreed now differs, a query that differed now
// agrees, a baselined query no longer ran, or a new query isn't in the
// baseline yet. baseline rewrites the baseline from the run, keeping the
// reasons already recorded and marking new differences "unclassified".
//
// Exit codes: 0 ok, 1 check found changes, 2 bad usage or the suite didn't run.
package main

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// DefaultTest is the parity test read when -test isn't given.
const DefaultTest = "TestPromQLParity"

// setupCase is the suite's data-loading step, not a query.
const setupCase = "_setup"

// Unclassified is the reason given to a difference nobody has looked at yet.
const Unclassified = "unclassified"

const (
	Agree  = "agree"
	Differ = "differ"
)

// Case is one query's entry in the baseline.
type Case struct {
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

// Baseline maps a query's subtest name (without <test>/gigapipe/) to its entry.
type Baseline struct {
	Cases map[string]Case `json:"cases"`
}

// errNoRun means the results don't contain a usable run of the suite.
var errNoRun = errors.New("the suite did not run")

// testEvent is the part of a `go test -json` event used here.
type testEvent struct {
	Action string
	Test   string
}

// ReadResults returns the state of every query of the parity test named test
// in a `go test -json` stream. A failed or missing _setup step, or a run
// without queries, is errNoRun.
func ReadResults(r io.Reader, test string) (map[string]string, error) {
	casePrefix := test + "/gigapipe/"
	res := map[string]string{}
	setup := ""
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var ev testEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("read results: %w", err)
		}
		name, ok := strings.CutPrefix(ev.Test, casePrefix)
		if !ok || (ev.Action != "pass" && ev.Action != "fail") {
			continue
		}
		if name == setupCase {
			setup = ev.Action
			continue
		}
		if ev.Action == "pass" {
			res[name] = Agree
		} else {
			res[name] = Differ
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read results: %w", err)
	}
	if setup != "pass" {
		return nil, fmt.Errorf("%w: _setup %s", errNoRun, cmp.Or(setup, "missing"))
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("%w: no queries", errNoRun)
	}
	return res, nil
}

// Change is one query whose state differs from the baseline.
type Change struct {
	Name string
	Kind string
}

// Kinds of change. #922 asks for the first four; NowAgrees makes a fixed
// query get recorded too, and the last two catch a hand-edited baseline that
// is wrong.
const (
	NowDiffers  = "agreed in the baseline, now differs"
	NotRun      = "in the baseline, did not run"
	NewDiffers  = "new query, differs"
	NewAgrees   = "new query, not in the baseline yet"
	NowAgrees   = "differed in the baseline, now agrees: update the baseline"
	NoReason    = "differs in the baseline without a reason"
	BadBaseline = "unknown state in the baseline"
)

// Compare lists every query whose state changed since the baseline, sorted
// by name.
func Compare(b Baseline, res map[string]string) []Change {
	var out []Change
	for name, c := range b.Cases {
		got, ran := res[name]
		switch {
		case c.State != Agree && c.State != Differ:
			out = append(out, Change{name, BadBaseline})
		case c.State == Differ && c.Reason == "":
			out = append(out, Change{name, NoReason})
		case !ran:
			out = append(out, Change{name, NotRun})
		case c.State == Agree && got == Differ:
			out = append(out, Change{name, NowDiffers})
		case c.State == Differ && got == Agree:
			out = append(out, Change{name, NowAgrees})
		}
	}
	for name, got := range res {
		if _, ok := b.Cases[name]; ok {
			continue
		}
		if got == Differ {
			out = append(out, Change{name, NewDiffers})
		} else {
			out = append(out, Change{name, NewAgrees})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Update returns the baseline for this run. Reasons recorded for queries that
// still differ are kept; new differences get Unclassified.
func Update(old Baseline, res map[string]string) Baseline {
	nb := Baseline{Cases: make(map[string]Case, len(res))}
	for name, got := range res {
		c := Case{State: got}
		if got == Differ {
			c.Reason = Unclassified
			if prev, ok := old.Cases[name]; ok && prev.State == Differ && prev.Reason != "" {
				c.Reason = prev.Reason
			}
		}
		nb.Cases[name] = c
	}
	return nb
}

// Counts returns how many queries agree and differ.
func Counts(res map[string]string) (agree, differ int) {
	for _, s := range res {
		if s == Agree {
			agree++
		} else {
			differ++
		}
	}
	return agree, differ
}

// Reasons returns how many baselined differences carry each reason.
func Reasons(b Baseline) map[string]int {
	out := map[string]int{}
	for _, c := range b.Cases {
		if c.State == Differ {
			out[c.Reason]++
		}
	}
	return out
}

func readBaseline(path string) (Baseline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Baseline{}, err
	}
	var b Baseline
	if err := json.Unmarshal(data, &b); err != nil {
		return Baseline{}, fmt.Errorf("%s: %w", path, err)
	}
	if b.Cases == nil {
		b.Cases = map[string]Case{}
	}
	return b, nil
}

// writeBaseline writes one query per line, sorted, so a diff of the file
// shows exactly which queries changed.
func writeBaseline(path string, b Baseline) error {
	names := make([]string, 0, len(b.Cases))
	for name := range b.Cases {
		names = append(names, name)
	}
	sort.Strings(names)
	// Queries contain < > and &; keep them readable instead of \u003c etc.
	marshal := func(v any) []byte {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(v)
		return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	}
	var sb bytes.Buffer
	sb.WriteString("{\n  \"cases\": {\n")
	for i, name := range names {
		sb.WriteString("    ")
		sb.Write(marshal(name))
		sb.WriteString(": ")
		sb.Write(marshal(b.Cases[name]))
		if i < len(names)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("  }\n}\n")
	return os.WriteFile(path, sb.Bytes(), 0o644)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	// printf writes to stdout or stderr. A failed write there can't be
	// reported anywhere, so its error is ignored.
	printf := func(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	if len(args) == 0 {
		printf(stderr, "usage: ratchet report|check|baseline -results FILE [-baseline FILE]\n")
		return 2
	}
	mode := args[0]
	if mode != "report" && mode != "check" && mode != "baseline" {
		printf(stderr, "unknown mode %q\n", mode)
		return 2
	}
	fs := flag.NewFlagSet(mode, flag.ContinueOnError)
	fs.SetOutput(stderr)
	resultsPath := fs.String("results", "out/parity.json", "go test -json output of the parity test")
	baselinePath := fs.String("baseline", "baseline.json", "baseline file")
	test := fs.String("test", DefaultTest, "name of the parity test whose results are read")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}

	f, err := os.Open(*resultsPath)
	if err != nil {
		printf(stderr, "%v\n", err)
		return 2
	}
	res, err := ReadResults(f, *test)
	_ = f.Close() // read only: nothing to flush
	if err != nil {
		printf(stderr, "%v\n", err)
		return 2
	}
	agree, differ := Counts(res)
	printf(stdout, "agree: %d\ndiffer: %d\n", agree, differ)

	switch mode {
	case "report":
		return 0
	case "check":
		b, err := readBaseline(*baselinePath)
		if err != nil {
			printf(stderr, "%v\n", err)
			return 2
		}
		changes := Compare(b, res)
		if len(changes) == 0 {
			printf(stdout, "matches the baseline (%d queries)\n", len(res))
			return 0
		}
		printf(stdout, "%d queries changed since the baseline:\n", len(changes))
		for _, c := range changes {
			printf(stdout, "  %s: %s\n", c.Name, c.Kind)
		}
		return 1
	case "baseline":
		old, err := readBaseline(*baselinePath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			printf(stderr, "%v\n", err)
			return 2
		}
		nb := Update(old, res)
		if err := writeBaseline(*baselinePath, nb); err != nil {
			printf(stderr, "%v\n", err)
			return 2
		}
		printf(stdout, "wrote %s\n", *baselinePath)
		reasons := Reasons(nb)
		keys := make([]string, 0, len(reasons))
		for r := range reasons {
			keys = append(keys, r)
		}
		sort.Strings(keys)
		for _, r := range keys {
			printf(stdout, "  %4d %s\n", reasons[r], r)
		}
	}
	return 0
}
