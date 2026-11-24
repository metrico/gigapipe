package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// events builds a `go test -json` stream of TestPromQLParity with one
// pass/fail event per query.
func events(setup string, cases map[string]string) string {
	return testEvents("TestPromQLParity", setup, cases)
}

// testEvents is events for the parity test named test.
func testEvents(test, setup string, cases map[string]string) string {
	var sb strings.Builder
	sb.WriteString(`{"Action":"start","Package":"p"}` + "\n")
	sb.WriteString(`{"Action":"run","Test":"` + test + `/gigapipe/_setup"}` + "\n")
	if setup != "" {
		sb.WriteString(`{"Action":"` + setup + `","Test":"` + test + `/gigapipe/_setup"}` + "\n")
	}
	for name, state := range cases {
		action := "pass"
		if state == Differ {
			action = "fail"
		}
		sb.WriteString(`{"Action":"output","Test":"` + test + `/gigapipe/` + name + `","Output":"--- x\n"}` + "\n")
		sb.WriteString(`{"Action":"` + action + `","Test":"` + test + `/gigapipe/` + name + `"}` + "\n")
	}
	// The reference's own subtests and the parent test are not queries.
	sb.WriteString(`{"Action":"pass","Test":"` + test + `/prometheus/_setup"}` + "\n")
	sb.WriteString(`{"Action":"fail","Test":"` + test + `"}` + "\n")
	return sb.String()
}

func TestReadResults(t *testing.T) {
	want := map[string]string{"rate(a[5m])": Agree, "a_offset_-1m": Differ}
	got, err := ReadResults(strings.NewReader("not json\n"+events("pass", want)), DefaultTest)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReadResultsNoRun(t *testing.T) {
	for name, stream := range map[string]string{
		"setup failed":  events("fail", map[string]string{"a": Agree}),
		"setup missing": events("", map[string]string{"a": Agree}),
		"no queries":    events("pass", nil),
		"empty":         "",
	} {
		if _, err := ReadResults(strings.NewReader(stream), DefaultTest); !errors.Is(err, errNoRun) {
			t.Errorf("%s: err = %v, want errNoRun", name, err)
		}
	}
}

func TestCompare(t *testing.T) {
	b := Baseline{Cases: map[string]Case{
		"same-agree":   {State: Agree},
		"same-differ":  {State: Differ, Reason: "r"},
		"now-differs":  {State: Agree},
		"now-agrees":   {State: Differ, Reason: "r"},
		"gone":         {State: Agree},
		"no-reason":    {State: Differ},
		"bad-state":    {State: "maybe"},
		"still-differ": {State: Differ, Reason: Unclassified},
	}}
	res := map[string]string{
		"same-agree":   Agree,
		"same-differ":  Differ,
		"now-differs":  Differ,
		"now-agrees":   Agree,
		"no-reason":    Differ,
		"bad-state":    Agree,
		"still-differ": Differ,
		"new-differs":  Differ,
		"new-agrees":   Agree,
	}
	want := []Change{
		{"bad-state", BadBaseline},
		{"gone", NotRun},
		{"new-agrees", NewAgrees},
		{"new-differs", NewDiffers},
		{"no-reason", NoReason},
		{"now-agrees", NowAgrees},
		{"now-differs", NowDiffers},
	}
	if got := Compare(b, res); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestUpdateKeepsReasons(t *testing.T) {
	old := Baseline{Cases: map[string]Case{
		"kept":  {State: Differ, Reason: "known"},
		"fixed": {State: Differ, Reason: "known"},
		"gone":  {State: Differ, Reason: "known"},
	}}
	res := map[string]string{"kept": Differ, "fixed": Agree, "new": Differ, "ok": Agree}
	want := Baseline{Cases: map[string]Case{
		"kept":  {State: Differ, Reason: "known"},
		"fixed": {State: Agree},
		"new":   {State: Differ, Reason: Unclassified},
		"ok":    {State: Agree},
	}}
	got := Update(old, res)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
	if c := Compare(got, res); len(c) != 0 {
		t.Fatalf("an updated baseline must match its own run, got %v", c)
	}
}

// TestFlippedQuery is #922's acceptance check: a run where one query flips
// from agree to differ fails the check, and the same run passes once that
// query is baselined with a reason.
func TestFlippedQuery(t *testing.T) {
	dir := t.TempDir()
	baseline := filepath.Join(dir, "baseline.json")
	before := filepath.Join(dir, "before.json")
	after := filepath.Join(dir, "after.json")
	write := func(path, data string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(before, events("pass", map[string]string{"q1": Agree, "q2": Agree, "q3": Differ}))
	write(after, events("pass", map[string]string{"q1": Agree, "q2": Differ, "q3": Differ}))

	mustRun := func(wantCode int, args ...string) string {
		t.Helper()
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != wantCode {
			t.Fatalf("ratchet %v: exit %d, want %d\n%s%s", args, code, wantCode, out.String(), errOut.String())
		}
		return out.String()
	}

	mustRun(0, "baseline", "-results", before, "-baseline", baseline)
	mustRun(0, "check", "-results", before, "-baseline", baseline)

	out := mustRun(1, "check", "-results", after, "-baseline", baseline)
	if !strings.Contains(out, "q2: "+NowDiffers) {
		t.Fatalf("check output doesn't name q2:\n%s", out)
	}

	// Record q2 as a known difference with a reason, as a person would after
	// triage, and the same run passes.
	b, err := readBaseline(baseline)
	if err != nil {
		t.Fatal(err)
	}
	b.Cases["q2"] = Case{State: Differ, Reason: "https://github.com/metrico/gigapipe/issues/1"}
	if err := writeBaseline(baseline, b); err != nil {
		t.Fatal(err)
	}
	mustRun(0, "check", "-results", after, "-baseline", baseline)

	// Regenerating keeps that reason.
	mustRun(0, "baseline", "-results", after, "-baseline", baseline)
	b, err = readBaseline(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Cases["q2"].Reason; got != "https://github.com/metrico/gigapipe/issues/1" {
		t.Fatalf("q2 reason = %q after regenerating", got)
	}
}

func TestRunSuiteDidNotRun(t *testing.T) {
	dir := t.TempDir()
	results := filepath.Join(dir, "r.json")
	if err := os.WriteFile(results, []byte(events("fail", nil)), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"report", "-results", results}, &out, &errOut); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
}

func TestRunBadUsage(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"verify", "-results", "does-not-matter.json"},
		{"check", "-results", filepath.Join(t.TempDir(), "missing.json")},
	} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 2 {
			t.Errorf("ratchet %v: exit %d, want 2", args, code)
		}
	}
}

// TestOtherParityTest reads a stream holding two parity tests: each name
// sees only its own queries, through ReadResults and through -test.
func TestOtherParityTest(t *testing.T) {
	stream := events("pass", map[string]string{"prom-q": Agree}) +
		testEvents("TestLogQLParity", "pass", map[string]string{"logql-q": Differ})
	for test, want := range map[string]map[string]string{
		DefaultTest:       {"prom-q": Agree},
		"TestLogQLParity": {"logql-q": Differ},
	} {
		got, err := ReadResults(strings.NewReader(stream), test)
		if err != nil {
			t.Fatalf("%s: %v", test, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", test, got, want)
		}
	}

	results := filepath.Join(t.TempDir(), "r.json")
	if err := os.WriteFile(results, []byte(stream), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"report", "-test", "TestLogQLParity", "-results", results}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if got := out.String(); got != "agree: 0\ndiffer: 1\n" {
		t.Errorf("report output %q", got)
	}
}
