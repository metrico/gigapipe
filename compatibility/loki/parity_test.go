//go:build parity

package loki

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
)

// TestLogQLParity runs every query in queries.txt against Loki and gigapipe.
// A query answered differently is a failing subtest named
// TestLogQLParity/gigapipe/<query>; the data is written in
// TestLogQLParity/gigapipe/_setup. compatibility/ratchet compares the results
// with baseline.json.
//
// Run it with run.sh, which starts ClickHouse, gigapipe built from this tree
// and a pinned Loki. Against servers that are already running:
//
//	GIGAPIPE_URL=http://localhost:3100 LOKI_URL=http://localhost:3101 \
//		go test -tags parity -v -run '^TestLogQLParity$' ./compatibility/loki/
func TestLogQLParity(t *testing.T) {
	gigapipeURL, lokiURL := os.Getenv("GIGAPIPE_URL"), os.Getenv("LOKI_URL")
	if gigapipeURL == "" || lokiURL == "" {
		t.Skip("GIGAPIPE_URL and LOKI_URL must be set; run.sh sets both")
	}
	f, err := os.Open("queries.txt")
	if err != nil {
		t.Fatal(err)
	}
	queries, err := parseQueries(f)
	f.Close()
	if err != nil {
		t.Fatalf("queries.txt: %v", err)
	}

	run := fmt.Sprintf("logql-%d", time.Now().UnixNano())
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	fixture := seed.Default(run, start)
	streams := fixture.Logs()
	lines := 0
	for _, s := range streams {
		lines += len(s.Lines)
	}
	w := window{
		start: start,
		from:  start.Add(-time.Minute),
		to:    start.Add(3 * time.Minute),
		at:    start.Add(2 * time.Minute),
	}
	reference := server{name: "loki", base: lokiURL}
	target := server{name: "gigapipe", base: gigapipeURL}

	t.Run("gigapipe", func(t *testing.T) {
		ok := t.Run("_setup", func(t *testing.T) {
			body := seed.LokiPushJSON(streams)
			for _, s := range []server{reference, target} {
				if err := s.push(body); err != nil {
					t.Fatal(err)
				}
			}
			// Both store asynchronously: wait until each returns every line.
			deadline := time.Now().Add(60 * time.Second)
			for _, s := range []server{reference, target} {
				for {
					got, err := s.countLines(run, w)
					if err == nil && got == lines {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("%s returns %d of the %d lines written (last error: %v)", s.name, got, lines, err)
					}
					time.Sleep(500 * time.Millisecond)
				}
			}
		})
		if !ok {
			t.FailNow()
		}
		// PARITY_ANSWERS_DIR (run.sh: out/answers) keeps both full answers of
		// every query that differs, for triage; the failure message shortens them.
		answersDir := os.Getenv("PARITY_ANSWERS_DIR")
		for i, q := range queries {
			t.Run(q.text, func(t *testing.T) {
				want, lokiErr, err := reference.ask(q, run, w)
				if err != nil {
					t.Fatalf("loki: %v", err)
				}
				got, gigapipeErr, err := target.ask(q, run, w)
				if err != nil {
					t.Fatalf("gigapipe: %v", err)
				}
				if g, l := canonical(got), canonical(want); g != l {
					if answersDir != "" {
						saveAnswers(t, answersDir, i, q, g+errorNote(gigapipeErr), l+errorNote(lokiErr))
					}
					t.Errorf("answers differ (%s)\ngigapipe:\n%s\nloki:\n%s", q.section, shorten(g+errorNote(gigapipeErr)), shorten(l+errorNote(lokiErr)))
				}
			})
		}
	})
}

// errorNote shows an error answer's body under its status; it isn't compared.
func errorNote(body string) string {
	if body == "" {
		return ""
	}
	return "\nerror body: " + shorten(body)
}

// saveAnswers writes both full answers of query number i to dir.
func saveAnswers(t *testing.T, dir string, i int, q query, gigapipe, loki string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Errorf("save answers: %v", err)
		return
	}
	text := fmt.Sprintf("query: %s\nsection: %s\n\ngigapipe:\n%s\n\nloki:\n%s\n", q.text, q.section, gigapipe, loki)
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%03d.txt", i+1)), []byte(text), 0o644); err != nil {
		t.Errorf("save answers: %v", err)
	}
}
