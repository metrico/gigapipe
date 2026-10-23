package nestedset

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// recorder is an inserter that records each statement and reports rows[i]
// inserted rows for the i-th.
type recorder struct {
	rows []uint64
	err  error
	got  []string
}

func (r *recorder) insert(_ context.Context, q string) (uint64, error) {
	i := len(r.got)
	r.got = append(r.got, q)
	if r.err != nil {
		return 0, r.err
	}
	if i < len(r.rows) {
		return r.rows[i], nil
	}
	return 0, nil
}

var fakeStatements = statements{
	batch:  func(id string) string { return "batch " + id },
	number: func(id string) string { return "number " + id },
}

func TestAPassWithAnEmptyBatchSkipsTheNumbering(t *testing.T) {
	r := &recorder{rows: []uint64{0}}
	if err := run(context.Background(), r.insert, fakeStatements); err != nil {
		t.Fatal(err)
	}
	if len(r.got) != 1 || !strings.HasPrefix(r.got[0], "batch ") {
		t.Errorf("ran %q, want only the batch statement", r.got)
	}
}

func TestAPassNumbersTheBatchItWrote(t *testing.T) {
	r := &recorder{rows: []uint64{3, 40}}
	if err := run(context.Background(), r.insert, fakeStatements); err != nil {
		t.Fatal(err)
	}
	var batch, number string
	if len(r.got) != 2 {
		t.Fatalf("ran %q, want the batch then the numbering", r.got)
	}
	if n, err := fmt.Sscanf(r.got[0], "batch %s", &batch); n != 1 || err != nil {
		t.Fatalf("batch statement %q: %v", r.got[0], err)
	}
	if n, err := fmt.Sscanf(r.got[1], "number %s", &number); n != 1 || err != nil {
		t.Fatalf("numbering statement %q: %v", r.got[1], err)
	}
	if number != batch {
		t.Errorf("ran %q, want both statements with one pass id", r.got)
	}
}

func TestEachPassHasItsOwnID(t *testing.T) {
	r := &recorder{rows: []uint64{0, 0}}
	for range 2 {
		if err := run(context.Background(), r.insert, fakeStatements); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.got) != 2 || r.got[0] == r.got[1] {
		t.Errorf("ran %q, want two batches with different pass ids", r.got)
	}
}

func TestAFailedBatchSkipsTheNumbering(t *testing.T) {
	r := &recorder{err: errors.New("boom")}
	if err := run(context.Background(), r.insert, fakeStatements); err == nil || len(r.got) != 1 {
		t.Errorf("err %v after %q, want the batch error and no numbering", err, r.got)
	}
}

func bothStatements() map[string]statements {
	return map[string]statements{
		"local":   localStatements(time.Second, 10, "0"),
		"cluster": clusterStatements("c", "db", time.Second, 10),
	}
}

func TestTheBatchReadsTheNumberingVersionsNotTheNestedSetHistory(t *testing.T) {
	for name, st := range bothStatements() {
		q := st.batch("p1")
		if !strings.Contains(q, "tempo_traces_numbered") || regexp.MustCompile(`tempo_traces_nested_set\b`).MatchString(q) {
			t.Errorf("%s batch reads the nested-set history or no numbering versions:\n%s", name, q)
		}
		if !strings.Contains(q, "toUUID('p1') AS pass") {
			t.Errorf("%s batch does not write its pass id:\n%s", name, q)
		}
	}
}

func TestTheNumberingReadsOnlyItsBatch(t *testing.T) {
	for name, st := range bothStatements() {
		q := st.number("p1")
		if !strings.Contains(q, "tempo_traces_nested_set_batch WHERE pass = toUUID('p1')") || strings.Contains(q, "tempo_traces_touched") {
			t.Errorf("%s numbering does not read only batch p1:\n%s", name, q)
		}
	}
}

func TestPassStatementsDoNotReadTenants(t *testing.T) {
	for name, st := range bothStatements() {
		if q := st.batch("p1") + st.number("p1"); regexp.MustCompile(`\boid\b`).MatchString(q) {
			t.Errorf("%s pass reads oid:\n%s", name, q)
		}
	}
}

func TestClusterStatementsRunOnEachShardsFirstReplica(t *testing.T) {
	st := clusterStatements("c", "db", time.Second, 10)
	for target, q := range map[string]string{
		"`db`.tempo_traces_nested_set_batch_dist": st.batch("p1"),
		"`db`.tempo_traces_nested_set_dist":       st.number("p1"),
	} {
		for _, want := range []string{
			"INSERT INTO " + target + " ", "FROM cluster('c', view(",
			"parallel_distributed_insert_select = 2", "load_balancing = 'in_order'", "prefer_localhost_replica = 0",
		} {
			if !strings.Contains(q, want) {
				t.Errorf("statement into %s lacks %q:\n%s", target, want, q)
			}
		}
	}
}
