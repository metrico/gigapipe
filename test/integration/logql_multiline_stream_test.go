//go:build integration

// Regression test for issue #964: on a single node, a LogQL query that matches
// a stream with several log lines must return all of them, not one.
//
// The label-resolution join in LabelsJoinPlanner joins the samples (one row per
// log line) to the time_series labels (one row per fingerprint). An ANY INNER
// JOIN keeps at most one matched pair per key and collapses every multi-line
// stream to a single line; the fix keeps ANY LEFT JOIN and drops unresolved
// rows with a length(labels) > 0 filter instead.
//
// This drives the real push and query HTTP paths, so it fails on the collapse
// and passes on the fix without asserting any SQL. Run via test/integration/run.sh
// or against a running stack:
//
//	GIGAPIPE_URL=http://localhost:3100 go test -tags integration ./test/integration/... -v

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// pushStreams sends log lines to gigapipe using the Loki push v1 JSON format.
// Each element of streams maps a label set to its lines.
func pushStreams(t *testing.T, streams []lokiPushStream) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"streams": streams})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL()+"/loki/api/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("push status = %d, body = %q", resp.StatusCode, raw)
	}
}

type lokiPushStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"` // [ [ "<ns ts>", "<line>" ], ... ]
}

// queryStreamLineCounts runs an instant query and returns line counts keyed by
// the value of the given label, so the assertion does not depend on label
// ordering or fingerprint internals.
func queryStreamLineCounts(t *testing.T, query, labelKey string) map[string]int {
	t.Helper()
	u := fmt.Sprintf("%s/loki/api/v1/query?query=%s", baseURL(), urlQueryEscape(query))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status = %d, body = %q", resp.StatusCode, raw)
	}

	var out struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode query response: %v\nbody: %s", err, raw)
	}
	if out.Data.ResultType != "streams" {
		t.Fatalf("resultType = %q, want streams\nbody: %s", out.Data.ResultType, raw)
	}

	counts := map[string]int{}
	for _, s := range out.Data.Result {
		counts[s.Stream[labelKey]] += len(s.Values)
	}
	return counts
}

// TestLogQLReturnsAllLinesOfAMultiLineStream reproduces issue #964: a stream
// with three lines must come back with three lines, not one.
func TestLogQLReturnsAllLinesOfAMultiLineStream(t *testing.T) {
	waitReady(t)

	// Unique label value per run so repeated runs against a shared ClickHouse
	// don't read each other's data.
	nodeA := "itest_node_a_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	nodeB := "itest_node_b_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	nowNs := time.Now().UnixNano()

	ns := func(offset int64) string { return strconv.FormatInt(nowNs+offset, 10) }

	// nodeA is one stream with three lines; nodeB is one stream with one line.
	pushStreams(t, []lokiPushStream{
		{
			Stream: map[string]string{"itest_stream": nodeA},
			Values: [][]string{
				{ns(0), "line-a-1"},
				{ns(1), "line-a-2"},
				{ns(2), "line-a-3"},
			},
		},
		{
			Stream: map[string]string{"itest_stream": nodeB},
			Values: [][]string{
				{ns(3), "line-b-1"},
			},
		},
	})

	query := fmt.Sprintf(`{itest_stream=~"%s|%s"}`, nodeA, nodeB)

	// Ingestion is asynchronous (bulk flush); poll until both streams are
	// visible, then assert the per-stream line counts.
	deadline := time.Now().Add(30 * time.Second)
	var counts map[string]int
	for {
		counts = queryStreamLineCounts(t, query, "itest_stream")
		if counts[nodeA] >= 1 && counts[nodeB] >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("streams never became queryable; got counts %v", counts)
		}
		time.Sleep(time.Second)
	}

	if got := counts[nodeA]; got != 3 {
		t.Errorf("multi-line stream returned %d lines, want 3 (issue #964: labels join collapses multi-line streams)", got)
	}
	if got := counts[nodeB]; got != 1 {
		t.Errorf("single-line stream returned %d lines, want 1", got)
	}
}

func urlQueryEscape(s string) string {
	// Minimal escaping for the LogQL selector characters we use; net/url would
	// also work but keeps the dependency surface identical to the sibling test.
	repl := map[rune]string{
		'{': "%7B", '}': "%7D", '"': "%22", '=': "%3D",
		'~': "%7E", '|': "%7C", ' ': "%20",
	}
	var b bytes.Buffer
	for _, r := range s {
		if v, ok := repl[r]; ok {
			b.WriteString(v)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
