package loki

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestQueriesFile checks the committed query list, so a malformed line fails
// here and not only in a parity run.
func TestQueriesFile(t *testing.T) {
	f, err := os.Open("queries.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	queries, err := parseQueries(f)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, q := range queries {
		kinds[q.kind]++
		if q.kind == "instant" && strings.HasPrefix(q.text, "{") {
			t.Errorf("instant log query %q: Loki only answers metric queries as instant queries", q.text)
		}
	}
	if kinds["range"] == 0 || kinds["instant"] == 0 {
		t.Errorf("want both range and instant queries, got %v", kinds)
	}
}

func TestParseQueriesRejects(t *testing.T) {
	for name, text := range map[string]string{
		"no section":    "range {m2_run=\"$RUN\"}\n",
		"bad kind":      "## s\nrandom {m2_run=\"$RUN\"}\n",
		"no query":      "## s\nrange\n",
		"no $RUN":       "## s\nrange {app=\"checkout\"}\n",
		"slash":         "## s\nrange {m2_run=\"$RUN\"} |~ \"a/b\"\n",
		"duplicate":     "## s\nrange {m2_run=\"$RUN\"}\nrange {m2_run=\"$RUN\"}\n",
		"empty":         "# only a comment\n",
		"space variant": "## s\nrange {m2_run=\"$RUN\"} |= \"a b\"\nrange {m2_run=\"$RUN\"} |= \"a_b\"\n",
	} {
		if _, err := parseQueries(strings.NewReader(text)); err == nil {
			t.Errorf("%s: accepted %q", name, text)
		}
	}
}

func TestParseQueries(t *testing.T) {
	text := "# comment\n\n## page > First\nrange {m2_run=\"$RUN\"}\n## page > Second\ninstant count_over_time({m2_run=\"$RUN\"}[5m])\n"
	got, err := parseQueries(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	want := []query{
		{section: "page > First", kind: "range", text: `{m2_run="$RUN"}`},
		{section: "page > Second", kind: "instant", text: `count_over_time({m2_run="$RUN"}[5m])`},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("query %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

var start = time.Unix(1_800_000_000, 0)

func TestNormalizeStreams(t *testing.T) {
	// Streams in different orders, run label value, nanosecond timestamps.
	a := `{"status":"success","data":{"resultType":"streams","result":[
		{"stream":{"m2_run":"r1","pod":"p2"},"values":[["1800000010000000000","b"]]},
		{"stream":{"m2_run":"r1","pod":"p1"},"values":[["1800000000000000000","a"]]}]}}`
	b := `{"status":"success","data":{"resultType":"streams","result":[
		{"stream":{"pod":"p1","m2_run":"r2"},"values":[["1800000000000000000","a"]]},
		{"stream":{"pod":"p2","m2_run":"r2"},"values":[["1800000010000000000","b"]]}],"stats":{"x":1}}}`
	ga, err := normalizer{run: "r1", start: start}.response([]byte(a))
	if err != nil {
		t.Fatal(err)
	}
	gb, err := normalizer{run: "r2", start: start}.response([]byte(b))
	if err != nil {
		t.Fatal(err)
	}
	if canonical(ga) != canonical(gb) {
		t.Errorf("same answer normalized differently:\n%s\n%s", canonical(ga), canonical(gb))
	}
	if !strings.Contains(canonical(ga), `"+10s"`) || !strings.Contains(canonical(ga), `"RUN"`) {
		t.Errorf("offsets or run label not normalized:\n%s", canonical(ga))
	}
}

func TestNormalizeValues(t *testing.T) {
	// "4" and "4.0" are the same number; the sample time is in seconds.
	a := `{"data":{"resultType":"vector","result":[{"metric":{"app":"x"},"value":[1800000120,"4"]}]}}`
	b := `{"data":{"resultType":"vector","result":[{"metric":{"app":"x"},"value":[1800000120.000,"4.0"]}]}}`
	c := `{"data":{"resultType":"vector","result":[{"metric":{"app":"x"},"value":[1800000120,"5"]}]}}`
	n := normalizer{start: start}
	na, _ := n.response([]byte(a))
	nb, _ := n.response([]byte(b))
	nc, _ := n.response([]byte(c))
	if canonical(na) != canonical(nb) {
		t.Errorf("4 and 4.0 differ:\n%s\n%s", canonical(na), canonical(nb))
	}
	if canonical(na) == canonical(nc) {
		t.Errorf("4 and 5 compare equal")
	}
	if !strings.Contains(canonical(na), `"+2m0s"`) {
		t.Errorf("sample time not an offset:\n%s", canonical(na))
	}
}

func TestNormalizeOrder(t *testing.T) {
	desc := `{"data":{"resultType":"vector","result":[
		{"metric":{"pod":"p2"},"value":[1800000120,"7"]},
		{"metric":{"pod":"p1"},"value":[1800000120,"4"]}]}}`
	asc := `{"data":{"resultType":"vector","result":[
		{"metric":{"pod":"p1"},"value":[1800000120,"4"]},
		{"metric":{"pod":"p2"},"value":[1800000120,"7"]}]}}`
	sorted := normalizer{start: start}
	ordered := normalizer{start: start, keepOrder: true}
	d1, _ := sorted.response([]byte(desc))
	a1, _ := sorted.response([]byte(asc))
	if canonical(d1) != canonical(a1) {
		t.Error("result order matters without keepOrder")
	}
	d2, _ := ordered.response([]byte(desc))
	a2, _ := ordered.response([]byte(asc))
	if canonical(d2) == canonical(a2) {
		t.Error("result order ignored with keepOrder (sort, sort_desc)")
	}
}

func TestNormalizeMatrixAndErrors(t *testing.T) {
	m := `{"data":{"resultType":"matrix","result":[{"metric":{},"values":[[1800000030,"1"],[1800000060,"2"]]}]}}`
	got, err := normalizer{start: start}.response([]byte(m))
	if err != nil {
		t.Fatal(err)
	}
	if s := canonical(got); !strings.Contains(s, `"+30s"`) || !strings.Contains(s, `"+1m0s"`) {
		t.Errorf("matrix times not normalized:\n%s", s)
	}
	s, err := normalizer{start: start}.response([]byte(`{"data":{"resultType":"scalar","result":[1800000120,"0"]}}`))
	if err != nil || canonical(s) != canonical(map[string]any{"resultType": "scalar", "result": [2]string{"+2m0s", "0"}}) {
		t.Errorf("scalar: %v %s", err, canonical(s))
	}
	for _, bad := range []string{`not json`, `{"data":{"resultType":"other","result":[]}}`} {
		if _, err := (normalizer{start: start}).response([]byte(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

// TestNormalizeTies: lines sharing a timestamp may come in any order, but the
// order of different timestamps is still compared.
func TestNormalizeTies(t *testing.T) {
	stream := func(values string) string {
		return `{"data":{"resultType":"streams","result":[{"stream":{"app":"x"},"values":[` + values + `]}]}}`
	}
	n := normalizer{start: start}
	a, _ := n.response([]byte(stream(`["1800000010000000000","b"],["1800000010000000000","a"],["1800000000000000000","c"]`)))
	b, _ := n.response([]byte(stream(`["1800000010000000000","a"],["1800000010000000000","b"],["1800000000000000000","c"]`)))
	if canonical(a) != canonical(b) {
		t.Errorf("order of lines with the same timestamp compared:\n%s\n%s", canonical(a), canonical(b))
	}
	backward, _ := n.response([]byte(stream(`["1800000010000000000","a"],["1800000000000000000","c"]`)))
	forward, _ := n.response([]byte(stream(`["1800000000000000000","c"],["1800000010000000000","a"]`)))
	if canonical(backward) == canonical(forward) {
		t.Error("order of different timestamps ignored")
	}
}
