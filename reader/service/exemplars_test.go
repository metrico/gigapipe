package service

import (
	"encoding/json"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/prometheus/prometheus/model/labels"
)

// Prometheus answers query_exemplars for every selector in the expression, not
// only for a bare one, so a query wrapped in a function still has to resolve.
func TestSelectorsOfFindsSelectorsInsideExpressions(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  int
	}{
		{`voice_turn_latency_ms`, 1},
		{`voice_turn_latency_ms{span_name="turn"}`, 1},
		{`rate(voice_turn_latency_ms[5m])`, 1},
		{`rate(a[5m]) / rate(b[5m])`, 2},
		{`histogram_quantile(0.99, sum(rate(h_bucket[5m])) by (le))`, 1},
	} {
		got, err := selectorsOf(tc.query)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		if len(got) != tc.want {
			t.Errorf("%s: found %d selectors, want %d", tc.query, len(got), tc.want)
		}
	}
}

func TestSelectorsOfCarriesTheMetricNameAsAMatcher(t *testing.T) {
	got, err := selectorsOf(`voice_turn_latency_ms{span_name="turn"}`)
	if err != nil {
		t.Fatal(err)
	}
	// The metric name is stored as the __name__ label, so it has to arrive as
	// a matcher like any other or the selector would match every metric.
	var names []string
	for _, m := range got[0] {
		names = append(names, m.Name)
	}
	wantName, wantSpan := false, false
	for _, n := range names {
		if n == labels.MetricName {
			wantName = true
		}
		if n == "span_name" {
			wantSpan = true
		}
	}
	if !wantName || !wantSpan {
		t.Errorf("matchers = %v, want both %s and span_name", names, labels.MetricName)
	}
}

func TestSelectorsOfRejectsAQueryWithNoSelector(t *testing.T) {
	// Returning no selectors would otherwise read every exemplar in the window.
	if _, err := selectorsOf(`1 + 1`); err == nil {
		t.Error("accepted a query with no metric selector")
	}
}

func TestSelectorsOfRejectsUnparseablePromQL(t *testing.T) {
	if _, err := selectorsOf(`{{{nonsense`); err == nil {
		t.Error("accepted unparseable PromQL")
	}
}

func TestFingerprintPlannerMapsEveryMatcherType(t *testing.T) {
	matchers := []*labels.Matcher{
		{Type: labels.MatchEqual, Name: "a", Value: "1"},
		{Type: labels.MatchNotEqual, Name: "b", Value: "2"},
		{Type: labels.MatchRegexp, Name: "c", Value: "3"},
		{Type: labels.MatchNotRegexp, Name: "d", Value: "4"},
	}
	planner, err := fingerprintPlanner(matchers)
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := planner.(*clickhouse_planner.StreamSelectPlanner)
	if !ok {
		t.Fatalf("planner is %T, want *StreamSelectPlanner", planner)
	}
	// The operators are spelled the same in PromQL and LogQL; this pins that
	// they are not silently reordered or dropped relative to their labels.
	wantOps := []string{"=", "!=", "=~", "!~"}
	if len(sel.Ops) != len(wantOps) {
		t.Fatalf("ops = %v, want %v", sel.Ops, wantOps)
	}
	for i := range wantOps {
		if sel.Ops[i] != wantOps[i] {
			t.Errorf("ops[%d] = %q, want %q", i, sel.Ops[i], wantOps[i])
		}
		if sel.LabelNames[i] != matchers[i].Name || sel.Values[i] != matchers[i].Value {
			t.Errorf("matcher %d mismatched: %q=%q", i, sel.LabelNames[i], sel.Values[i])
		}
	}
}

// Prometheus does not marshal an exemplar with the struct tags on
// exemplar.Exemplar. It registers a custom encoder
// (web/api/v1/json_codec.go, marshalExemplarJSON) that sends the value through
// jsonutil.MarshalFloat, which wraps it in quotes, and the timestamp through
// jsonutil.MarshalTimestamp, which does not. So the value is a JSON string and
// the timestamp a JSON number, and a client that type-checks either one breaks
// if we get it the wrong way round. qryn-go already emits the quoted form.
func TestExemplarJSONUsesPrometheusValueAndTimestampTypes(t *testing.T) {
	var got struct {
		Labels    map[string]string `json:"labels"`
		Value     any               `json:"value"`
		Timestamp any               `json:"timestamp"`
	}
	raw := exemplarJSON("trace_id", "0102030405060708090a0b0c0d0e0f10", 3, 1600096945479000000)
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, raw)
	}
	if _, ok := got.Value.(string); !ok {
		t.Errorf("value must be a JSON string, got %T in:\n%s", got.Value, raw)
	}
	if _, ok := got.Timestamp.(float64); !ok {
		t.Errorf("timestamp must be a JSON number, got %T in:\n%s", got.Timestamp, raw)
	}
	if got.Labels["trace_id"] != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace id not carried on the trace_id label: %#v", got.Labels)
	}
}

// The timestamp is fractional seconds, not milliseconds and not nanoseconds.
func TestExemplarJSONRendersTheTimestampAsFractionalSeconds(t *testing.T) {
	raw := exemplarJSON("trace_id", "abc", 1, 1600096945479000000)
	want := `{"labels":{"trace_id":"abc"},"value":"1","timestamp":1600096945.479}`
	if raw != want {
		t.Errorf("got  %s\nwant %s", raw, want)
	}
}

// MarshalTimestamp appends a fraction only when there is one, so a whole
// second must not pick up a trailing .000 that Prometheus would never send.
func TestPromTimestampOmitsAnEmptyFraction(t *testing.T) {
	for _, c := range []struct {
		ms   int64
		want string
	}{
		{1600096945000, "1600096945"},
		{1600096945479, "1600096945.479"},
		{1600096945010, "1600096945.010"},
		{1600096945001, "1600096945.001"},
		{-1500, "-1.500"},
		{0, "0"},
	} {
		if got := promTimestamp(c.ms); got != c.want {
			t.Errorf("promTimestamp(%d) = %s, want %s", c.ms, got, c.want)
		}
	}
}

// MarshalFloat switches to exponent notation below 1e-6 and at or above 1e21.
func TestPromFloatMatchesPrometheusFormatting(t *testing.T) {
	for _, c := range []struct {
		in   float64
		want string
	}{
		{3, "3"},
		{1.5, "1.5"},
		{0, "0"},
		{1e-7, "1e-07"},
		{1e21, "1e+21"},
		{1e20, "100000000000000000000"},
	} {
		if got := promFloat(c.in); got != c.want {
			t.Errorf("promFloat(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}
