package traceql_transpiler

import (
	"encoding/json"
	"math"
	"slices"
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
)

// Spanset attributes serialise as OTLP key-values: ints as decimal strings,
// floats as doubles, durations as Go duration strings, bools as bools and
// strings as their text; nil is the string "nil".
func TestSpanSetAttrsJSON(t *testing.T) {
	attrs := spanSetAttrs(
		[]string{"count()", "avg(.a)", "avg(duration)", "max(span:duration)", "sum(.b)", "sum(.c)",
			"by(.x)", "by(.y = 1)", "by(.z)", "by(.w)"},
		[]string{"int", "float", "duration", "duration", "int", "float", "string", "bool", "nil", "bool"},
		[]float64{3, 2.5, 500225, 2e6, -7, math.Inf(1), 12, 1, 0, 0},
		[]string{"", "", "", "", "", "", "12", "", "", ""})
	got, err := json.Marshal(model.SpanSet{Spans: []model.SpanInfo{}, Matched: 3, Attributes: attrs})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"spans":[],"matched":3,"attributes":[` +
		`{"key":"count()","value":{"intValue":"3"}},` +
		`{"key":"avg(.a)","value":{"doubleValue":2.5}},` +
		`{"key":"avg(duration)","value":{"stringValue":"500.225µs"}},` +
		`{"key":"max(span:duration)","value":{"stringValue":"2ms"}},` +
		`{"key":"sum(.b)","value":{"intValue":"-7"}},` +
		`{"key":"sum(.c)","value":{"doubleValue":"Infinity"}},` +
		`{"key":"by(.x)","value":{"stringValue":"12"}},` +
		`{"key":"by(.y = 1)","value":{"boolValue":true}},` +
		`{"key":"by(.z)","value":{"stringValue":"nil"}},` +
		`{"key":"by(.w)","value":{"boolValue":false}}]}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestSpanSetWithoutAttrsOmitsThem(t *testing.T) {
	got, err := json.Marshal(model.SpanSet{Spans: []model.SpanInfo{}, Attributes: spanSetAttrs(nil, nil, nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"spans":[],"matched":0}` {
		t.Errorf("got %s", got)
	}
}

// Selected values serialise as string attributes of the span; the span name
// is its name.
func TestSelectedValuesJSON(t *testing.T) {
	span := model.SpanInfo{SpanID: "01", StartTimeUnixNano: "1", DurationNanos: "2", Attributes: make([]model.SpanAttr, 0)}
	addSelected(&span, []string{"a", "name", "status"}, []string{"200", "GET /", "error"})
	got, err := json.Marshal(span)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"spanID":"01","name":"GET /","startTimeUnixNano":"1","durationNanos":"2","attributes":[` +
		`{"key":"a","value":{"stringValue":"200"}},{"key":"status","value":{"stringValue":"error"}}]}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	plain, _ := json.Marshal(model.SpanInfo{SpanID: "01", Attributes: make([]model.SpanAttr, 0)})
	if string(plain) != `{"spanID":"01","startTimeUnixNano":"","durationNanos":"","attributes":[]}` {
		t.Errorf("got %s", plain)
	}
}

// Selected nested-set values serialise as int attributes, among string ones.
func TestSelectedNestedSetValuesJSON(t *testing.T) {
	span := model.SpanInfo{SpanID: "01", StartTimeUnixNano: "1", DurationNanos: "2", Attributes: make([]model.SpanAttr, 0)}
	addSelected(&span, []string{"a", "nestedSetLeft", "nestedSetParent", "nestedSetRight"}, []string{"7", "3", "-1", "0"})
	got, err := json.Marshal(span)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"spanID":"01","startTimeUnixNano":"1","durationNanos":"2","attributes":[` +
		`{"key":"a","value":{"stringValue":"7"}},{"key":"nestedSetLeft","value":{"intValue":"3"}},` +
		`{"key":"nestedSetParent","value":{"intValue":"-1"}},{"key":"nestedSetRight","value":{"intValue":"0"}}]}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// A span's duration serialises as its nanoseconds.
func TestSearchSpanDurationEqualsTimestamp(t *testing.T) {
	got, err := json.Marshal(searchSpan("01", 1700000000000000000, 1700000000000000000))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"spanID":"01","startTimeUnixNano":"1700000000000000000","durationNanos":"1700000000000000000","attributes":[]}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// A spanset lists its spans earliest-first, ties by span id.
func TestSortSpansEarliestFirst(t *testing.T) {
	spans := []model.SpanInfo{
		{SpanID: "0e", StartTimeUnixNano: "20"},
		{SpanID: "0b", StartTimeUnixNano: "9"},
		{SpanID: "0c", StartTimeUnixNano: "20"},
		{SpanID: "0a", StartTimeUnixNano: "100"},
		{SpanID: "0d", StartTimeUnixNano: "20"},
	}
	sortSpans(spans)
	var got []string
	for _, s := range spans {
		got = append(got, s.SpanID)
	}
	if want := []string{"0b", "0c", "0d", "0e", "0a"}; !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}
