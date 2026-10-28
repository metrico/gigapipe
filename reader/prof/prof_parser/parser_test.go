package prof_parser

import "testing"

// TestParseLabelName pins which label names a selector accepts. Profile tags
// keep the attribute names they were ingested with (service.version, k8s.pod.name),
// so a name outside [a-zA-Z_][a-zA-Z0-9_]* has to be selectable. It is written
// in double quotes, as in the Prometheus UTF-8 matcher syntax, and the parsed
// selector carries the name without the quotes.
func TestParseLabelName(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{`{service_name="a"}`, "service_name"},
		{`{"service.version"="1.4.0"}`, "service.version"},
		{`{"service_name"="a"}`, "service_name"},
		{`{"a\"b"="x"}`, `a"b`},
		{`{"k8s.pod.name"!~"api-.*"}`, "k8s.pod.name"},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			script, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse(%s): %v", tt.query, err)
			}
			if len(script.Selectors) != 1 {
				t.Fatalf("want 1 selector, got %d", len(script.Selectors))
			}
			if got := string(script.Selectors[0].Name); got != tt.want {
				t.Errorf("Name = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestParseMixedLabelNames checks quoted and bare names combine in one selector.
func TestParseMixedLabelNames(t *testing.T) {
	script, err := Parse(`{service_name="checkout", "service.version"="1.4.0"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	var names []string
	for _, s := range script.Selectors {
		names = append(names, string(s.Name))
	}
	if len(names) != 2 || names[0] != "service_name" || names[1] != "service.version" {
		t.Fatalf("names = %q, want [service_name service.version]", names)
	}
}

// TestParseRejectsBareDottedName keeps the bare form an error: an unquoted name
// is limited to [a-zA-Z_][a-zA-Z0-9_]*, as in PromQL.
func TestParseRejectsBareDottedName(t *testing.T) {
	if _, err := Parse(`{service.version="1.4.0"}`); err == nil {
		t.Fatal("want a parse error for an unquoted dotted label name")
	}
}
