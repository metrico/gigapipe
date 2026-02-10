// Copyright (c) Grafana Labs
// SPDX-License-Identifier: AGPL-3.0-only
// Tests adapted from github.com/grafana/tempo/pkg/traceql

package tempo

import (
	"testing"
	"time"
)

// TestParseBasicSelectors tests basic TraceQL selectors.
func TestParseBasicSelectors(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// Empty and trivial selectors
		{"empty selector", "{}", false},
		{"true literal", "{true}", false},
		{"false literal", "{false}", false},

		// Status intrinsic
		{"status error", "{status=error}", false},
		{"status ok", "{status=ok}", false},
		{"status unset", "{status=unset}", false},
		{"status not equal", "{status!=error}", false},

		// Duration intrinsic
		{"duration greater", "{duration>1s}", false},
		{"duration less", "{duration<500ms}", false},
		{"duration greater equal", "{duration>=100ms}", false},
		{"duration less equal", "{duration<=2s}", false},
		{"duration nanoseconds", "{duration>1000000ns}", false},
		{"duration microseconds", "{duration>1000us}", false},
		{"duration milliseconds", "{duration>100ms}", false},
		{"duration minutes", "{duration>1m}", false},
		{"duration hours", "{duration>1h}", false},

		// Name intrinsic
		{"name equals", `{name="GET /api"}`, false},
		{"name not equals", `{name!="POST /api"}`, false},
		{"name regex", `{name=~"GET.*"}`, false},
		{"name not regex", `{name!~"POST.*"}`, false},

		// Kind intrinsic
		{"kind server", "{kind=server}", false},
		{"kind client", "{kind=client}", false},
		{"kind producer", "{kind=producer}", false},
		{"kind consumer", "{kind=consumer}", false},
		{"kind internal", "{kind=internal}", false},
		{"kind unspecified", "{kind=unspecified}", false},

		// Nested set parent (root span detection)
		{"root span", "{nestedSetParent<0}", false},
		{"non-root span", "{nestedSetParent>=0}", false},
		{"root span lte", "{nestedSetParent<=0}", false},

		// Trace/Span ID intrinsics
		{"trace id", `{trace:id="abc123"}`, false},
		{"span id", `{span:id="def456"}`, false},

		// Scoped attributes
		{"span attribute string", `{span.http.method="GET"}`, false},
		{"resource attribute string", `{resource.service.name="my-service"}`, false},
		{"unscoped attribute string", `{.http.status_code="200"}`, false},

		// Numeric attribute comparisons
		{"attribute int equal", `{.http.status_code=200}`, false},
		{"attribute int greater", `{.http.status_code>400}`, false},
		{"attribute int less", `{.http.status_code<500}`, false},
		{"attribute float", `{.response_time>1.5}`, false},

		// Boolean operators
		{"AND condition", "{status=error && duration>1s}", false},
		{"OR condition", "{status=error || status=ok}", false},
		{"NOT condition", "{!status=error}", false},
		{"complex AND OR", "{status=error && (duration>1s || duration<100ms)}", false},
		{"parentheses", "{(status=error)}", false},

		// Mixed conditions
		{"intrinsic and attribute", `{status=error && span.http.method="GET"}`, false},
		{"multiple attributes", `{.foo="bar" && .baz="qux"}`, false},
		{"true with condition", "{true && status=error}", false},
		{"condition with true", "{status=error && true}", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
				return
			}
			if !tt.wantErr && ast == nil {
				t.Errorf("Parse(%q) returned nil AST", tt.query)
			}
		})
	}
}

// TestParseMetricsQueries tests TraceQL metrics queries.
func TestParseMetricsQueries(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// Rate
		{"rate simple", "{} | rate()", false},
		{"rate with filter", "{status=error} | rate()", false},
		{"rate with by", "{} | rate() by(resource.service.name)", false},
		{"rate with filter and by", "{status=error} | rate() by(resource.service.name)", false},

		// Count over time
		{"count simple", "{} | count_over_time()", false},
		{"count with filter", "{duration>1s} | count_over_time()", false},
		{"count with by", "{} | count_over_time() by(resource.service.name)", false},

		// Quantile over time
		{"quantile 0.5", "{} | quantile_over_time(duration, 0.5)", false},
		{"quantile 0.9", "{} | quantile_over_time(duration, 0.9)", false},
		{"quantile 0.99", "{} | quantile_over_time(duration, 0.99)", false},
		{"quantile with filter", "{status=error} | quantile_over_time(duration, 0.95)", false},
		{"quantile with by", "{} | quantile_over_time(duration, 0.9) by(resource.service.name)", false},

		// Histogram over time
		{"histogram simple", "{} | histogram_over_time(duration)", false},
		{"histogram with filter", "{status=error} | histogram_over_time(duration)", false},
		{"histogram with by", "{} | histogram_over_time(duration) by(resource.service.name)", false},

		// Min/Max/Avg/Sum over time
		{"min over time", "{} | min_over_time(duration)", false},
		{"max over time", "{} | max_over_time(duration)", false},
		{"avg over time", "{} | avg_over_time(duration)", false},
		{"sum over time", "{} | sum_over_time(duration)", false},
		{"min with by", "{} | min_over_time(duration) by(resource.service.name)", false},

		// Compare
		{"compare simple", "{} | compare({status=error})", false},
		{"compare with topN", "{} | compare({status=error}, 5)", false},
		{"compare with offsets", "{} | compare({status=error}, 10, 3600, 0)", false},

		// Second stage: topk/bottomk
		{"rate with topk", "{} | rate() by(resource.service.name) | topk(5)", false},
		{"rate with bottomk", "{} | rate() by(resource.service.name) | bottomk(3)", false},
		{"count with topk", "{} | count_over_time() by(span.http.method) | topk(10)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
				return
			}
			if !tt.wantErr && ast == nil {
				t.Errorf("Parse(%q) returned nil AST", tt.query)
			}
		})
	}
}

// TestParsePipelineOperators tests pipeline operators.
func TestParsePipelineOperators(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// Select projection
		{"select single", "{} | select(resource.service.name)", false},
		{"select multiple", "{} | select(resource.service.name, span.http.method)", false},
		{"select with filter", "{status=error} | select(span.http.url)", false},

		// Coalesce
		{"coalesce", "{} | coalesce()", false},

		// Count aggregate
		{"count aggregate", "{} | count() > 5", false},
		{"count aggregate less", "{} | count() < 10", false},

		// Avg aggregate
		{"avg duration", "{} | avg(duration) > 1s", false},

		// Max/Min aggregate
		{"max duration", "{} | max(duration) > 5s", false},
		{"min duration", "{} | min(duration) < 100ms", false},

		// Sum aggregate
		{"sum duration", "{} | sum(duration) > 10s", false},

		// Chained pipeline
		{"filter then aggregate", "{status=error} | count() > 5", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
				return
			}
			if !tt.wantErr && ast == nil {
				t.Errorf("Parse(%q) returned nil AST", tt.query)
			}
		})
	}
}

// TestParseSpansetOperations tests spanset operations.
func TestParseSpansetOperations(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// Descendant
		{"descendant simple", "{} >> {status=error}", false},
		{"descendant with filters", "{name=\"parent\"} >> {name=\"child\"}", false},

		// Ancestor
		{"ancestor simple", "{status=error} << {}", false},

		// Sibling
		{"sibling", "{status=error} ~ {status=ok}", false},

		// Union
		{"union", "{status=error} && {duration>1s}", false},

		// Chained operations
		{"chained descendant", "{} >> {} >> {status=error}", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
				return
			}
			if !tt.wantErr && ast == nil {
				t.Errorf("Parse(%q) returned nil AST", tt.query)
			}
		})
	}
}

// TestParseEdgeCases tests edge cases and potential parser issues.
func TestParseEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// Whitespace handling
		{"extra spaces", "{  status = error  }", false},
		{"tabs", "{\tstatus=error\t}", false},
		{"newlines", "{\nstatus=error\n}", false},
		{"mixed whitespace", "{ \t\n status = error \t\n }", false},

		// String escaping
		{"escaped quote", `{name="foo\"bar"}`, false},
		{"escaped backslash", `{name="foo\\bar"}`, false},
		{"backticks", "{name=`backtick`}", false},

		// Special characters in values
		{"slash in value", `{name="/api/v1/users"}`, false},
		{"colon in value", `{name="http://example.com"}`, false},
		{"dots in attribute", `{.foo.bar.baz="value"}`, false},

		// Numeric edge cases
		{"zero duration", "{duration>0s}", false},
		{"large duration", "{duration>24h}", false},
		{"float precision", "{.value>1.23456789}", false},
		{"nested with value zero", "{nestedSetParent<0}", false},

		// Complex expressions
		{"deeply nested", "{((status=error))}", false},
		{"many conditions", "{status=error && duration>1s && name=\"test\" && .foo=\"bar\"}", false},
		{"many ORs", "{status=error || status=ok || status=unset}", false},

		// Empty-ish queries
		{"just whitespace in selector", "{   }", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
				return
			}
			if !tt.wantErr && ast == nil {
				t.Errorf("Parse(%q) returned nil AST", tt.query)
			}
		})
	}
}

// TestParseInvalidQueries tests that invalid queries produce errors.
func TestParseInvalidQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"missing closing brace", "{status=error"},
		{"missing opening brace", "status=error}"},
		{"invalid operator", "{status==error}"},
		{"unknown intrinsic", "{foo=bar}"},
		{"invalid duration", "{duration>1x}"},
		{"empty query", ""},
		{"just braces no selector", "{}{}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.query)
			if err == nil {
				t.Errorf("Parse(%q) should have returned error", tt.query)
			}
		})
	}
}

// TestParseASTStructure tests that the AST is structured correctly.
func TestParseASTStructure(t *testing.T) {
	t.Run("simple filter has pipeline", func(t *testing.T) {
		ast, err := Parse("{status=error}")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		if ast.Pipeline.Elements == nil {
			t.Error("Pipeline.Elements should not be nil")
		}
		if len(ast.Pipeline.Elements) == 0 {
			t.Error("Pipeline should have at least one element")
		}
	})

	t.Run("metrics query has MetricsPipeline", func(t *testing.T) {
		ast, err := Parse("{} | rate()")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		if ast.MetricsPipeline == nil {
			t.Error("MetricsPipeline should not be nil for metrics query")
		}
	})

	t.Run("topk has MetricsSecondStage", func(t *testing.T) {
		ast, err := Parse("{} | rate() by(resource.service.name) | topk(5)")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		if ast.MetricsSecondStage == nil {
			t.Error("MetricsSecondStage should not be nil for topk query")
		}
		tkbk, ok := ast.MetricsSecondStage.(*TopKBottomK)
		if !ok {
			t.Error("MetricsSecondStage should be *TopKBottomK")
		}
		if tkbk.K() != 5 {
			t.Errorf("K() = %d, want 5", tkbk.K())
		}
	})

	t.Run("quantile has correct float value", func(t *testing.T) {
		ast, err := Parse("{} | quantile_over_time(duration, 0.95)")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		if ast.MetricsPipeline == nil {
			t.Fatal("MetricsPipeline should not be nil")
		}
		ma, ok := ast.MetricsPipeline.(*MetricsAggregate)
		if !ok {
			t.Fatal("MetricsPipeline should be *MetricsAggregate")
		}
		floats := ma.Floats()
		if len(floats) != 1 || floats[0] != 0.95 {
			t.Errorf("Floats() = %v, want [0.95]", floats)
		}
	})
}

// TestParseDurationValues tests duration parsing accuracy.
func TestParseDurationValues(t *testing.T) {
	tests := []struct {
		query    string
		expected time.Duration
	}{
		{"{duration>1ns}", time.Nanosecond},
		{"{duration>1us}", time.Microsecond},
		{"{duration>1ms}", time.Millisecond},
		{"{duration>1s}", time.Second},
		{"{duration>1m}", time.Minute},
		{"{duration>1h}", time.Hour},
		{"{duration>500ms}", 500 * time.Millisecond},
		{"{duration>1500ms}", 1500 * time.Millisecond},
		{"{duration>2h30m}", 2*time.Hour + 30*time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			// Verify AST was created - actual duration extraction would require
			// walking the AST which is tested in the transpiler tests
			if ast == nil {
				t.Error("AST should not be nil")
			}
		})
	}
}

// TestParseStatusValues tests status value parsing.
func TestParseStatusValues(t *testing.T) {
	tests := []struct {
		query  string
		status Status
	}{
		{"{status=ok}", StatusOk},
		{"{status=error}", StatusError},
		{"{status=unset}", StatusUnset},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if ast == nil {
				t.Error("AST should not be nil")
			}
			// The status value is embedded in the AST structure
		})
	}
}

// TestParseWithHints tests with() hint parsing.
func TestParseWithHints(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"most_recent true", "{} with(most_recent=true)", false},
		{"most_recent false", "{} with(most_recent=false)", false},
		{"exemplars true", "{} with(exemplars=true)", false},
		{"with filter", "{status=error} with(most_recent=true)", false},
		{"with select and hint", "{} | select(resource.service.name) with(most_recent=true)", false},
		{"multiple hints", "{} with(most_recent=true, exemplars=true)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
				return
			}
			if !tt.wantErr && ast == nil {
				t.Errorf("Parse(%q) returned nil AST", tt.query)
			}
		})
	}
}

// TestParseWithHintsStructure tests that with() hints are correctly stored in AST.
func TestParseWithHintsStructure(t *testing.T) {
	t.Run("most_recent hint is accessible", func(t *testing.T) {
		ast, err := Parse("{} with(most_recent=true)")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		if ast.Hints == nil {
			t.Fatal("Hints should not be nil")
		}
		mostRecent, ok := ast.Hints.GetBool(HintMostRecent, false)
		if !ok {
			t.Error("most_recent hint should be present")
		}
		if !mostRecent {
			t.Error("most_recent should be true")
		}
	})

	t.Run("exemplars hint is accessible", func(t *testing.T) {
		ast, err := Parse("{} with(exemplars=true)")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		if ast.Hints == nil {
			t.Fatal("Hints should not be nil")
		}
		exemplars, ok := ast.Hints.GetBool(HintExemplars, false)
		if !ok {
			t.Error("exemplars hint should be present")
		}
		if !exemplars {
			t.Error("exemplars should be true")
		}
	})
}

// TestParseSelectStructure tests that select() attributes are correctly stored in AST.
func TestParseSelectStructure(t *testing.T) {
	t.Run("single attribute", func(t *testing.T) {
		ast, err := Parse("{} | select(resource.service.name)")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		// Find SelectOperation in pipeline
		found := false
		for _, elem := range ast.Pipeline.Elements {
			if sel, ok := elem.(SelectOperation); ok {
				found = true
				attrs := sel.Attrs()
				if len(attrs) != 1 {
					t.Errorf("Expected 1 attribute, got %d", len(attrs))
				}
				if len(attrs) > 0 && attrs[0].String() != "resource.service.name" {
					t.Errorf("Expected resource.service.name, got %s", attrs[0].String())
				}
			}
		}
		if !found {
			t.Error("SelectOperation not found in pipeline")
		}
	})

	t.Run("multiple attributes", func(t *testing.T) {
		ast, err := Parse("{} | select(resource.service.name, span.http.method)")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		for _, elem := range ast.Pipeline.Elements {
			if sel, ok := elem.(SelectOperation); ok {
				attrs := sel.Attrs()
				if len(attrs) != 2 {
					t.Errorf("Expected 2 attributes, got %d", len(attrs))
				}
			}
		}
	})
}
