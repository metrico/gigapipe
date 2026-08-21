package goldentest

import (
	"reflect"
	"testing"
)

// ParserPinCase is one table-driven "valid query -> expected AST shape" or
// "invalid query -> expected error class" pin for a single query head's
// parser.
//
// Deep AST-shape assertions are genuinely different per head (PromQL,
// LogQL and TraceQL parsers produce unrelated types with no common
// interface worth inventing), so this helper only guarantees the shallow,
// generic assertion that is easy to make generic: a valid query parses to a
// non-nil root with no error, and an invalid query returns a non-nil error.
// A case that wants a deeper shape pin supplies Check, run only when parsing
// itself succeeds as expected.
type ParserPinCase[T any] struct {
	// Name is the subtest name (t.Run).
	Name string
	// Query is the raw text handed to the parser.
	Query string
	// WantErr marks this as an invalid-query case: Parse must return a
	// non-nil error, and Check (if set) is not run.
	WantErr bool
	// Check, if set, receives the parsed AST for a valid-query case for a
	// head to assert deeper shape beyond "non-nil root".
	Check func(t *testing.T, ast T)
}

// RunParserPins runs every case in cases as a subtest, calling parse and
// asserting either a non-nil root with no error (WantErr == false) or a
// non-nil error (WantErr == true).
func RunParserPins[T any](t *testing.T, parse func(query string) (T, error), cases []ParserPinCase[T]) {
	t.Helper()
	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			ast, err := parse(c.Query)
			if c.WantErr {
				if err == nil {
					t.Fatalf("Parse(%q): expected an error, got none", c.Query)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q): unexpected error: %v", c.Query, err)
			}
			if isNilAST(ast) {
				t.Fatalf("Parse(%q): expected a non-nil AST root", c.Query)
			}
			if c.Check != nil {
				c.Check(t, ast)
			}
		})
	}
}

// isNilAST reports whether a generic AST root (typically a pointer) is nil,
// without requiring T to satisfy any particular interface.
func isNilAST(ast any) bool {
	v := reflect.ValueOf(ast)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}
