package traceql_ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func attrs(names ...string) []*Attribute {
	res := make([]*Attribute, len(names))
	for i, n := range names {
		res[i] = &Attribute{Name: n}
	}
	return res
}

func TestLowerSelect(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  Pipeline
	}{
		{`{.a = "a"} | select(.x, span.y, resource.z, duration, name, status, span:name)`,
			stages(a(), &Select{Fields: attrs(".x", "span.y", "resource.z", "duration", "name", "status", "span:name")})},
		{`select(.x) | {.a = "a"}`, stages(&Select{Fields: attrs(".x")}, a())},
		{`({.a = "a"} | select(.x)) >> {.b = "b"}`,
			stages(op(OpDescendant, &Pipeline{Elements: []Element{a(), &Select{Fields: attrs(".x")}}}, b()))},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: tc.want}, mustParse(t, tc.query))
		})
	}
}

func TestLowerSelectRejectsFieldExpressions(t *testing.T) {
	for _, query := range []string{
		`{} | select(.a + 1)`,
		`{} | select(.a = 1)`,
		`{} | select(!.a)`,
		`{} | select(-.a)`,
		`{} | select((.a))`,
		`{} | select(1)`,
		`{} | select("a")`,
		`{} | select(true)`,
		`{} | select(foo)`,
		`{} | select(.a, .b * 2)`,
	} {
		t.Run(query, func(t *testing.T) {
			_, err := Parse(query)
			require.EqualError(t, err, "syntax error: select() takes attributes and intrinsics only")
		})
	}
}

func TestSelectString(t *testing.T) {
	require.Equal(t, "select(.a, duration)", (&Select{Fields: attrs(".a", "duration")}).String())
}

// The selected fields of a query are those of every select() stage, nested
// pipelines included, each once in first-seen order.
func TestSelectedFields(t *testing.T) {
	for query, want := range map[string][]*Attribute{
		`{.a = "a"}`:                               nil,
		`{.a = "a"} | select(.x)`:                  attrs(".x"),
		`select(.x, .y) | {} | select(.y, .z)`:     attrs(".x", ".y", ".z"),
		`({} | select(.x)) >> {} | select(.y, .x)`: attrs(".x", ".y"),
		`{} && ({} | select(.x) | count() > 1)`:    attrs(".x"),
		`{} | select(.x) | rate()`:                 attrs(".x"),
	} {
		require.Equal(t, want, mustParse(t, query).SelectedFields(), query)
	}
}

// The filtered fields of a query are those every spanset filter reads, across
// operators and nested pipelines, each once in first-seen order.
func TestFilteredFields(t *testing.T) {
	for query, want := range map[string][]*Attribute{
		`{}`:                                               nil,
		`{} | select(.x) | count() > 1`:                    nil,
		`{.a = 1 && (name = "n" || !(-.b > 2))}`:           attrs(".a", "name", ".b"),
		`{.a != nil} >> ({span.c = 3} | {.a = 1})`:         attrs(".a", "span.c"),
		`{nestedSetParent < 0} && {duration > 1s}`:         attrs("nestedSetParent", "duration"),
		`({.a = .b + 1} | avg(.x) > 1) | by(.y)`:           attrs(".a", ".b"),
		`{status = error} | count() > 1 | {kind = server}`: attrs("status", "kind"),
		`{.a = 1} | rate()`:                                attrs(".a"),
	} {
		require.Equal(t, want, mustParse(t, query).FilteredFields(), query)
	}
}
