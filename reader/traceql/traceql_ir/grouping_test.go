package traceql_ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLowerGroupingStages(t *testing.T) {
	x := func() *GroupBy { return &GroupBy{Expr: &Attribute{Name: ".x"}} }
	for _, tc := range []struct {
		query string
		want  Pipeline
	}{
		{`{.a = "a"} | by(.x)`, stages(a(), x())},
		{`{.a = "a"} | by(.x) | by(.y + 1) | coalesce()`, stages(a(), x(),
			&GroupBy{Expr: bin("+", &Attribute{Name: ".y"}, &Static{Kind: ValueNumber, Text: "1"})}, &Coalesce{})},
		{`by(.x) | count() > 2`, stages(x(), count(">", "2"))},
		{`({.a = "a"} | by(.x)) | {.b = "b"}`, stages(&Pipeline{Elements: []Element{a(), x()}}, b())},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: tc.want}, mustParse(t, tc.query))
		})
	}

	_, err := Parse(`coalesce() | {.a = "a"}`)
	require.EqualError(t, err, "syntax error: unexpected coalesce()")
}

func TestGroupByCanonicalForm(t *testing.T) {
	for query, want := range map[string]string{
		`{} | by(.a)`:                    "by(.a)",
		`{} | by(resource.service.name)`: "by(resource.service.name)",
		`{} | by(span.a+span.b)`:         "by(span.a + span.b)",
		`{} | by(span.a + span.b * 2)`:   "by(span.a + (span.b * 2))",
		`{} | by(.a = nil)`:              "by(.a = nil)",
		`{} | by(duration > 1ms)`:        "by(duration > 1ms)",
		`{} | by(!(.a = "x"))`:           `by(!(.a = "x"))`,
	} {
		g := mustParse(t, query).Pipeline.Elements[1].(*GroupBy)
		require.Equal(t, want, g.String(), query)
	}
}

func TestCheckGroupBy(t *testing.T) {
	for query, want := range map[string]string{
		`{} | by(.a)`:             "",
		`{} | by(.a + 1)`:         "",
		`{} | by(status)`:         "",
		`{} | by(1)`:              "grouping field expressions must reference the span: by(1)",
		`{} | by(1 + 2)`:          "grouping field expressions must reference the span: by(1 + 2)",
		`{} | by("x")`:            `grouping field expressions must reference the span: by("x")`,
		`{} | by(foo)`:            "grouping field expressions must reference the span: by(foo)",
		`{} | by(.a + "x")`:       `illegal operation for the given types: .a + "x"`,
		`{} | by(status > 1)`:     "binary operations must operate on the same type: status > 1",
		`{} | by(duration = nil)`: "duration=nil is not valid because intrinsics cannot be nil",
	} {
		err := CheckGroupBy(mustParse(t, query).Pipeline.Elements[1].(*GroupBy))
		if want == "" {
			require.NoError(t, err, query)
		} else {
			require.EqualError(t, err, want, query)
		}
	}
}

// A spanset operator over grouped spansets is an error until coalesce().
func TestCheckGrouping(t *testing.T) {
	for query, wantErr := range map[string]bool{
		`{.a = "a"} >> {.b = "b"} | by(.x)`:                          false,
		`{.a = "a"} | by(.x) | {.b = "b"}`:                           false,
		`{.a = "a"} | by(.x) | count() > 1 | {.b = "b"} | by(.y)`:    false,
		`{.a = "a"} | by(.x) | coalesce() | {.b = "b"} > {.c = "c"}`: false,
		`({.a = "a"} | by(.x) | coalesce()) && {.b = "b"}`:           false,
		`{.a = "a"} | by(.x) | ({.b = "b"} | count() > 1)`:           false,
		`{.a = "a"} | ({.b = "b"} | by(.x)) | {.c = "c"}`:            false,
		`by(.x)`: false,
		`{.a = "a"} | by(.x) | {.b = "b"} > {.c = "c"}`:         true,
		`{.a = "a"} | by(.x) | {.b = "b"} && {.c = "c"}`:        true,
		`{.a = "a"} | by(.x) | {.b = "b"} || {.c = "c"}`:        true,
		`{.a = "a"} | by(.x) | ({.b = "b"} >> {.c = "c"})`:      true,
		`{.a = "a"} | by(.x) | ({.b = "b"} | {.c = "c"} ~ {})`:  true,
		`({.a = "a"} | by(.x)) >> {.b = "b"}`:                   true,
		`{.b = "b"} && ({.a = "a"} | by(.x) | count() > 1)`:     true,
		`{.b = "b"} || ({.a = "a"} | coalesce() | by(.x))`:      true,
		`{.b = "b"} || ((({.a = "a"} | by(.x))))`:               true,
		`{.a = "a"} | ({.b = "b"} | by(.x)) | {.c = "c"} !< {}`: true,
		`by(.x) | {.a = "a"} &>> {}`:                            true,
	} {
		err := CheckGrouping(&mustParse(t, query).Pipeline)
		if wantErr {
			require.ErrorIs(t, err, ErrGroupedOperand, query)
		} else {
			require.NoError(t, err, query)
		}
	}
	require.Equal(t, "spanset operators are not supported for multiple spansets per trace. consider using coalesce()",
		ErrGroupedOperand.Error())
}
