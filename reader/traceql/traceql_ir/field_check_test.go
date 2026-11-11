package traceql_ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func filterOf(t *testing.T, query string) FieldExpr {
	t.Helper()
	return mustParse(t, query).Pipeline.Elements[0].(*Selector).Filter
}

func TestCheckFilter(t *testing.T) {
	for query, want := range map[string]string{
		`{.a = 1}`:                      "",
		`{.a = .b}`:                     "",
		`{.a + .b > 3}`:                 "",
		`{!(.a = 1)}`:                   "",
		`{!.a}`:                         "",
		`{.a}`:                          "",
		`{.a = nil}`:                    "",
		`{name != nil}`:                 "",
		`{duration > 100}`:              "",
		`{.a > 5ms}`:                    "",
		`{.a = "x" || .b =~ "y.*"}`:     "",
		`{status = error}`:              "",
		`{kind != server}`:              "",
		`{span.foo = bar}`:              "",
		`{.a = 1 = true}`:               "",
		`{1 + 2 * 3 = 7}`:               "",
		`{-.a = 3}`:                     "",
		`{.a + 1}`:                      "span filter field expressions must resolve to a boolean: { .a + 1 }",
		`{-.a}`:                         "span filter field expressions must resolve to a boolean: { -.a }",
		`{.a * .b}`:                     "span filter field expressions must resolve to a boolean: { .a * .b }",
		`{!.a = 1}`:                     "binary operations must operate on the same type: (!.a) = 1",
		`{!.a = true}`:                  "",
		`{foo = "x"}`:                   "unknown identifier: foo",
		`{foo = bar}`:                   "unknown identifier: foo",
		`{name = foo}`:                  "",
		`{.a + .b = "x"}`:               `binary operations must operate on the same type: (.a + .b) = "x"`,
		`{name}`:                        "span filter field expressions must resolve to a boolean: { name }",
		`{error}`:                       "span filter field expressions must resolve to a boolean: { error }",
		`{"x" = 1}`:                     `binary operations must operate on the same type: "x" = 1`,
		`{name = 1}`:                    "binary operations must operate on the same type: name = 1",
		`{duration = "1s"}`:             `binary operations must operate on the same type: duration = "1s"`,
		`{.a + "x" = 1}`:                `illegal operation for the given types: .a + "x"`,
		`{.a = true + 1}`:               "binary operations must operate on the same type: true + 1",
		`{1 =~ 2}`:                      "illegal operation for the given types: 1 =~ 2",
		`{.a =~ .b}`:                    "invalid type for =~ or !~: .b",
		`{.a =~ "("}`:                   `invalid regex: "("`,
		`{.a && 1}`:                     "illegal operation for the given types: .a && 1",
		`{!1}`:                          "illegal operation for the given type: !1",
		`{-"x" = 1}`:                    `illegal operation for the given type: -"x"`,
		`{status = "error"}`:            `binary operations must operate on the same type: status = "error"`,
		`{status = 2}`:                  "binary operations must operate on the same type: status = 2",
		`{status > error}`:              "illegal operation for the given types: status > error",
		`{status = foo}`:                "status compares only with = or != against error, ok, unset: status = foo",
		`{kind = error}`:                "kind compares only with = or != against unspecified, internal, server, client, producer, consumer: kind = error",
		`{status = .a}`:                 "status compares only with = or != against error, ok, unset: status = .a",
		`{error = status}`:              "",
		`{status = kind}`:               "binary operations must operate on the same type: status = kind",
		`{duration = nil}`:              "duration=nil is not valid because intrinsics cannot be nil",
		`{resource.service.name = nil}`: "resource.service.name=nil is not valid because resource.service.name cannot be nil",
	} {
		t.Run(query, func(t *testing.T) {
			err := CheckFilter(filterOf(t, query))
			if want == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, want)
			}
		})
	}
}

func TestCheckAggregate(t *testing.T) {
	for query, want := range map[string]string{
		`{} | avg(.a + .b) > 1`:      "",
		`{} | sum(duration * 2) > 1`: "",
		`{} | min(-.a) > 1`:          "",
		`{} | max(spanN) > 1`:        "",
		`{} | count() > 1`:           "",
		`{} | avg(!.a) > 1`:          "aggregate field expressions must resolve to a number type: avg(!.a)",
		`{} | avg(.a > 1) > 1`:       "aggregate field expressions must resolve to a number type: avg(.a > 1)",
		`{} | avg(name) > 1`:         "aggregate field expressions must resolve to a number type: avg(name)",
		`{} | avg(true) > 1`:         "aggregate field expressions must resolve to a number type: avg(true)",
		`{} | avg("x") > 1`:          `aggregate field expressions must resolve to a number type: avg("x")`,
		`{} | avg(2) > 1`:            "aggregate field expressions must reference the span: avg(2)",
		`{} | avg(1 + 2) > 1`:        "aggregate field expressions must reference the span: avg(1 + 2)",
		`{} | avg(.a + "x") > 1`:     `illegal operation for the given types: .a + "x"`,
	} {
		t.Run(query, func(t *testing.T) {
			err := CheckAggregate(mustParse(t, query).Pipeline.Elements[1].(*ScalarFilter).LHS.(*Aggregate))
			if want == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, want)
			}
		})
	}
}
