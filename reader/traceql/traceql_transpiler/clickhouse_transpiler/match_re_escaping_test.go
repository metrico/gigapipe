package clickhouse_transpiler

import (
	"testing"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// Regex values reach ClickHouse inside string literals. A backslash must not
// change where the literal ends, or the value becomes part of the query text.

func TestMatchReEscapesTheRegex(t *testing.T) {
	for _, tc := range []struct{ name, re, want string }{
		{"trailing backslash", `\`, `match(val,'\\')`},
		{"backslash before a quote", `\') OR 1=1 -- `, `match(val,'\\\') OR 1=1 -- ')`},
		{"quote", `it's`, `match(val,'it\'s')`},
		{"regex class", `^a\d+$`, `match(val,'^a\\d+$')`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchRe{field: sql.NewRawObject("val"), re: tc.re}.String(&sql.Ctx{})
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
