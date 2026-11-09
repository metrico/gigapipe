package tempo

import (
	"strings"
	"testing"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// By() labels reach ClickHouse inside string literals. A backslash must not
// change where the literal ends, or the label becomes part of the query text.

func TestByLabelIsEscaped(t *testing.T) {
	window := sql.Eq(sql.NewRawObject("1"), sql.NewRawObject("1"))
	with, _ := buildByPivotCTE([]string{`span.a\'b`}, window, "attrs")
	got := render(t, with.GetQuery())
	if want := `anyIf(val, key = 'a\\\'b')`; !strings.Contains(got, want) {
		t.Fatalf("want %s in:\n%s", want, got)
	}
}
