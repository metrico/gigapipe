package shared

import (
	"context"
	"testing"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func TestSanitizeOid(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"platform", "platform", true},
		{"platform|data", "platform|data", true},
		{"team-a_1", "team-a_1", true},
		{"a.b/c", "a.b/c", true},
		{"", "", false},
		{"'; DROP TABLE x --", "", false},
		{`a\`, "", false},
		{"a\nb", "", false},
		{`x" OR "1"="1`, "", false},
	}
	for _, c := range cases {
		got, ok := SanitizeOid(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("SanitizeOid(%q) = (%q,%v), want (%q,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestOidConditionFromContext_OffIsNil(t *testing.T) {
	// federation disabled by default in tests (FEDERATED unset).
	if c := OidConditionFromContext(context.Background(), "samples"); c != nil {
		t.Fatalf("expected nil condition when federation is off, got %v", c)
	}
}

func TestOidMatchRendersAnchoredMatch(t *testing.T) {
	m := &oidMatch{col: "samples.oid", pattern: "^(platform|data)$"}
	got, err := m.String(&sql.Ctx{})
	if err != nil {
		t.Fatal(err)
	}
	want := `match(samples.oid, '^(platform|data)$')`
	if got != want {
		t.Errorf("oidMatch = %q, want %q", got, want)
	}
}
