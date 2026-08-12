package clickhouse_planner

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func renderCond(t *testing.T, c sql.SQLCondition) string {
	t.Helper()
	if c == nil {
		return "<nil>"
	}
	s, err := c.String(&sql.Ctx{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGetOidFilter_NotFederatedIsNil(t *testing.T) {
	if f := GetOidFilter(&shared.PlannerContext{Federated: false}, "samples"); f != nil {
		t.Fatalf("expected nil when not federated, got %q", renderCond(t, f))
	}
}

func TestGetOidFilter_DenyIsFalse(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{Deny: true}}
	if got := renderCond(t, GetOidFilter(ctx, "samples")); got != "(1) == (0)" {
		t.Errorf("deny filter = %q, want (1) == (0)", got)
	}
}

func TestGetOidFilter_EmptyRegexIsFalse(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{}}
	if got := renderCond(t, GetOidFilter(ctx, "")); got != "(1) == (0)" {
		t.Errorf("empty-regex filter = %q, want (1) == (0)", got)
	}
}

func TestGetOidFilter_RegexAnchoredMatch(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{Regex: "platform|data"}}
	got := renderCond(t, GetOidFilter(ctx, "samples"))
	want := `(match(samples.oid, '^(platform|data)$')) == (1)`
	if got != want {
		t.Errorf("regex filter = %q, want %q", got, want)
	}
}

func TestGetOidFilter_UnqualifiedColumn(t *testing.T) {
	ctx := &shared.PlannerContext{Federated: true, OidFilter: shared.OidFilter{Regex: "platform"}}
	got := renderCond(t, GetOidFilter(ctx, ""))
	want := `(match(oid, '^(platform)$')) == (1)`
	if got != want {
		t.Errorf("unqualified filter = %q, want %q", got, want)
	}
}
