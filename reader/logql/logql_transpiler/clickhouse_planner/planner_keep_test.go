package clickhouse_planner

import (
	"strings"
	"testing"

	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

func TestMapKeepFilter(t *testing.T) {
	filter := mapKeepFilter{
		col:    sql.NewRawObject("labels"),
		labels: []string{"k8s_object_kind", "method"},
		values: []string{"", "GET"},
	}

	ctx := &sql.Ctx{Params: map[string]sql.SQLObject{}, Result: map[string]sql.SQLObject{}}
	got, err := filter.String(ctx)
	if err != nil {
		t.Fatalf("String() error = %v", err)
	}

	if !strings.Contains(got, "mapFilter") {
		t.Fatalf("expected mapFilter expression, got %q", got)
	}
	if !strings.Contains(got, "k=='k8s_object_kind'") {
		t.Fatalf("expected unconditional keep for k8s_object_kind, got %q", got)
	}
	if !strings.Contains(got, "(k, v)==('method', 'GET')") {
		t.Fatalf("expected conditional keep for method=GET, got %q", got)
	}
	if !strings.Contains(got, " or ") {
		t.Fatalf("expected OR-combined keep clauses, got %q", got)
	}
}
