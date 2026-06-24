package clickhouse_planner

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

func TestLineFormatPlannerBuiltins(t *testing.T) {
	planner := LineFormatPlanner{
		Template: `{{.k8s_object_name}} - {{__line__}}`,
	}

	ctx := &shared.PlannerContext{}
	if err := planner.ProcessTpl(ctx); err != nil {
		t.Fatalf("ProcessTpl() error = %v", err)
	}

	sqlCtx := &sql.Ctx{Params: map[string]sql.SQLObject{}, Result: map[string]sql.SQLObject{}}
	got, err := (&sqlFormat{
		format: planner.formatStr,
		args:   planner.args,
	}).String(sqlCtx)
	if err != nil {
		t.Fatalf("String() error = %v", err)
	}

	if !strings.Contains(got, "format(") {
		t.Fatalf("expected format() SQL, got %q", got)
	}
	if !strings.Contains(got, "labels['k8s_object_name']") {
		t.Fatalf("expected label reference, got %q", got)
	}
	if !strings.Contains(got, "string") {
		t.Fatalf("expected log line column reference, got %q", got)
	}
}

func TestLineFormatPlannerTimestampBuiltin(t *testing.T) {
	planner := LineFormatPlanner{
		Template: `{{ __timestamp__ }}`,
	}

	ctx := &shared.PlannerContext{}
	if err := planner.ProcessTpl(ctx); err != nil {
		t.Fatalf("ProcessTpl() error = %v", err)
	}

	sqlCtx := &sql.Ctx{Params: map[string]sql.SQLObject{}, Result: map[string]sql.SQLObject{}}
	got, err := (&sqlFormat{
		format: planner.formatStr,
		args:   planner.args,
	}).String(sqlCtx)
	if err != nil {
		t.Fatalf("String() error = %v", err)
	}

	if !strings.Contains(got, "fromUnixTimestamp64Nano(timestamp_ns)") {
		t.Fatalf("expected timestamp conversion, got %q", got)
	}
}
