package clickhouse_planner

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// All time_series label-resolution paths must bound the resolved fingerprint set
// to the query time window, otherwise a high-cardinality selector builds labels
// for every fingerprint that ever matched it (issue #702 OOM). In cluster mode the
// bound must be GLOBAL IN (both tables distributed -> error 288 with a plain IN).

func TestByWithoutBoundsLabelsToWindowCluster(t *testing.T) {
	var cache *sql.With
	planner := &ByWithoutPlanner{
		NoStreamSelect:     true,
		Main:               staticPlanner{mockMain()},
		Labels:             []string{"level"},
		By:                 true,
		UseTimeSeriesTable: true,
		LabelsCache:        &cache,
	}
	got, err := renderCluster(planner)
	if err != nil {
		t.Fatalf("render error: %v", err)
	}
	if !strings.Contains(got, "time_series.fingerprint GLOBAL IN (") {
		t.Fatalf("by/without labels not bounded to window via GLOBAL IN, got:\n%s", got)
	}
}

func TestDropSimpleBoundsLabelsToWindowCluster(t *testing.T) {
	var cache *sql.With
	planner := &PlannerDropSimple{
		NoStreamSelect: true,
		Labels:         []string{"level"},
		Vals:           []string{""},
		LabelsCache:    &cache,
		Main:           staticPlanner{mockMain()},
	}
	got, err := renderCluster(planner)
	if err != nil {
		t.Fatalf("render error: %v", err)
	}
	if !strings.Contains(got, "time_series.fingerprint GLOBAL IN (") {
		t.Fatalf("drop labels not bounded to window via GLOBAL IN, got:\n%s", got)
	}
}

func renderCluster(p shared.SQLRequestPlanner) (string, error) {
	sel, err := p.Process(&shared.PlannerContext{IsCluster: true})
	if err != nil {
		return "", err
	}
	return sel.String(newCtx())
}
