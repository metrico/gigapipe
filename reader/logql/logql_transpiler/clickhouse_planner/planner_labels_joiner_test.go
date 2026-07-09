package clickhouse_planner

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// staticPlanner is a test double returning a fixed SELECT.
type staticPlanner struct {
	sel sql.ISelect
}

func (s staticPlanner) Process(_ *shared.PlannerContext) (sql.ISelect, error) {
	return s.sel, nil
}

func mockMain() sql.ISelect {
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
			sql.NewSimpleCol("samples.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("samples.string", "string"),
			sql.NewSimpleCol("0", "value")).
		From(sql.NewSimpleCol("samples_v3_dist", "samples"))
}

func mockTimeSeries() sql.ISelect {
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("time_series.fingerprint", "fingerprint"),
			sql.NewSimpleCol("time_series.labels", "labels")).
		From(sql.NewSimpleCol("time_series_dist", "time_series"))
}

func newCtx() *sql.Ctx {
	return &sql.Ctx{Params: map[string]sql.SQLObject{}, Result: map[string]sql.SQLObject{}}
}

// The labels resolution must only cover fingerprints that actually appear in the
// time-windowed `main` subquery, not every fingerprint matching the stream selector.
// Otherwise high-cardinality hosts build a huge fingerprint->labels map (issue #702).
func TestLabelsJoinBoundsClusterMapToMainFingerprints(t *testing.T) {
	planner := &LabelsJoinPlanner{
		NoStreamSelect: true,
		Main:           staticPlanner{mockMain()},
		TimeSeries:     staticPlanner{mockTimeSeries()},
	}

	sel, err := planner.Process(&shared.PlannerContext{IsCluster: true})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	got, err := sel.String(newCtx())
	if err != nil {
		t.Fatalf("String() error = %v", err)
	}

	// Cluster mode must use GLOBAL IN: both `main` (samples_v3_dist) and
	// `_time_series` (time_series_dist) are distributed, and a plain IN between
	// them trips ClickHouse's distributed_product_mode='deny' (error 288).
	if !strings.Contains(got, "time_series.fingerprint GLOBAL IN ( SELECT fingerprint FROM main)") {
		t.Fatalf("cluster labels map not bounded via GLOBAL IN, got:\n%s", got)
	}
}

func TestLabelsJoinBoundsSingleNodeJoinToMainFingerprints(t *testing.T) {
	planner := &LabelsJoinPlanner{
		NoStreamSelect: true,
		Main:           staticPlanner{mockMain()},
		TimeSeries:     staticPlanner{mockTimeSeries()},
	}

	sel, err := planner.Process(&shared.PlannerContext{IsCluster: false})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	got, err := sel.String(newCtx())
	if err != nil {
		t.Fatalf("String() error = %v", err)
	}

	if !strings.Contains(got, "time_series.fingerprint IN ( SELECT fingerprint FROM main)") {
		t.Fatalf("single-node labels join not bounded to main fingerprints, got:\n%s", got)
	}
}
