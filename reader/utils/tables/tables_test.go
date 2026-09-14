package tables

import (
	"os"
	"testing"
	"time"

	clokiconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/shared/distconfig"
	"github.com/metrico/qryn/v5/shared/samplesconfig"
)

func reload(t *testing.T) {
	t.Helper()
	if err := samplesconfig.Reload(); err != nil {
		t.Fatalf("samplesconfig reload: %v", err)
	}
	t.Cleanup(func() {
		t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "false")
		t.Setenv("METRICS_AGGR_ENABLED", "true")
		t.Setenv("METRICS_AGGR_INTERVAL", "")
		_ = samplesconfig.Reload()
	})
}

func singleNodeDB() *model.DataDatabasesMap {
	return &model.DataDatabasesMap{Config: &clokiconfig.ClokiBaseDataBase{Name: "qryn"}}
}

// PopulateTableNames appends distconfig.Suffix() to the dist names, and that
// suffix is empty until Init runs.
func TestMain(m *testing.M) {
	distconfig.Init()
	os.Exit(m.Run())
}

// Without the split both signals read the shared tables, exactly as before.
func TestPopulateTableNamesNoSplit(t *testing.T) {
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "false")
	reload(t)

	for _, tp := range []uint8{shared.SAMPLES_TYPE_LOGS, shared.SAMPLES_TYPE_METRICS, shared.SAMPLES_TYPE_BOTH} {
		ctx := PopulateTableNames(&shared.PlannerContext{Type: tp}, singleNodeDB())
		if ctx.SamplesTableName != "samples_v3" {
			t.Errorf("type %d: samples table = %q, want samples_v3", tp, ctx.SamplesTableName)
		}
		if ctx.Metrics15sTableName != "metrics_15s" {
			t.Errorf("type %d: aggr table = %q, want metrics_15s", tp, ctx.Metrics15sTableName)
		}
		if ctx.AggrInterval != 15*time.Second {
			t.Errorf("type %d: interval = %v, want 15s", tp, ctx.AggrInterval)
		}
	}
}

// With the split the metrics context reads the metrics tables and every other
// context reads the log ones. Type is the selector every reader entry point
// already sets.
func TestPopulateTableNamesSplit(t *testing.T) {
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	t.Setenv("METRICS_AGGR_INTERVAL", "60s")
	reload(t)

	metrics := PopulateTableNames(&shared.PlannerContext{Type: shared.SAMPLES_TYPE_METRICS}, singleNodeDB())
	if metrics.SamplesTableName != "samples_metrics" {
		t.Errorf("metrics samples table = %q", metrics.SamplesTableName)
	}
	if metrics.Metrics15sTableName != "metrics_aggr" {
		t.Errorf("metrics aggr table = %q", metrics.Metrics15sTableName)
	}
	if metrics.AggrInterval != time.Minute {
		t.Errorf("metrics interval = %v, want 1m", metrics.AggrInterval)
	}

	for _, tp := range []uint8{shared.SAMPLES_TYPE_LOGS, shared.SAMPLES_TYPE_BOTH} {
		logs := PopulateTableNames(&shared.PlannerContext{Type: tp}, singleNodeDB())
		if logs.SamplesTableName != "samples_logs" {
			t.Errorf("type %d: samples table = %q", tp, logs.SamplesTableName)
		}
		if logs.Metrics15sTableName != "logs_aggr" {
			t.Errorf("type %d: aggr table = %q", tp, logs.Metrics15sTableName)
		}
		// Logs keep their 15s preaggregate whatever the metrics interval is.
		if logs.AggrInterval != 15*time.Second {
			t.Errorf("type %d: interval = %v, want 15s", tp, logs.AggrInterval)
		}
	}
}

// A disabled metrics aggregate reports interval 0, which is how the read path
// learns to go to raw samples.
func TestPopulateTableNamesAggrDisabled(t *testing.T) {
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	t.Setenv("METRICS_AGGR_ENABLED", "false")
	reload(t)

	metrics := PopulateTableNames(&shared.PlannerContext{Type: shared.SAMPLES_TYPE_METRICS}, singleNodeDB())
	if metrics.AggrInterval != 0 {
		t.Errorf("metrics interval = %v, want 0", metrics.AggrInterval)
	}
	if metrics.SamplesTableName != "samples_metrics" {
		t.Errorf("metrics samples table = %q", metrics.SamplesTableName)
	}
	// Logs are unaffected by the metrics switch.
	logs := PopulateTableNames(&shared.PlannerContext{Type: shared.SAMPLES_TYPE_LOGS}, singleNodeDB())
	if logs.AggrInterval != 15*time.Second {
		t.Errorf("logs interval = %v, want 15s", logs.AggrInterval)
	}
}

// In cluster mode the dist names carry the db prefix and the configured suffix.
func TestPopulateTableNamesCluster(t *testing.T) {
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	reload(t)

	db := &model.DataDatabasesMap{Config: &clokiconfig.ClokiBaseDataBase{
		Name: "qryn", ClusterName: "c",
	}}
	ctx := PopulateTableNames(&shared.PlannerContext{Type: shared.SAMPLES_TYPE_METRICS}, db)
	if ctx.SamplesDistTableName != "`qryn`.samples_metrics_dist" {
		t.Errorf("dist samples table = %q", ctx.SamplesDistTableName)
	}
	if ctx.Metrics15sDistTableName != "`qryn`.metrics_aggr_dist" {
		t.Errorf("dist aggr table = %q", ctx.Metrics15sDistTableName)
	}
}
