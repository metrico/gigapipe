package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
)

func TestOnlyMigrateMetricsModeMigrates(t *testing.T) {
	for _, mode := range []string{"all", "writer", "reader", "init_only", ""} {
		if migratesMetrics(mode) {
			t.Errorf("mode %q migrates metrics", mode)
		}
	}
	if !migratesMetrics("migrate_metrics") {
		t.Error("migrate_metrics does not migrate metrics")
	}
}

func TestStartUpModesRunTheSchemaStep(t *testing.T) {
	for _, mode := range []string{"all", "writer", "init_only"} {
		if !runsSchemaStep(mode) {
			t.Errorf("mode %q skips the schema step", mode)
		}
	}
	if runsSchemaStep("reader") {
		t.Error("reader runs the schema step")
	}
}

func TestOmitCreateTablesSkipsTheSchemaStepInMigrateMetrics(t *testing.T) {
	ran := false
	saved := createTables
	createTables = func(*clconfig.ClokiConfig) { ran = true }
	defer func() { createTables = saved }()
	cfg := &clconfig.ClokiConfig{}

	t.Setenv("OMIT_CREATE_TABLES", "true")
	schemaStep(cfg)
	if ran {
		t.Fatal("schema step ran with OMIT_CREATE_TABLES")
	}
	t.Setenv("OMIT_CREATE_TABLES", "")
	schemaStep(cfg)
	if !ran {
		t.Fatal("schema step did not run")
	}
}

func runMigrate(t *testing.T, reports []maintenance.MetricMigrationReport, err error) (int, []string) {
	t.Helper()
	var out strings.Builder
	code := runMigrateMetrics(context.Background(), func(context.Context) ([]maintenance.MetricMigrationReport, error) {
		return reports, err
	}, &out)
	return code, strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
}

func TestASuccessfulMigrationPrintsItsSummaryAndExitsZero(t *testing.T) {
	code, lines := runMigrate(t, []maintenance.MetricMigrationReport{{
		Database: "cloki", RawSamples: 1200, Series: 30, Metadata: 4, Duration: 90 * time.Second,
	}}, nil)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	last := lines[len(lines)-1]
	for _, want := range []string{"metric migration:", "1200 raw samples", "30 series", "4 metadata", "1m30s"} {
		if !strings.Contains(last, want) {
			t.Errorf("summary %q lacks %q", last, want)
		}
	}
}

func TestAFailedMigrationNamesTheUnitAndExitsOne(t *testing.T) {
	code, lines := runMigrate(t, nil, &maintenance.MigrationFailedError{
		Database: "cloki",
		Step:     "raw (2026-10-02T08:00:00Z, 2026-10-02T09:00:00Z]",
		Err:      errors.New("code: 241, memory limit exceeded"),
	})
	if code != 1 {
		t.Fatalf("exit %d", code)
	}
	last := lines[len(lines)-1]
	for _, want := range []string{"metric migration:", "raw (2026-10-02T08:00:00Z, 2026-10-02T09:00:00Z]",
		"memory limit exceeded", "re-run"} {
		if !strings.Contains(last, want) {
			t.Errorf("last line %q lacks %q", last, want)
		}
	}
}

func TestARefusedMigrationExitsTwo(t *testing.T) {
	code, lines := runMigrate(t, nil, &maintenance.MigrationRefusedError{Reason: "cluster form not supported"})
	if want := "metric migration refused: cluster form not supported"; code != 2 || lines[len(lines)-1] != want {
		t.Fatalf("exit %d, %q, want %q", code, lines, want)
	}
}

func TestAHeldLeaseExitsThreeNamingTheHolder(t *testing.T) {
	code, lines := runMigrate(t, nil, &maintenance.MigrationLeaseHeldError{
		Holder: "host-a-12", Renewed: time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC), TakeoverIn: 2 * time.Minute,
	})
	want := "metric migration lease held by host-a-12, renewed 2026-10-02T09:00:00Z, taken over in 2m0s"
	if code != 3 || lines[len(lines)-1] != want {
		t.Fatalf("exit %d, %q, want %q", code, lines, want)
	}
}

func TestOtherErrorsExitOne(t *testing.T) {
	if code, _ := runMigrate(t, nil, errors.New("boom")); code != 1 {
		t.Fatalf("exit %d", code)
	}
}

const noNewData = "metric migration: no new data to migrate"

func TestARunWithNoNewDataSaysSoAfterItsSummary(t *testing.T) {
	code, lines := runMigrate(t, []maintenance.MetricMigrationReport{{Database: "cloki", Units: 3, Series: 2}}, nil)
	if code != 0 || len(lines) != 2 || lines[1] != noNewData {
		t.Fatalf("exit %d, %q", code, lines)
	}
}

func TestEachDatabaseWithNoNewDataSaysSo(t *testing.T) {
	_, lines := runMigrate(t, []maintenance.MetricMigrationReport{
		{Database: "a"}, {Database: "b", RawSamples: 5}, {Database: "c"},
	}, nil)
	if len(lines) != 5 || lines[1] != noNewData || lines[3] == noNewData || lines[4] != noNewData {
		t.Fatalf("%q", lines)
	}
}

func TestARunThatCopiedOrRecoveredDoesNotSayNoNewData(t *testing.T) {
	for _, r := range []maintenance.MetricMigrationReport{
		{Database: "cloki", RawSamples: 5},
		{Database: "cloki", Recovered: 1},
	} {
		if _, lines := runMigrate(t, []maintenance.MetricMigrationReport{r}, nil); len(lines) != 1 {
			t.Errorf("%+v printed %q", r, lines)
		}
	}
}

func TestAFailedRunDoesNotSayNoNewData(t *testing.T) {
	_, lines := runMigrate(t, nil,
		&maintenance.MigrationFailedError{Database: "cloki", Step: "lease", Err: errors.New("boom")})
	for _, l := range lines {
		if l == noNewData {
			t.Fatalf("printed %q", lines)
		}
	}
}

func TestASignalledMigrationExitsOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out strings.Builder
	code := runMigrateMetrics(ctx, func(ctx context.Context) ([]maintenance.MetricMigrationReport, error) {
		return nil, &maintenance.MigrationFailedError{Database: "cloki", Step: "raw", Err: ctx.Err()}
	}, &out)
	if code != 1 || !strings.Contains(out.String(), "context canceled") {
		t.Fatalf("exit %d, %q", code, out.String())
	}
}

func migrateMain(t *testing.T, db config.ClokiBaseDataBase) (code int, out string, schemaRan bool) {
	t.Helper()
	return migrateMainWith(t, db, func() {})
}

// migrateMainWith runs migrateMetricsMain with createTables replaced by a stub that calls schema.
func migrateMainWith(t *testing.T, db config.ClokiBaseDataBase, schema func()) (code int, out string, schemaRan bool) {
	t.Helper()
	saved := createTables
	createTables = func(*clconfig.ClokiConfig) { schemaRan = true; schema() }
	defer func() { createTables = saved }()
	cfg := &clconfig.ClokiConfig{Setting: &config.ClokiBaseSettingServer{DATABASE_DATA: []config.ClokiBaseDataBase{db}}}
	var b strings.Builder
	code = migrateMetricsMain(cfg, &b)
	return code, b.String(), schemaRan
}

func TestABadMigrateMetricsEnvValueExitsTwoBeforeTheSchemaStep(t *testing.T) {
	t.Setenv("GIGAPIPE_MIGRATE_METRICS_PAUSE", "soon")
	code, out, schemaRan := migrateMain(t, config.ClokiBaseDataBase{Name: "db", Host: "127.0.0.1", Port: 1})
	if code != 2 || !strings.Contains(out, "GIGAPIPE_MIGRATE_METRICS_PAUSE") || schemaRan {
		t.Fatalf("exit %d, %q, schema step ran: %v", code, out, schemaRan)
	}
}

func TestAClusterDatabaseExitsTwoBeforeTheSchemaStep(t *testing.T) {
	code, out, schemaRan := migrateMain(t, config.ClokiBaseDataBase{Name: "db", Host: "127.0.0.1", Port: 1, ClusterName: "c1"})
	want := "metric migration refused: database db is on cluster c1; CLUSTER_NAME must be unset, the cluster form is not supported\n"
	if code != 2 || out != want || schemaRan {
		t.Fatalf("exit %d, %q, schema step ran: %v", code, out, schemaRan)
	}
}

func TestABadMetrics15sTTLExitsTwoBeforeTheSchemaStep(t *testing.T) {
	t.Setenv("METRICS_15S_TTL_DAYS", "-1")
	code, out, schemaRan := migrateMain(t, config.ClokiBaseDataBase{Name: "db", Host: "127.0.0.1", Port: 1})
	want := "metric migration refused: METRICS_15S_TTL_DAYS=\"-1\": expected a positive number of days\n"
	if code != 2 || out != want || schemaRan {
		t.Fatalf("exit %d, %q, schema step ran: %v", code, out, schemaRan)
	}
}

func TestAFailedSchemaStepExitsOneNamingIt(t *testing.T) {
	code, out, _ := migrateMainWith(t, config.ClokiBaseDataBase{Name: "db", Host: "127.0.0.1", Port: 1}, func() {
		panic(errors.New("dial tcp 127.0.0.1:1: connection refused"))
	})
	want := "metric migration: schema step failed: dial tcp 127.0.0.1:1: connection refused; re-run MODE=migrate_metrics to resume\n"
	if code != 1 || out != want {
		t.Fatalf("exit %d, %q", code, out)
	}
}

func TestASignalDuringTheSchemaStepExitsOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	saved := signalContext
	signalContext = func() (context.Context, context.CancelFunc) { return ctx, cancel }
	defer func() { signalContext = saved }()
	code, out, _ := migrateMainWith(t, config.ClokiBaseDataBase{Name: "db", Host: "127.0.0.1", Port: 1}, cancel)
	want := "metric migration: stopped by a signal before the migration started; re-run MODE=migrate_metrics to resume\n"
	if code != 1 || out != want {
		t.Fatalf("exit %d, %q", code, out)
	}
}

func TestTheSignalHandlerCatchesSIGINT(t *testing.T) {
	ctx, stop := signalContext()
	defer stop()
	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("SIGINT did not cancel the context")
	}
}
