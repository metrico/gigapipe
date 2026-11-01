package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
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

func TestMigrateMetricsRunsTheSchemaStep(t *testing.T) {
	for _, mode := range []string{"all", "writer", "init_only", "migrate_metrics"} {
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
	if code != 2 || !strings.Contains(lines[len(lines)-1], "cluster form not supported") {
		t.Fatalf("exit %d, %q", code, lines)
	}
}

func TestAHeldLeaseExitsThreeNamingTheHolder(t *testing.T) {
	code, lines := runMigrate(t, nil, &maintenance.MigrationLeaseHeldError{
		Holder: "host-a-12", Renewed: time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC), TakeoverIn: 2 * time.Minute,
	})
	if code != 3 {
		t.Fatalf("exit %d", code)
	}
	last := lines[len(lines)-1]
	for _, want := range []string{"host-a-12", "2026-10-02T09:00:00Z", "2m0s"} {
		if !strings.Contains(last, want) {
			t.Errorf("last line %q lacks %q", last, want)
		}
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
