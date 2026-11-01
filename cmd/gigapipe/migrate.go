package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"syscall"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/ctrl"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
)

const migrateMetricsMode = "migrate_metrics"

// runsSchemaStep reports whether mode creates and upgrades the tables on start;
// migrateMetricsMain runs it for migrate_metrics.
func runsSchemaStep(mode string) bool {
	return slices.Contains([]string{"all", "writer", "init_only"}, mode)
}

// migratesMetrics reports whether mode runs one metric migration and exits.
func migratesMetrics(mode string) bool {
	return mode == migrateMetricsMode
}

// createTables runs the schema step.
var createTables = func(cfg *clconfig.ClokiConfig) {
	if err := ctrl.Init(cfg, "gigapipe"); err != nil {
		panic(err)
	}
	if err := ctrl.Rotate(cfg, "gigapipe"); err != nil {
		panic(err)
	}
}

// schemaStep runs the schema step unless OMIT_CREATE_TABLES is set.
func schemaStep(cfg *clconfig.ClokiConfig) {
	omit, err := boolEnv("OMIT_CREATE_TABLES")
	if err != nil {
		panic(err)
	}
	if !omit {
		createTables(cfg)
	}
}

// signalContext is cancelled by the first SIGINT or SIGTERM; after it, a second one terminates the process.
var signalContext = func() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx, stop
}

// migrateMetricsMain refuses bad migration settings before the schema step, then runs
// the schema step and one migration under a signal context and returns the exit code.
func migrateMetricsMain(cfg *clconfig.ClokiConfig, out io.Writer) int {
	migrate, err := ctrl.PrepareMetricMigration(cfg)
	if err != nil {
		return runMigrateMetrics(context.Background(), func(context.Context) ([]maintenance.MetricMigrationReport, error) {
			return nil, err
		}, out)
	}
	ctx, stop := signalContext()
	defer stop()
	if err := runSchemaStep(cfg); err != nil {
		fmt.Fprintf(out, "metric migration: schema step failed: %v; re-run MODE=%s to resume\n", err, migrateMetricsMode)
		return 1
	}
	if ctx.Err() != nil {
		fmt.Fprintf(out, "metric migration: stopped by a signal before the migration started; re-run MODE=%s to resume\n", migrateMetricsMode)
		return 1
	}
	return runMigrateMetrics(ctx, migrate, out)
}

// runSchemaStep runs schemaStep, returning its panic as an error.
func runSchemaStep(cfg *clconfig.ClokiConfig) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	schemaStep(cfg)
	return nil
}

// runMigrateMetrics runs migrate, prints each database's summary or the failure, and returns
// the exit code: 0 success, 1 failure or signal, 2 refused, 3 lease held.
func runMigrateMetrics(ctx context.Context, migrate func(context.Context) ([]maintenance.MetricMigrationReport, error), out io.Writer) int {
	reports, err := migrate(ctx)
	for _, r := range reports {
		fmt.Fprintln(out, r.Summary())
		if r.NoNewData() {
			fmt.Fprintln(out, "metric migration: no new data to migrate")
		}
	}
	if err == nil {
		return 0
	}
	var failed *maintenance.MigrationFailedError
	var refused *maintenance.MigrationRefusedError
	var held *maintenance.MigrationLeaseHeldError
	switch {
	case errors.As(err, &refused):
		fmt.Fprintln(out, refused)
		return 2
	case errors.As(err, &held):
		fmt.Fprintln(out, held)
		return 3
	case errors.As(err, &failed):
		fmt.Fprintf(out, "metric migration: %s failed at %s: %v; re-run MODE=%s to resume\n",
			failed.Database, failed.Step, failed.Err, migrateMetricsMode)
		return 1
	default:
		fmt.Fprintf(out, "metric migration: %v; re-run MODE=%s to resume\n", err, migrateMetricsMode)
		return 1
	}
}
