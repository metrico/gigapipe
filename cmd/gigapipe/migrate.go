package main

import (
	"errors"
	"fmt"
	"io"
	"slices"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/ctrl"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
)

const migrateMetricsMode = "migrate_metrics"

// runsSchemaStep reports whether mode creates and upgrades the tables on start.
func runsSchemaStep(mode string) bool {
	return slices.Contains([]string{"all", "writer", "init_only", migrateMetricsMode}, mode)
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

// runMigrateMetrics runs migrate, prints each database's summary or the failure, and returns
// the exit code: 0 success, 1 failure, 2 refused, 3 lease held.
func runMigrateMetrics(migrate func() ([]maintenance.MetricMigrationReport, error), out io.Writer) int {
	reports, err := migrate()
	for _, r := range reports {
		fmt.Fprintln(out, r.Summary())
	}
	if err == nil {
		return 0
	}
	var failed *maintenance.MigrationFailedError
	var refused *maintenance.MigrationRefusedError
	var held *maintenance.MigrationLeaseHeldError
	switch {
	case errors.As(err, &refused):
		fmt.Fprintf(out, "metric migration: %v\n", err)
		return 2
	case errors.As(err, &held):
		fmt.Fprintf(out, "metric migration: %v\n", err)
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
