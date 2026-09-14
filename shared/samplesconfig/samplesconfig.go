// Package samplesconfig carries the samples-storage layout: whether logs and
// metrics live in separate tables, and how the metrics preaggregate is built.
//
// It is a standalone env-backed package rather than fields on the main config
// because ctrl, writer and reader all need these values, while the config
// struct itself lives in an external module.
package samplesconfig

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

// logsAggrInterval is the bucket width of the logs preaggregate. It stays fixed
// because the LogQL shortcut planner and the stored logs_aggr rows were built
// against it; only the metrics side is tunable so far.
const logsAggrInterval = 15 * time.Second

var (
	splitBySignal bool
	aggrEnabled   = true
	aggrInterval  = logsAggrInterval
	aggrDays      int
	metricsOrder  string
	initErr       error

	once sync.Once
)

// Init reads the environment once at startup. Call it before anything reads a
// getter, and before tables.InitDistTableNames. Every call reports the result
// of the single load, so a later caller cannot mistake a failed load for a
// successful one.
func Init() error {
	once.Do(func() { initErr = load() })
	return initErr
}

// Reload re-reads the environment, bypassing the once guard. It exists for
// tests, which need to exercise both layouts in one process.
func Reload() error { return load() }

func load() error {
	var err error
	if splitBySignal, err = boolEnv("SAMPLES_SPLIT_BY_SIGNAL", false); err != nil {
		return err
	}
	if aggrEnabled, err = boolEnv("METRICS_AGGR_ENABLED", true); err != nil {
		return err
	}
	aggrInterval = logsAggrInterval
	if v := os.Getenv("METRICS_AGGR_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("METRICS_AGGR_INTERVAL: %w", err)
		}
		// Whole seconds only. The view buckets on nanoseconds while the read
		// path snaps the step grid on milliseconds, so a width that is not a
		// round number of milliseconds would have the two disagree, and a
		// sub-millisecond one would truncate to a zero divisor.
		if d < time.Second || d%time.Second != 0 {
			return fmt.Errorf("METRICS_AGGR_INTERVAL must be a whole number of seconds, got %s", v)
		}
		aggrInterval = d
	}
	aggrDays = 0
	if v := os.Getenv("METRICS_AGGR_DAYS"); v != "" {
		d, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("METRICS_AGGR_DAYS: %w", err)
		}
		if d < 0 {
			return fmt.Errorf("METRICS_AGGR_DAYS must not be negative, got %s", v)
		}
		aggrDays = d
	}
	metricsOrder = os.Getenv("ADVANCED_METRICS_ORDERING")
	return nil
}

func boolEnv(key string, def bool) (bool, error) {
	val := os.Getenv(key)
	if val == "" {
		return def, nil
	}
	if slices.Contains([]string{"true", "1", "yes", "y"}, val) {
		return true, nil
	}
	if slices.Contains([]string{"false", "0", "no", "n"}, val) {
		return false, nil
	}
	return false, fmt.Errorf("%s value must be one of [no, n, false, 0, yes, y, true, 1]", key)
}

// SplitBySignal reports whether logs and metrics are stored apart.
func SplitBySignal() bool { return splitBySignal }

// MetricsAggrEnabled reports whether metrics_aggr_mv should exist. It is
// meaningful only under SplitBySignal; without the split the shared metrics_15s
// view is always present.
func MetricsAggrEnabled() bool { return aggrEnabled }

// MetricsAggrAvailable reports whether a metrics preaggregate exists to read
// from. This is the question the read path asks, and it differs from
// MetricsAggrEnabled precisely when the split is off.
func MetricsAggrAvailable() bool { return !splitBySignal || aggrEnabled }

// MetricsAggrInterval is the metrics bucket width. It is the configured value
// whether or not the aggregate is enabled, so the read path's step arithmetic
// stays on one grid either way.
func MetricsAggrInterval() time.Duration { return aggrInterval }

// MetricsAggrIntervalMs is MetricsAggrInterval in milliseconds, the unit
// prometheus SelectHints use.
func MetricsAggrIntervalMs() int64 { return aggrInterval.Milliseconds() }

// LogsAggrInterval is the logs bucket width, fixed at 15s.
func LogsAggrInterval() time.Duration { return logsAggrInterval }

// MetricsAggrDays is the metrics_aggr retention in days; 0 means inherit the
// samples retention, which only the caller holding the db config can resolve.
func MetricsAggrDays() int { return aggrDays }

// MetricsOrdering is the ORDER BY rule for samples_metrics; empty means inherit
// the samples ordering.
func MetricsOrdering() string { return metricsOrder }

// SamplesTable names the raw samples table for one signal.
func SamplesTable(metrics bool) string {
	if !splitBySignal {
		return "samples_v3"
	}
	if metrics {
		return "samples_metrics"
	}
	return "samples_logs"
}

// AggrTable names the preaggregate table for one signal.
func AggrTable(metrics bool) string {
	if !splitBySignal {
		return "metrics_15s"
	}
	if metrics {
		return "metrics_aggr"
	}
	return "logs_aggr"
}
