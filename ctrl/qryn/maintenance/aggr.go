package maintenance

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/shared/samplesconfig"
)

// metricsAggrMVQuery is not part of the versioned migration on purpose. A
// versioned script runs once, so it cannot express enabling, disabling or
// re-intervalling the view; SyncMetricsAggrMV drives it from stored state
// instead, the same way rotateTables drives TTL.
const metricsAggrMVQuery = `CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_aggr_mv {{.OnCluster}} TO {{.DB}}.metrics_aggr
AS SELECT
    fingerprint,
    intDiv(samples.timestamp_ns, {{.AGGR_INTERVAL_NS}}) * {{.AGGR_INTERVAL_NS}} as timestamp_ns,
    argMaxState(value, samples.timestamp_ns) as last,
    maxSimpleState(value) as max,
    minSimpleState(value) as min,
    countState() as count,
    sumSimpleState(value) as sum,
    type
FROM {{.DB}}.samples_metrics as samples
GROUP BY fingerprint, timestamp_ns, type`

const aggrSettingType = "aggr"
const aggrSettingName = "metrics_aggr_mv"

func aggrStateString(enabled bool, interval time.Duration) string {
	return fmt.Sprintf("enabled=%v interval=%d", enabled, interval.Nanoseconds())
}

// parseAggrInterval reads the bucket width back out of a stored state string.
// An unrecognised string reports absent, so the caller recreates the view rather
// than validating a change against a granularity it cannot confirm.
func parseAggrInterval(state string) (time.Duration, bool) {
	for _, field := range strings.Fields(state) {
		raw, ok := strings.CutPrefix(field, "interval=")
		if !ok {
			continue
		}
		ns, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || ns <= 0 {
			return 0, false
		}
		return time.Duration(ns), true
	}
	return 0, false
}

// checkIntervalChange reports whether re-bucketing metrics_aggr from oldIv to
// newIv keeps the rows already stored readable.
//
// Reads re-bucket the stored aggregate states with intDiv(timestamp_ns, step) *
// step, so old rows fold onto a coarser grid only when newIv is a whole
// multiple of oldIv. Otherwise the table ends up holding buckets coarser than
// the interval the read path assumes, and a step at the new width finds a whole
// old bucket's aggregate in its first sub-bucket and nothing in the rest.
func checkIntervalChange(oldIv, newIv time.Duration) error {
	if oldIv <= 0 || oldIv == newIv {
		return nil
	}
	if newIv%oldIv != 0 {
		return fmt.Errorf(
			"METRICS_AGGR_INTERVAL changed from %s to %s, which is not a whole multiple: "+
				"rows already in metrics_aggr would be coarser than the new interval and "+
				"would read back wrong. Either pick a multiple of %s, or clear the table "+
				"first (TRUNCATE TABLE metrics_aggr) and optionally rebuild it from "+
				"samples_metrics",
			oldIv, newIv, oldIv)
	}
	return nil
}

// aggrDropDays resolves METRICS_AGGR_DAYS, where 0 means inherit the samples
// retention. Only the caller holds the db config that carries the fallback.
func aggrDropDays(configured, fallback int) int {
	if configured <= 0 {
		return fallback
	}
	return configured
}

// parseAggrEnabled reads the enabled flag back out of a stored state string.
// ok is false for an absent or unrecognised string, which the caller reads as
// "no prior state" rather than "was disabled".
func parseAggrEnabled(state string) (enabled bool, ok bool) {
	for _, field := range strings.Fields(state) {
		raw, found := strings.CutPrefix(field, "enabled=")
		if !found {
			continue
		}
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return false, false
		}
		return v, true
	}
	return false, false
}

// SyncMetricsAggrMV brings metrics_aggr_mv in line with the configuration,
// doing nothing when it already matches. It must run after the split tables
// exist, since a materialized view needs both its source and its target.
func SyncMetricsAggrMV(db clickhouse.Conn, dbname, clusterName string,
	distributed, replicated bool, logger logger.ILogger) error {
	if !samplesconfig.SplitBySignal() {
		return nil
	}
	interval := samplesconfig.MetricsAggrInterval()
	want := aggrStateString(samplesconfig.MetricsAggrEnabled(), interval)
	have, err := getSetting(db, distributed, aggrSettingType, aggrSettingName)
	if err != nil || have == want {
		return err
	}
	// Validate against whatever width the stored rows were built at, whether or
	// not the view was enabled at the time: the rows outlive the view.
	if oldIv, ok := parseAggrInterval(have); ok {
		if err := checkIntervalChange(oldIv, interval); err != nil {
			return err
		}
	}

	env := scriptEnv(dbname, clusterName, "", "", 0, replicated, false)
	exec := getDBExec(db, env, logger)
	if err := exec("DROP VIEW IF EXISTS {{.DB}}.metrics_aggr_mv {{.OnCluster}}"); err != nil {
		return err
	}
	if samplesconfig.MetricsAggrEnabled() {
		if wasEnabled, ok := parseAggrEnabled(have); !ok || !wasEnabled {
			logger.Info(
				"metrics_aggr_mv re-enabled: metrics_aggr holds nothing for the period " +
					"it was absent (or has never run); queries over it will read that " +
					"period as empty until it is rebuilt from samples_metrics")
		}
		logger.Info(fmt.Sprintf("Creating metrics_aggr_mv at %s buckets", interval))
		if err := exec(metricsAggrMVQuery); err != nil {
			return err
		}
	} else {
		logger.Info("metrics_aggr_mv disabled; metric queries will read samples_metrics")
	}
	return putSetting(db, aggrSettingType, aggrSettingName, want)
}
