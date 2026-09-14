package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
)

// metrics15sMVBody is the canonical materialized view feeding metrics_15s,
// identical to the latest version in log.sql. The WHERE token filters metrics
// out of the aggregation when it is disabled; log rows keep flowing either
// way, so the LogQL shortcut stays accelerated.
const metrics15sMVBody = `CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_15s_mv {{.OnCluster}} TO metrics_15s
AS SELECT
    fingerprint,
    intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
    argMaxState(value, samples.timestamp_ns) as last,
    maxSimpleState(value) as max,
    minSimpleState(value) as min,
    countState() as count,
    sumSimpleState(value) as sum,
    sumSimpleState(length(string)) as bytes,
    type
FROM samples_v3 as samples
{{.Where}}
GROUP BY fingerprint, timestamp_ns, type`

// metrics15sDisabledMarker is the 'metrics enabled since' value stored while
// metric aggregation is disabled: 2100-01-01, far enough that no query window
// qualifies and small enough that the reader's second-to-nanosecond conversion
// cannot overflow int64.
const metrics15sDisabledMarker = "4102444800"

// SyncMetrics15s reconciles metric aggregation into metrics_15s with the
// enabled flag.
//
// Disabled: recreates the MV with a type != 2 filter so metrics stop flowing
// into the aggregate, and deletes the accumulated metric rows. Readers serve
// metric queries from raw samples; log aggregation is unaffected.
//
// Enabled again: recreates the unfiltered MV and stores the enable time in
// the settings table; readers use the aggregate only for metric query windows
// starting after that time, since older buckets were never materialized.
func SyncMetrics15s(db clickhouse.Conn, dbname string, clusterName string,
	distributed bool, enabled bool, lg logger.ILogger) error {
	exists, err := tableExists(db, "metrics_15s")
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	state, err := getSetting(db, distributed, "metrics_15s", "state")
	if err != nil {
		return err
	}

	env := map[string]string{"DB": dbname, "OnCluster": " ", "Where": ""}
	if clusterName != "" {
		env["OnCluster"] = "ON CLUSTER `" + clusterName + "`"
	}
	exec := getDBExec(db, env, lg)

	recreateMV := func() error {
		if err := exec(`DROP TABLE IF EXISTS {{.DB}}.metrics_15s_mv {{.OnCluster}}`); err != nil {
			return err
		}
		return exec(metrics15sMVBody)
	}

	switch {
	case !enabled && state != "disabled":
		lg.Info("metrics_15s: metric aggregation disabled, filtering metrics out of the MV")
		env["Where"] = "WHERE samples.type != 2"
		if err := recreateMV(); err != nil {
			return err
		}
		if err := db.Exec(context.Background(), fmt.Sprintf(
			"ALTER TABLE `%s`.metrics_15s %s DELETE WHERE type = 2", dbname, env["OnCluster"])); err != nil {
			return err
		}
		if err := putSetting(db, "update", "metrics_15s", metrics15sDisabledMarker); err != nil {
			return err
		}
		return putSetting(db, "metrics_15s", "state", "disabled")
	case enabled && state == "disabled":
		lg.Info("metrics_15s: metric aggregation re-enabled")
		if err := recreateMV(); err != nil {
			return err
		}
		now := strconv.FormatInt(time.Now().Unix(), 10)
		if err := putSetting(db, "update", "metrics_15s", now); err != nil {
			return err
		}
		return putSetting(db, "metrics_15s", "state", "enabled")
	}
	return nil
}
