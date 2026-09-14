package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
)

// metrics15sMVBody is the materialized view feeding metrics_15s. The WHERE
// token holds metrics15sFilter while metric aggregation is disabled; log rows
// keep flowing either way, so the LogQL shortcut stays accelerated.
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

// metrics15sFilter keeps metric rows (type 2) out of the aggregation.
const metrics15sFilter = "WHERE samples.type != 2"

// metrics15sDisabledMarker is the 'metrics enabled since' value stored while
// metric aggregation is disabled: 2100-01-01, so no query window qualifies.
const metrics15sDisabledMarker = "4102444800"

// readerMarkerWait covers the readers' version-info cache TTL: after the
// disabled marker is written, every reader routes metric queries to raw
// samples within this long.
const readerMarkerWait = 15 * time.Second

// SyncMetrics15s reconciles metric aggregation into metrics_15s with the
// enabled flag: disabling filters metrics out of the MV, deletes the stored
// metric rows and marks the aggregation unavailable to readers; re-enabling
// restores the unfiltered MV and marks the aggregation available from now on.
// Log aggregation into the same table is unaffected.
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

	// swapMV replaces the MV via rename: the old MV keeps aggregating until
	// the new one exists, so overlapping inserts are double-counted rather
	// than lost.
	swapMV := func(where string) error {
		env["Where"] = where
		if err := exec(`DROP TABLE IF EXISTS {{.DB}}.metrics_15s_mv_bak {{.OnCluster}}`); err != nil {
			return err
		}
		mvExists, err := tableExists(db, "metrics_15s_mv")
		if err != nil {
			return err
		}
		if mvExists {
			if err := exec(`RENAME TABLE {{.DB}}.metrics_15s_mv TO metrics_15s_mv_bak {{.OnCluster}}`); err != nil {
				return err
			}
		}
		if err := exec(metrics15sMVBody); err != nil {
			return err
		}
		return exec(`DROP TABLE IF EXISTS {{.DB}}.metrics_15s_mv_bak {{.OnCluster}}`)
	}

	switch {
	case !enabled && state != "disabled":
		lg.Info("metrics_15s: metric aggregation disabled, filtering metrics out of the MV")
		// Readers must stop trusting the aggregate before its rows disappear:
		// marker first, then the filter, then the delete once every reader
		// cache has expired.
		if err := putSetting(db, "update", "metrics_15s", metrics15sDisabledMarker); err != nil {
			return err
		}
		if err := swapMV(metrics15sFilter); err != nil {
			return err
		}
		time.Sleep(readerMarkerWait)
		if err := db.Exec(context.Background(), fmt.Sprintf(
			"ALTER TABLE `%s`.metrics_15s %s DELETE WHERE type = 2", dbname, env["OnCluster"])); err != nil {
			return err
		}
		return putSetting(db, "metrics_15s", "state", "disabled")
	case enabled && state == "disabled":
		lg.Info("metrics_15s: metric aggregation re-enabled")
		if err := swapMV(""); err != nil {
			return err
		}
		now := strconv.FormatInt(time.Now().Unix(), 10)
		if err := putSetting(db, "update", "metrics_15s", now); err != nil {
			return err
		}
		return putSetting(db, "metrics_15s", "state", "enabled")
	case !enabled && state == "disabled":
		// A migration recreating the MV would drop the filter; reapply it.
		filtered, err := metrics15sMVFiltered(db, dbname)
		if err != nil {
			return err
		}
		if !filtered {
			lg.Info("metrics_15s: reapplying the metric filter to the MV")
			return swapMV(metrics15sFilter)
		}
	}
	return nil
}

// metrics15sMVFiltered reports whether the current MV definition carries the
// metric filter. The MV is identical on every shard, so the local node is
// representative.
func metrics15sMVFiltered(db clickhouse.Conn, dbname string) (bool, error) {
	rows, err := db.Query(context.Background(),
		"SELECT create_table_query FROM system.tables WHERE database = $1 AND name = 'metrics_15s_mv'",
		dbname)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var create string
	for rows.Next() {
		if err := rows.Scan(&create); err != nil {
			return false, err
		}
	}
	return strings.Contains(create, "type != 2"), nil
}
