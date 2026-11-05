package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// metrics15sMVBody is the materialized view feeding metrics_15s. The WHERE
// token holds metrics15sFilter while metric aggregation is disabled; log rows
// keep flowing either way, so the LogQL shortcut stays accelerated. The
// StartCount token holds metrics15sStartCountCol while metrics_15s has the
// column.
const metrics15sMVBody = `CREATE MATERIALIZED VIEW IF NOT EXISTS {{.DB}}.metrics_15s_mv {{.OnCluster}} TO metrics_15s
AS SELECT
    fingerprint,
    intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
    argMaxState(value, samples.timestamp_ns) as last,
    maxSimpleState(value) as max,
    minSimpleState(value) as min,
    countState() as count,
    sumSimpleState(value) as sum,
    sumSimpleState(length(string)) as bytes,{{.StartCount}}
    type
FROM samples_v3 as samples
{{.Where}}
GROUP BY fingerprint, timestamp_ns, type`

// metrics15sStartCountCol counts the lines exactly on the cell start.
const metrics15sStartCountCol = `
    sumSimpleState(toUInt64(samples.timestamp_ns % 15000000000 = 0)) as start_count,`

// metrics15sFilter keeps metric rows (type 2) out of the aggregation.
const metrics15sFilter = "WHERE samples." + metrics15sFilterCond

// metrics15sFilterCond is the condition metrics15sFilter puts on the MV.
const metrics15sFilterCond = "type != 2"

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

	onCluster := metrics15sEnv(dbname, clusterName)["OnCluster"]
	swapMV := func(where string) error { return swapMetrics15sMV(db, dbname, clusterName, where, lg) }

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
			"ALTER TABLE `%s`.metrics_15s %s DELETE WHERE type = 2", dbname, onCluster)); err != nil {
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

// metrics15sEnv is the template env of the MV statements.
func metrics15sEnv(dbname, clusterName string) map[string]string {
	env := map[string]string{"DB": dbname, "OnCluster": " ", "Where": "", "StartCount": ""}
	if clusterName != "" {
		env["OnCluster"] = "ON CLUSTER `" + clusterName + "`"
	}
	return env
}

// RecoverMetrics15sMV leaves one MV feeding metrics_15s after an interrupted
// swap: a lone metrics_15s_mv_bak is renamed to metrics_15s_mv, a second one
// is dropped, and with neither, the MV is created from metrics15sMVBody once
// metrics_15s has the type column.
func RecoverMetrics15sMV(db clickhouse.Conn, dbname string, clusterName string,
	distributed bool, lg logger.ILogger) error {
	exists, err := tableExists(db, "metrics_15s")
	if err != nil || !exists {
		return err
	}
	mv, err := tableExists(db, "metrics_15s_mv")
	if err != nil {
		return err
	}
	bak, err := tableExists(db, "metrics_15s_mv_bak")
	if err != nil {
		return err
	}
	env := metrics15sEnv(dbname, clusterName)
	exec := getDBExec(db, env, lg)
	switch {
	case mv && bak:
		lg.Info("metrics_15s: dropping the second MV, metrics_15s_mv_bak")
		return exec(`DROP TABLE IF EXISTS {{.DB}}.metrics_15s_mv_bak {{.OnCluster}}`)
	case bak:
		lg.Info("metrics_15s: renaming metrics_15s_mv_bak to metrics_15s_mv")
		return exec(`RENAME TABLE {{.DB}}.metrics_15s_mv_bak TO metrics_15s_mv {{.OnCluster}}`)
	case !mv:
		// Without type, the MV is still the log scripts' to create.
		hasType, err := metrics15sHasColumn(db, dbname, "type")
		if err != nil || !hasType {
			return err
		}
		lg.Info("metrics_15s: creating the missing MV")
		filtered, err := metrics15sKeepsFilter(db, distributed, "")
		if err != nil {
			return err
		}
		if filtered {
			env["Where"] = metrics15sFilter
		}
		if err := metrics15sEnvStartCount(db, dbname, env); err != nil {
			return err
		}
		return exec(metrics15sMVBody)
	}
	return nil
}

// metrics15sEnvStartCount fills env's StartCount token while metrics_15s has
// the start_count column.
func metrics15sEnvStartCount(db clickhouse.Conn, dbname string, env map[string]string) error {
	hasColumn, err := metrics15sHasColumn(db, dbname, "start_count")
	if hasColumn {
		env["StartCount"] = metrics15sStartCountCol
	}
	return err
}

// swapMetrics15sMV replaces the MV with metrics15sMVBody via rename: the old
// MV keeps aggregating until the new one exists, so overlapping inserts are
// double-counted rather than lost.
func swapMetrics15sMV(db clickhouse.Conn, dbname string, clusterName string, where string,
	lg logger.ILogger) error {
	if err := RecoverMetrics15sMV(db, dbname, clusterName, clusterName != "", lg); err != nil {
		return err
	}
	env := metrics15sEnv(dbname, clusterName)
	env["Where"] = where
	if err := metrics15sEnvStartCount(db, dbname, env); err != nil {
		return err
	}
	exec := getDBExec(db, env, lg)
	if err := exec(`RENAME TABLE {{.DB}}.metrics_15s_mv TO metrics_15s_mv_bak {{.OnCluster}}`); err != nil {
		return err
	}
	if err := exec(metrics15sMVBody); err != nil {
		return err
	}
	return exec(`DROP TABLE IF EXISTS {{.DB}}.metrics_15s_mv_bak {{.OnCluster}}`)
}

// metrics15sMVFiltered reports whether the current MV definition carries the
// metric filter. The MV is identical on every shard, so the local node is
// representative.
func metrics15sMVFiltered(db clickhouse.Conn, dbname string) (bool, error) {
	create, err := metrics15sMVCreate(db, dbname)
	return strings.Contains(create, metrics15sFilterCond), err
}

// metrics15sMVCreate returns the local node's metrics_15s_mv definition, or
// "" when there is none.
func metrics15sMVCreate(db clickhouse.Conn, dbname string) (string, error) {
	rows, err := db.Query(context.Background(),
		"SELECT create_table_query FROM system.tables WHERE database = $1 AND name = 'metrics_15s_mv'",
		dbname)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var create string
	for rows.Next() {
		if err := rows.Scan(&create); err != nil {
			return "", err
		}
	}
	return create, rows.Err()
}

// metrics15sStartCountMarker is the 'update' setting holding the unix time of
// the first cell whose start_count the MV fills on every node; readers load it
// as dbversion.MarkerMetrics15sStartCount.
const metrics15sStartCountMarker = "metrics_15s_start_count"

// metrics15sCellNs is the width of a metrics_15s cell.
const metrics15sCellNs = int64(15 * time.Second)

// startCountMargin puts the start_count cutoff this far past the newest
// stored cell and the server clock.
const startCountMargin = time.Minute

// RecordMetrics15sStartCount records startCountCutoff once metrics_15s has the
// column and keeps it; a missing MV, or one without start_count, is swapped for
// metrics15sMVBody and gets newestCellCutoff. A cutoff not readable off every
// shard waits for the next run.
func RecordMetrics15sStartCount(db clickhouse.Conn, dbname string, clusterName string,
	distributed bool, lg logger.ILogger) error {
	hasColumn, err := metrics15sHasColumn(db, dbname, "start_count")
	if err != nil || !hasColumn {
		return err
	}
	create, err := metrics15sMVCreate(db, dbname)
	if err != nil {
		return err
	}
	var cutoff int64
	if !strings.Contains(create, "start_count") {
		lg.Info("metrics_15s: the MV lacks start_count, swapping it")
		if err := clearSetting(db, "update", metrics15sStartCountMarker); err != nil {
			return err
		}
		where := ""
		if filtered, err := metrics15sKeepsFilter(db, distributed, create); err != nil {
			return err
		} else if filtered {
			where = metrics15sFilter
		}
		if err := swapMetrics15sMV(db, dbname, clusterName, where, lg); err != nil {
			return err
		}
		cutoff, err = newestCellCutoff(db, distributed)
	} else {
		var recorded string
		if recorded, err = getSetting(db, distributed, "update", metrics15sStartCountMarker); err != nil || recorded != "" {
			return err
		}
		cutoff, err = startCountCutoff(db, distributed)
	}
	if err != nil {
		lg.Error("metrics_15s: start_count cutoff deferred: ", err.Error())
		return nil
	}
	lg.Info(fmt.Sprintf("metrics_15s: start_count from %s", time.Unix(0, cutoff).UTC().Format(time.RFC3339)))
	return putSetting(db, "update", metrics15sStartCountMarker, strconv.FormatInt(cutoff/1000000000, 10))
}

// metrics15sKeepsFilter reports whether the MV filters metric rows: from its
// definition create, or from the aggregation state when there is none.
func metrics15sKeepsFilter(db clickhouse.Conn, distributed bool, create string) (bool, error) {
	if create != "" {
		return strings.Contains(create, metrics15sFilterCond), nil
	}
	state, err := getSetting(db, distributed, "metrics_15s", "state")
	return state == "disabled", err
}

// metrics15sTable is the metrics_15s table read across every shard.
func metrics15sTable(distributed bool) string {
	if distributed {
		return "metrics_15s" + distconfig.Suffix()
	}
	return "metrics_15s"
}

// startCountCutoff is 0 while metrics_15s holds no row, newestCellCutoff
// otherwise.
func startCountCutoff(db clickhouse.Conn, distributed bool) (int64, error) {
	var rows uint64
	if err := db.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT count() FROM %s SETTINGS skip_unavailable_shards = 0", metrics15sTable(distributed))).Scan(&rows); err != nil {
		return 0, err
	}
	if rows == 0 {
		return 0, nil
	}
	return newestCellCutoff(db, distributed)
}

// newestCellCutoff is the cell boundary startCountMargin past the later of the
// newest cell and the server clock, read off every shard.
func newestCellCutoff(db clickhouse.Conn, distributed bool) (int64, error) {
	var newest, now int64
	// The newest cell is taken among those from a day before the clock on.
	if err := db.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT max(timestamp_ns), toUnixTimestamp64Nano(now64(9)) FROM %s "+
			"WHERE timestamp_ns >= toUnixTimestamp64Nano(now64(9)) - %d SETTINGS skip_unavailable_shards = 0",
		metrics15sTable(distributed), (24*time.Hour).Nanoseconds())).Scan(&newest, &now); err != nil {
		return 0, err
	}
	return (max(newest, now)+startCountMargin.Nanoseconds())/metrics15sCellNs*metrics15sCellNs + metrics15sCellNs, nil
}

// metrics15sHasColumn reports whether the local metrics_15s has the column.
func metrics15sHasColumn(db clickhouse.Conn, dbname string, column string) (bool, error) {
	var n uint64
	err := db.QueryRow(context.Background(),
		"SELECT count() FROM system.columns WHERE database = $1 AND table = 'metrics_15s' AND name = $2",
		dbname, column).Scan(&n)
	return n > 0, err
}
