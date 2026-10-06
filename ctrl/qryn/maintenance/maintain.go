package maintenance

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/maintenance"
	"github.com/metrico/qryn/v5/shared/metricretention"
)

func upgradeDB(dbObject *config.ClokiBaseDataBase, logger logger.ILogger) error {
	conn, err := maintenance.ConnectV2(dbObject, true)
	defer conn.Close()
	if err != nil {
		return err
	}
	mode := CLUST_MODE_SINGLE
	if dbObject.Cloud {
		mode = CLUST_MODE_CLOUD
	}
	if dbObject.ClusterName != "" {
		mode |= CLUST_MODE_DISTRIBUTED
	}
	if dbObject.TTLDays == 0 {
		return fmt.Errorf("ttl_days should be set for node#%s", dbObject.Node)
	}
	readCluster := os.Getenv("CLICKHOUSE_READ_CLUSTER")
	readSuffix := os.Getenv("CLICKHOUSE_READ_DIST_SUFFIX")
	if readSuffix == "" {
		readSuffix = "_dist"
	}
	return UpdateWithReadCluster(conn, dbObject.Name, dbObject.ClusterName, readCluster, readSuffix, mode,
		dbObject.TTLDays, dbObject.StoragePolicy, dbObject.SamplesOrdering, dbObject.SkipUnavailableShards, logger)
}

func InitDB(dbObject *config.ClokiBaseDataBase, logger logger.ILogger) error {
	if dbObject.Name == "" || dbObject.Name == "default" {
		return nil
	}
	conn, err := maintenance.ConnectV2(dbObject, false)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = maintenance.InitDBTry(conn, dbObject.ClusterName, dbObject.Name, dbObject.Cloud, logger)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	rows, err := conn.Query(ctx, fmt.Sprintf("SHOW CREATE DATABASE `%s`", dbObject.Name))
	if err != nil {
		return err
	}
	defer rows.Close()
	rows.Next()
	var create string
	err = rows.Scan(&create)
	if err != nil {
		return err
	}
	logger.Info(create)
	return nil
}

func TestDistributed(dbObject *config.ClokiBaseDataBase, logger logger.ILogger) (bool, error) {
	if dbObject.ClusterName == "" {
		return false, nil
	}
	conn, err := maintenance.ConnectV2(dbObject, true)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	onCluster := "ON CLUSTER `" + dbObject.ClusterName + "`"
	logger.Info("TESTING Distributed table support")
	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS dtest %s (a UInt64) Engine = Null", onCluster)
	logger.Info(q)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	err = conn.Exec(ctx, q)
	if err != nil {
		return false, err
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE dtest %s", onCluster))
	q = fmt.Sprintf("CREATE TABLE IF NOT EXISTS dtest_dist %s (a UInt64) Engine = Distributed('%s', '%s', 'dtest', a)",
		onCluster, dbObject.ClusterName, dbObject.Name)
	logger.Info(q)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	err = conn.Exec(ctx, q)
	if err != nil {
		logger.Error("Distributed creation error: ", err.Error())
		if strings.Contains(err.Error(), "Only tables with a Replicated engine or tables which do not store data on disk are allowed in a Replicated database") {
			logger.Info("Probably CH Cloud DEV. No Dist support.")
			return false, nil
		}
		return false, err
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE dtest_dist %s", onCluster))
	logger.Info("Distributed support ok")
	return true, nil
}

func rotateDB(dbObject *config.ClokiBaseDataBase) error {
	connDb, err := maintenance.ConnectV2(dbObject, true)
	if err != nil {
		return err
	}
	defer connDb.Close()
	ttlPolicy := make([]RotatePolicy, len(dbObject.TTLPolicy))
	for i, p := range dbObject.TTLPolicy {
		d, err := time.ParseDuration(p.Timeout)
		if err != nil {
			return err
		}
		ttlPolicy[i] = RotatePolicy{
			TTL:    d,
			MoveTo: p.MoveTo,
		}
	}
	tiers, err := metricTiers(dbObject)
	if err != nil {
		return err
	}
	return Rotate(connDb, dbObject.ClusterName, dbObject.ClusterName != "",
		ttlPolicy, dbObject.TTLDays, rollupTTLDays(dbObject), tiers, dbObject.StoragePolicy, logger.Logger)
}

// metricTiers is the tier lifetimes of dbObject under the configured metric retention settings.
func metricTiers(dbObject *config.ClokiBaseDataBase) (metricretention.Tiers, error) {
	return metricretention.Configured().Tiers(dbObject.TTLDays)
}

// rollupTTLDays is the lifetime of metrics_15s: METRICS_15S_TTL_DAYS, else the database's TTL.
func rollupTTLDays(dbObject *config.ClokiBaseDataBase) int {
	return metricretention.Configured().Rollup(dbObject.TTLDays)
}

// ImportAllMetrics runs the metric import of each database in the background until it completes.
func ImportAllMetrics(base []config.ClokiBaseDataBase, logger logger.ILogger) {
	for _, dbObject := range base {
		go func() {
			conn, err := maintenance.ConnectV2ReadTimeout(&dbObject, true, time.Hour)
			if err != nil {
				logger.Error("metric import: ", err.Error())
				return
			}
			defer conn.Close()
			RunMetricImport(context.Background(), conn, MetricImportOptions{
				Database:    dbObject.Name,
				Cluster:     dbObject.ClusterName,
				SamplesDays: dbObject.TTLDays,
				RollupDays:  rollupTTLDays(&dbObject),
				Logger:      logger,
			})
		}()
	}
}

func effectivePort(db *config.ClokiBaseDataBase) uint32 {
	if db.HttpPort != 0 {
		return db.HttpPort
	}
	return db.Port
}

func UpgradeAll(config []config.ClokiBaseDataBase, logger logger.ILogger) error {
	for _, dbObject := range config {
		logger.Info(fmt.Sprintf("Upgrading %s:%d/%s", dbObject.Host, effectivePort(&dbObject), dbObject.Name))
		err := upgradeDB(&dbObject, logger)
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Upgrading %s:%d/%s: OK", dbObject.Host, effectivePort(&dbObject), dbObject.Name))
	}
	return nil
}

func RotateAll(base []config.ClokiBaseDataBase, logger logger.ILogger) error {
	for _, dbObject := range base {
		logger.Info(fmt.Sprintf("Rotating %s:%d/%s", dbObject.Host, effectivePort(&dbObject), dbObject.Name))
		err := rotateDB(&dbObject)
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Rotating %s:%d/%s: OK", dbObject.Host, effectivePort(&dbObject), dbObject.Name))
	}

	return nil
}
