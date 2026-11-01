package maintenance

import (
	"context"
	"fmt"
	"os"
	"strconv"
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
	tiers, err := metricretention.FromEnv(dbObject.TTLDays, os.Getenv)
	if err != nil {
		return err
	}
	readCluster := os.Getenv("CLICKHOUSE_READ_CLUSTER")
	readSuffix := os.Getenv("CLICKHOUSE_READ_DIST_SUFFIX")
	if readSuffix == "" {
		readSuffix = "_dist"
	}
	return UpdateWithReadCluster(conn, dbObject.Name, dbObject.ClusterName, readCluster, readSuffix, mode,
		dbObject.TTLDays, dbObject.StoragePolicy, dbObject.SamplesOrdering, dbObject.SkipUnavailableShards, tiers, logger)
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
	metrics15sTTLDays, err := rollupTTLDays(dbObject)
	if err != nil {
		return err
	}
	tiers, err := metricretention.FromEnv(dbObject.TTLDays, os.Getenv)
	if err != nil {
		return err
	}
	return Rotate(connDb, dbObject.ClusterName, dbObject.ClusterName != "",
		ttlPolicy, dbObject.TTLDays, metrics15sTTLDays, tiers, dbObject.StoragePolicy, logger.Logger)
}

// rollupTTLDays is the lifetime of metrics_15s: METRICS_15S_TTL_DAYS, else the database's TTL.
func rollupTTLDays(dbObject *config.ClokiBaseDataBase) (int, error) {
	if v := os.Getenv("METRICS_15S_TTL_DAYS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("METRICS_15S_TTL_DAYS=%q: expected a positive number of days", v)
		}
		return n, nil
	}
	return dbObject.TTLDays, nil
}

// MetricMigration is a checked metric migration of each database in turn.
type MetricMigration struct {
	base       []config.ClokiBaseDataBase
	rollupDays []int
	opts       MetricMigrationOptions
}

// PrepareMetricMigration reads the MIGRATE_METRICS_* settings and checks each database,
// refusing before any connection what the run cannot migrate.
func PrepareMetricMigration(base []config.ClokiBaseDataBase) (*MetricMigration, error) {
	opts, err := MetricMigrationOptionsFromEnv(os.LookupEnv)
	if err != nil {
		return nil, err
	}
	if err := checkMigratorDSN(opts.DSN, base); err != nil {
		return nil, err
	}
	m := &MetricMigration{base: base, opts: opts}
	for _, dbObject := range base {
		if dbObject.ClusterName != "" {
			return nil, &MigrationRefusedError{
				Reason: fmt.Sprintf("database %s is on cluster %s; CLUSTER_NAME must be unset, the cluster form is not supported", dbObject.Name, dbObject.ClusterName),
			}
		}
		rollupDays, err := rollupTTLDays(&dbObject)
		if err != nil {
			return nil, &MigrationRefusedError{Reason: err.Error()}
		}
		m.rollupDays = append(m.rollupDays, rollupDays)
	}
	return m, nil
}

// Run migrates each database in turn, stopping at the first that does not succeed.
func (m *MetricMigration) Run(ctx context.Context, logger logger.ILogger) ([]MetricMigrationReport, error) {
	var reports []MetricMigrationReport
	for i := range m.base {
		report, err := migrateMetrics(ctx, &m.base[i], m.rollupDays[i], m.opts, logger)
		if err != nil {
			return reports, err
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func migrateMetrics(ctx context.Context, dbObject *config.ClokiBaseDataBase, rollupDays int, opts MetricMigrationOptions, logger logger.ILogger) (MetricMigrationReport, error) {
	conn, err := openMigratorConn(dbObject, opts)
	if err != nil {
		return MetricMigrationReport{}, &MigrationFailedError{Database: dbObject.Name, Step: "connect", Err: err}
	}
	defer conn.Close()
	opts.Database, opts.SamplesDays, opts.RollupDays, opts.Logger = dbObject.Name, dbObject.TTLDays, rollupDays, logger
	return RunMetricMigration(ctx, NewClickhouseExecutor(conn), opts)
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
