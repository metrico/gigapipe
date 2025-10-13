package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v4/ctrl/logger"
	"github.com/metrico/qryn/v4/ctrl/qryn/helputils"
)

func getSetting(db clickhouse.Conn, dist bool, tp string, name string) (string, error) {
	fp := helputils.FingerprintLabelsDJBHashPrometheus(
		fmt.Appendf(nil, `{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name)),
	)
	settings := "settings"
	if dist {
		settings += "_dist"
	}
	rows, err := db.Query(context.Background(),
		fmt.Sprintf(`SELECT argMax(value, inserted_at) as _value FROM %s WHERE fingerprint = $1 
GROUP BY fingerprint HAVING argMax(name, inserted_at) != ''`, settings), fp)
	if err != nil {
		return "", err
	}
	res := ""
	for rows.Next() {
		err = rows.Scan(&res)
		if err != nil {
			return "", err
		}
	}
	return res, nil
}

func putSetting(db clickhouse.Conn, tp string, name string, value string) error {
	_name := fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name))
	fp := helputils.FingerprintLabelsDJBHashPrometheus([]byte(_name))
	err := db.Exec(context.Background(), `INSERT INTO settings (fingerprint, type, name, value, inserted_at)
VALUES ($1, $2, $3, $4, NOW())`, fp, tp, name, value)
	return err
}

func rotateTables(db clickhouse.Conn, clusterName string, distributed bool, days []RotatePolicy, minTTL time.Duration,
	insertTimeExpression string, dropTTLExpression, settingName string,
	logger logger.ILogger, tables ...string) error {
	var rotateTTLArr []string
	for _, rp := range days {
		intsevalSec := int32(rp.TTL.Seconds())
		if intsevalSec < int32(minTTL.Seconds()) {
			intsevalSec = int32(minTTL.Seconds())
		}
		rotateTTL := fmt.Sprintf("%s + toIntervalSecond(%d)",
			insertTimeExpression,
			intsevalSec)
		if rp.MoveTo != "" {
			rotateTTL += fmt.Sprintf(" TO DISK '%s'", rp.MoveTo)
		}
		rotateTTLArr = append(rotateTTLArr, rotateTTL)
	}
	rotateTTLArr = append(rotateTTLArr, dropTTLExpression)
	rotateTTLStr := strings.Join(rotateTTLArr, ", ")

	onCluster := ""
	if clusterName != "" {
		onCluster = fmt.Sprintf(" ON CLUSTER `%s` ", clusterName)
	}

	val, err := getSetting(db, distributed, "rotate", settingName)
	if err != nil || val == rotateTTLStr {
		return err
	}
	for _, table := range tables {
		q := fmt.Sprintf(`ALTER TABLE %s %s
MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192`, table, onCluster)
		logger.Debug(q)
		err = db.Exec(context.Background(), q)
		if err != nil {
			return fmt.Errorf("query: %s\nerror: %v", q, err)
		}
		logger.Debug("Request OK")
		logger.Debug(q)
		q = fmt.Sprintf(`ALTER TABLE %s %s MODIFY TTL %s`, table, onCluster, rotateTTLStr)
		err = db.Exec(context.Background(), q)
		if err != nil {
			return fmt.Errorf("query: %s\nerror: %v", q, err)
		}
		logger.Debug("Request OK")
	}
	return putSetting(db, "rotate", settingName, rotateTTLStr)
}

func storagePolicyUpdate(db clickhouse.Conn, clusterName string,
	distributed bool, storagePolicy string, setting string, tables ...string) error {
	onCluster := ""
	if clusterName != "" {
		onCluster = fmt.Sprintf(" ON CLUSTER `%s` ", clusterName)
	}
	val, err := getSetting(db, distributed, "rotate", setting)
	if err != nil || storagePolicy == "" || val == storagePolicy {
		return err
	}
	for _, tbl := range tables {
		err = db.Exec(context.Background(), fmt.Sprintf(`ALTER TABLE %s %s MODIFY SETTING storage_policy=$1`,
			tbl, onCluster), storagePolicy)
		if err != nil {
			return err
		}
	}
	return putSetting(db, "rotate", setting, storagePolicy)
}

type RotatePolicy struct {
	TTL    time.Duration
	MoveTo string
}

func Rotate(db clickhouse.Conn, clusterName string, distributed bool, days []RotatePolicy, dropTTLDays int,
	storagePolicy string, logger logger.ILogger) error {
	//TODO: add pluggable extension
	err := storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "v3_storage_policy",
		"time_series", "time_series_gin", "samples_v3")
	if err != nil {
		return err
	}
	err = storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "v1_traces_storage_policy",
		"tempo_traces", "tempo_traces_attrs_gin", "tempo_traces_kv")
	if err != nil {
		return err
	}
	err = storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "metrics_15s", "metrics_15s")
	if err != nil {
		return err
	}

	logDefaultTTLString := func(column string) string {
		return fmt.Sprintf(
			"%s + toIntervalDay(%d)",
			column, dropTTLDays)
	}

	tracesDefaultTTLString := func(column string) string {
		return fmt.Sprintf(
			"%s + toIntervalDay(%d)",
			column, dropTTLDays)
	}

	minTTL := time.Minute
	dayTTL := time.Hour * 24

	err = rotateTables(
		db,
		clusterName,
		distributed,
		days,
		minTTL,
		"toDateTime(timestamp_ns / 1000000000)",
		logDefaultTTLString("toDateTime(timestamp_ns / 1000000000)"),
		"v3_samples_days", logger, "samples_v3")
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		dayTTL,
		"date",
		logDefaultTTLString("date"), "v3_time_series_days", logger,
		"time_series", "time_series_gin")
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		minTTL,
		"toDateTime(timestamp_ns / 1000000000)",
		tracesDefaultTTLString("toDateTime(timestamp_ns / 1000000000)"),
		"v1_traces_days",
		logger, "tempo_traces")
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		dayTTL,
		"date",
		tracesDefaultTTLString("date"), "tempo_attrs_v1",
		logger, "tempo_traces_attrs_gin", "tempo_traces_kv")
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		minTTL,
		"toDateTime(timestamp_ns / 1000000000)",
		logDefaultTTLString("toDateTime(timestamp_ns / 1000000000)"),
		"metrics_15s",
		logger, "metrics_15s")
	if err != nil {
		return err
	}

	err = rotateTables(db, clusterName, distributed, days,
		minTTL,
		"toDateTime(timestamp_10m * 600)",
		logDefaultTTLString("toDateTime(timestamp_10m * 600)"),
		"patterns",
		logger, "patterns")
	if err != nil {
		return err
	}

	return nil
}
