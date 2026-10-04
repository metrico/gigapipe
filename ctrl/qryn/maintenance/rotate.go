package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/helputils"
	"github.com/metrico/qryn/v5/shared/distconfig"
	"github.com/metrico/qryn/v5/shared/metricretention"
)

func getSetting(db clickhouse.Conn, dist bool, tp string, name string) (string, error) {
	fp := helputils.FingerprintLabelsDJBHashPrometheus(
		fmt.Appendf(nil, `{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name)),
	)
	settings := "settings"
	if dist {
		settings += distconfig.Suffix()
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

// tableRotation is the TTL Rotate keeps on a group of tables.
type tableRotation struct {
	setting        string
	minTTL         time.Duration
	insertTime     string
	dropTTL        string
	dropWholeParts bool
	tables         []string
}

// statements renders the TTL the rotation applies and the ALTERs applying it.
func (r tableRotation) statements(clusterName string, days []RotatePolicy) (string, []string) {
	var rotateTTLArr []string
	for _, rp := range days {
		intsevalSec := int32(rp.TTL.Seconds())
		if intsevalSec < int32(r.minTTL.Seconds()) {
			intsevalSec = int32(r.minTTL.Seconds())
		}
		rotateTTL := fmt.Sprintf("%s + toIntervalSecond(%d)",
			r.insertTime,
			intsevalSec)
		if rp.MoveTo != "" {
			rotateTTL += fmt.Sprintf(" TO DISK '%s'", rp.MoveTo)
		}
		rotateTTLArr = append(rotateTTLArr, rotateTTL)
	}
	rotateTTLArr = append(rotateTTLArr, r.dropTTL)
	rotateTTLStr := strings.Join(rotateTTLArr, ", ")

	onCluster := ""
	if clusterName != "" {
		onCluster = fmt.Sprintf(" ON CLUSTER `%s` ", clusterName)
	}
	settings := "merge_with_ttl_timeout = 3600, index_granularity = 8192"
	if r.dropWholeParts {
		settings = "ttl_only_drop_parts = 1, " + settings
	}
	var stmts []string
	for _, table := range r.tables {
		stmts = append(stmts,
			fmt.Sprintf("ALTER TABLE %s %s\nMODIFY SETTING %s", table, onCluster, settings),
			fmt.Sprintf("ALTER TABLE %s %s MODIFY TTL %s", table, onCluster, rotateTTLStr))
	}
	return rotateTTLStr, stmts
}

func rotateTables(db clickhouse.Conn, clusterName string, distributed bool, days []RotatePolicy, minTTL time.Duration,
	insertTimeExpression string, dropTTLExpression, settingName string,
	logger logger.ILogger, tables ...string) error {
	return rotate(db, clusterName, distributed, days, tableRotation{
		setting:        settingName,
		minTTL:         minTTL,
		insertTime:     insertTimeExpression,
		dropTTL:        dropTTLExpression,
		dropWholeParts: true,
		tables:         tables,
	}, logger)
}

func rotate(db clickhouse.Conn, clusterName string, distributed bool, days []RotatePolicy, r tableRotation,
	logger logger.ILogger) error {
	rotateTTLStr, stmts := r.statements(clusterName, days)
	val, err := getSetting(db, distributed, "rotate", r.setting)
	if err != nil || val == rotateTTLStr {
		return err
	}
	for _, q := range stmts {
		logger.Debug(q)
		err = db.Exec(context.Background(), q)
		if err != nil {
			return fmt.Errorf("query: %s\nerror: %v", q, err)
		}
		logger.Debug("Request OK")
	}
	return putSetting(db, "rotate", r.setting, rotateTTLStr)
}

// metricStoringTables are the metric tables that hold data.
var metricStoringTables = []string{
	"metric_samples", "metric_exemplars", "metric_series", "metric_metadata", "metrics_5m", "metrics_1h",
	"metric_label_names",
}

// metricRotations lists the TTL of each metric table that expires, from the retention tiers.
func metricRotations(tiers metricretention.Tiers) []tableRotation {
	dropAfter := func(column string, days int) string {
		return fmt.Sprintf("%s + toIntervalDay(%d)", column, days)
	}
	return []tableRotation{
		{
			setting:        "metric_samples_days",
			minTTL:         time.Minute,
			insertTime:     "toDateTime(timestamp)",
			dropTTL:        dropAfter("toDateTime(timestamp)", tiers.RawDays),
			dropWholeParts: true,
			tables:         []string{"metric_samples", "metric_exemplars"},
		},
		{
			setting:        "metrics_5m_days",
			minTTL:         time.Minute,
			insertTime:     "toDateTime(bucket)",
			dropTTL:        dropAfter("toDateTime(bucket)", tiers.FiveMinuteDays),
			dropWholeParts: true,
			tables:         []string{"metrics_5m"},
		},
		{
			setting:        "metrics_1h_days",
			minTTL:         time.Minute,
			insertTime:     "toDateTime(bucket)",
			dropTTL:        dropAfter("toDateTime(bucket)", tiers.HourDays),
			dropWholeParts: true,
			tables:         []string{"metrics_1h"},
		},
		{
			setting:    "metric_series_days",
			minTTL:     time.Minute,
			insertTime: "toDateTime(last_seen)",
			dropTTL:    dropAfter("toDateTime(last_seen)", tiers.HourDays),
			tables:     []string{"metric_series"},
		},
		{
			setting:    "metric_label_names_days",
			minTTL:     time.Minute,
			insertTime: "toDateTime(last_seen)",
			dropTTL:    dropAfter("toDateTime(last_seen)", tiers.HourDays),
			tables:     []string{"metric_label_names"},
		},
	}
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
	metrics15sTTLDays int, tiers metricretention.Tiers, storagePolicy string, logger logger.ILogger) error {
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
	err = storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "metric_storage_policy",
		metricStoringTables...)
	if err != nil {
		return err
	}
	metrics15sExists, err := tableExists(db, "metrics_15s")
	if err != nil {
		return err
	}
	if metrics15sExists {
		err = storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "metrics_15s", "metrics_15s")
		if err != nil {
			return err
		}
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
	if metrics15sExists {
		err = rotateTables(db, clusterName, distributed, days,
			minTTL,
			"toDateTime(timestamp_ns / 1000000000)",
			fmt.Sprintf("toDateTime(timestamp_ns / 1000000000) + toIntervalDay(%d)", metrics15sTTLDays),
			"metrics_15s",
			logger, "metrics_15s")
		if err != nil {
			return err
		}
	}

	for _, r := range metricRotations(tiers) {
		err = rotate(db, clusterName, distributed, days, r, logger)
		if err != nil {
			return err
		}
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
