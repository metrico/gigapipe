package maintenance

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

// recordingConn records every statement Rotate executes and answers every query with no rows.
type recordingConn struct {
	driver.Conn
	execs []recordedExec
}

type recordedExec struct {
	query string
	args  []any
}

func (c *recordingConn) Exec(_ context.Context, query string, args ...any) error {
	c.execs = append(c.execs, recordedExec{strings.Join(strings.Fields(query), " "), args})
	return nil
}

func (c *recordingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return noRows{}, nil
}

type noRows struct{ driver.Rows }

func (noRows) Next() bool   { return false }
func (noRows) Close() error { return nil }
func (noRows) Err() error   { return nil }

// rotateOnce runs Rotate on a database without rotate settings and returns what it executed.
func rotateOnce(t *testing.T, days []RotatePolicy, storagePolicy string) []recordedExec {
	t.Helper()
	conn := &recordingConn{}
	if err := Rotate(conn, "", false, days, 7, 7, testTiers, storagePolicy, logger.Logger); err != nil {
		t.Fatal(err)
	}
	return conn.execs
}

// altersOf returns the ALTER statements Rotate executed on table.
func altersOf(execs []recordedExec, table string) []string {
	var res []string
	for _, e := range execs {
		if strings.HasPrefix(e.query, "ALTER TABLE "+table+" ") {
			res = append(res, e.query)
		}
	}
	return res
}

func TestRotateAppliesTheRetentionTiersToTheMetricTables(t *testing.T) {
	execs := rotateOnce(t, nil, "")
	for table, ttl := range map[string]string{
		"metric_samples":     "toDateTime(timestamp) + toIntervalDay(3)",
		"metric_exemplars":   "toDateTime(timestamp) + toIntervalDay(3)",
		"metrics_5m":         "toDateTime(bucket) + toIntervalDay(14)",
		"metrics_1h":         "toDateTime(bucket) + toIntervalDay(90)",
		"metric_series":      "toDateTime(last_seen) + toIntervalDay(90)",
		"metric_label_names": "toDateTime(last_seen) + toIntervalDay(90)",
	} {
		stmts := altersOf(execs, table)
		if !slices.Contains(stmts, "ALTER TABLE "+table+" MODIFY TTL "+ttl) {
			t.Errorf("%s: no MODIFY TTL %s in %q", table, ttl, stmts)
		}
	}
	if stmts := altersOf(execs, "metric_metadata"); len(stmts) != 0 {
		t.Errorf("metric_metadata is given a TTL: %q", stmts)
	}
}

func TestRotateSetsThePartSettingsOfTheMetricTables(t *testing.T) {
	execs := rotateOnce(t, nil, "")
	const merge = "merge_with_ttl_timeout = 3600, index_granularity = 8192"
	for table, settings := range map[string]string{
		"metric_samples":     "ttl_only_drop_parts = 1, " + merge,
		"metric_exemplars":   "ttl_only_drop_parts = 1, " + merge,
		"metrics_5m":         "ttl_only_drop_parts = 1, " + merge,
		"metrics_1h":         "ttl_only_drop_parts = 1, " + merge,
		"metric_series":      merge,
		"metric_label_names": merge,
	} {
		stmts := altersOf(execs, table)
		if !slices.Contains(stmts, "ALTER TABLE "+table+" MODIFY SETTING "+settings) {
			t.Errorf("%s: no MODIFY SETTING %s in %q", table, settings, stmts)
		}
	}
}

func TestRotateAppliesTheMoveRulesToTheMetricTables(t *testing.T) {
	execs := rotateOnce(t, []RotatePolicy{{TTL: 24 * time.Hour, MoveTo: "cold"}}, "")
	want := "ALTER TABLE metrics_5m MODIFY TTL toDateTime(bucket) + toIntervalSecond(86400) TO DISK 'cold', " +
		"toDateTime(bucket) + toIntervalDay(14)"
	if stmts := altersOf(execs, "metrics_5m"); !slices.Contains(stmts, want) {
		t.Errorf("metrics_5m: no %q in %q", want, stmts)
	}
}

func TestOneStoragePolicySettingCoversEveryMetricTable(t *testing.T) {
	execs := rotateOnce(t, nil, "cold_policy")
	for _, table := range metricDataTables {
		stmts := altersOf(execs, table)
		if !slices.Contains(stmts, "ALTER TABLE "+table+" MODIFY SETTING storage_policy=$1") {
			t.Errorf("%s: no storage policy in %q", table, stmts)
		}
	}
	var recorded []string
	for _, e := range execs {
		if strings.HasPrefix(e.query, "INSERT INTO settings ") && len(e.args) == 4 && e.args[3] == "cold_policy" {
			recorded = append(recorded, e.args[2].(string))
		}
	}
	metric := slices.DeleteFunc(slices.Clone(recorded), func(name string) bool { return !strings.Contains(name, "metric") })
	if !slices.Equal(metric, []string{"metric_storage_policy"}) {
		t.Errorf("metric storage-policy settings = %q, want [metric_storage_policy]", metric)
	}
}

func TestNewMetricTablesTakeTheStoragePolicyAtCreation(t *testing.T) {
	env := migrationEnv("cloki", "", false, 7, "cold_policy", "", false)
	scripts, err := renderScripts(sql.MetricsScript, env)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range metricDataTables {
		if s := statement(t, scripts, table); !strings.HasSuffix(strings.TrimSuffix(s, ";"), "SETTINGS storage_policy = 'cold_policy'") {
			t.Errorf("%s is created without the storage policy:\n%s", table, s)
		}
	}
}
