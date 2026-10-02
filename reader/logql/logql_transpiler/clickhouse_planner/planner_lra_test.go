package clickhouse_planner

import (
	"strings"
	"sync"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

var configOnce sync.Once

func ensureConfig() {
	configOnce.Do(func() {
		if config.Cloki == nil {
			config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
		}
	})
}

// planSQL transpiles a LogQL metric query into the SQL string the planner
// would send to ClickHouse.
func planSQL(t *testing.T, query string, from, to time.Time, step time.Duration) string {
	t.Helper()
	ensureConfig()
	script, err := logql_parser.Parse(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	plan, err := Plan(script, true)
	if err != nil {
		t.Fatalf("plan %q: %v", query, err)
	}
	ctx := &shared.PlannerContext{
		From:  from,
		To:    to,
		Step:  step,
		CHSqlCtx: &sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		},
	}
	ctx.SamplesDistTableName = "samples_v3"
	ctx.TimeSeriesDistTableName = "time_series"
	ctx.TimeSeriesGinDistTableName = "time_series_gin"
	ctx.Metrics15sDistTableName = "metrics_15s"
	obj, err := plan.Process(ctx)
	if err != nil {
		t.Fatalf("process %q: %v", query, err)
	}
	res, err := obj.String(ctx.CHSqlCtx)
	if err != nil {
		t.Fatalf("string %q: %v", query, err)
	}
	return res
}

// bytes_over_time must return the total bytes in the range, not a per-second
// rate (that is bytes_rate).
func TestBytesOverTimeNotDividedByRange(t *testing.T) {
	from := time.Unix(0, 1759000000_000000000)
	to := from.Add(time.Hour)
	q := `sum(bytes_over_time({service_name="dayz_logz",instance="deerisle"}[1h]))`
	sqlQuery := planSQL(t, q, from, to, 15*time.Minute)

	if !strings.Contains(sqlQuery, "toFloat64(sum(length(_string)))") {
		t.Errorf("bytes_over_time value expression missing byte sum:\n%s", sqlQuery)
	}
	// The value must NOT be divided by the range length.
	if strings.Contains(sqlQuery, "toFloat64(sum(length(_string))) / ") {
		t.Errorf("bytes_over_time divided by range length (bytes_rate semantics):\n%s", sqlQuery)
	}
}

// bytes_rate keeps returning bytes per second.
func TestBytesRateStillDividedByRange(t *testing.T) {
	from := time.Unix(0, 1759000000_000000000)
	to := from.Add(time.Hour)
	q := `sum(bytes_rate({service_name="dayz_logz",instance="deerisle"}[1h]))`
	sqlQuery := planSQL(t, q, from, to, 15*time.Minute)

	if !strings.Contains(sqlQuery, "toFloat64(sum(length(_string))) / 3600") {
		t.Errorf("bytes_rate must divide by the range length in seconds:\n%s", sqlQuery)
	}
}
