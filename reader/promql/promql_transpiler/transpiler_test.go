package promql_transpiler

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/promql/promql_parser"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestTranspilerV2(t *testing.T) {
	script, err := promql_parser.Parse("sum by (a) (rate(http_requests_total{job=\"myjob\"}[1m]))")

	if err != nil {
		t.Fatal(err)
	}
	script, err = TranspileExpressionV2(script)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range script.Substitutes {
		req, err := v.Request.Process(&shared.PlannerContext{
			IsCluster:               false,
			From:                    time.Now(),
			To:                      time.Now().Add(time.Minute * -5),
			TimeSeriesGinTableName:  "time_series_gin",
			SamplesTableName:        "samples_v3",
			TimeSeriesTableName:     "time_series",
			TimeSeriesDistTableName: "time_series",
			Metrics15sTableName:     "metrics_15s",
		})
		if err != nil {
			t.Fatal(err)
		}
		str, err := req.String(sql.DefaultCtx())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(str)
	}
}

func TestTranspilerV2Agg(t *testing.T) {
	script, err := promql_parser.Parse("sum by (aaa) (http_requests_total{job=\"myjob\"})")
	if err != nil {
		t.Fatal(err)
	}
	script, err = TranspileExpressionV2(script)
	if err != nil {
		t.Fatal(err)
	}
	print(script.Expr.String())
}

func TestTranspilerV1(t *testing.T) {
	ctx := &shared.PlannerContext{
		IsCluster:               false,
		From:                    time.Now(),
		To:                      time.Now().Add(time.Minute * -5),
		TimeSeriesGinTableName:  "time_series_gin",
		SamplesTableName:        "samples_v3",
		TimeSeriesTableName:     "time_series",
		TimeSeriesDistTableName: "time_series",
		Metrics15sTableName:     "metrics_15s",
		Metrics15sDistTableName: "metrics_15s",
	}
	q, err := TranspileLabelMatchersDownsample(&storage.SelectHints{
		Step:  (time.Second * 15).Milliseconds(),
		Func:  "count_over_time",
		Range: (time.Minute * 5).Milliseconds(),
	}, ctx, &labels.Matcher{
		Type:  labels.MatchEqual,
		Name:  "__name__",
		Value: "aaa",
	})
	if err != nil {
		t.Fatal(err)
	}
	strQ, err := q.Query.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(strQ)
}

func TestCountOverTime_DoesNotGenerateRange(t *testing.T) {
	// Reproduces a bug where count_over_time with a large range (e.g. 14d)
	// generates range(toInt64(val)) in ClickHouse SQL. ClickHouse's range(N)
	// creates an array [0..N-1] which is then ARRAY JOINed to expand rows.
	// For large counts (14d window at 15s scrape = ~80k elements per bucket),
	// this crashes ClickHouse or causes OOM.
	now := time.Now()
	ctx := &shared.PlannerContext{
		IsCluster:               false,
		From:                    now.Add(-14 * 24 * time.Hour),
		To:                      now,
		TimeSeriesGinTableName:  "time_series_gin",
		SamplesTableName:        "samples_v3",
		SamplesDistTableName:    "samples_v3",
		TimeSeriesTableName:     "time_series",
		TimeSeriesDistTableName: "time_series",
		Metrics15sTableName:     "metrics_15s",
		Metrics15sDistTableName: "metrics_15s",
		Step:                    time.Minute,
		Type:                    2,
	}

	hints := &storage.SelectHints{
		Step:  (time.Minute).Milliseconds(),
		Func:  "count_over_time",
		Range: (14 * 24 * time.Hour).Milliseconds(),
		Start: ctx.From.UnixMilli(),
		End:   ctx.To.UnixMilli(),
	}

	q, err := TranspileLabelMatchersDownsample(hints, ctx,
		&labels.Matcher{
			Type:  labels.MatchEqual,
			Name:  "__name__",
			Value: "start_time",
		},
		&labels.Matcher{
			Type:  labels.MatchEqual,
			Name:  "job",
			Value: "metric_exporter",
		},
	)
	if err != nil {
		t.Fatalf("TranspileLabelMatchersDownsample() returned error: %v", err)
	}

	strQ, err := q.Query.String(sql.DefaultCtx())
	if err != nil {
		t.Fatalf("Query.String() returned error: %v", err)
	}

	// The generated SQL must NOT contain range(toInt64(...)) which creates
	// arrays that can be enormous and crash ClickHouse.
	if strings.Contains(strQ, "range(toInt64") {
		t.Errorf("generated SQL contains range(toInt64(...)) which is incorrect "+
			"for count_over_time with large ranges.\n"+
			"range(N) generates an array of N elements; for 14d of data this can be "+
			"tens of thousands of elements per row, causing OOM or crashes.\n"+
			"Got SQL:\n%s", strQ)
	}

	// The generated SQL should NOT use ARRAY JOIN for count_over_time.
	// The pre-aggregated count from countMerge(count) already IS the count.
	if strings.Contains(strings.ToLower(strQ), "array join") {
		t.Errorf("generated SQL uses ARRAY JOIN which is incorrect for count_over_time.\n"+
			"The pre-aggregated count from countMerge(count) should be used directly "+
			"as the sample value rather than being expanded into individual rows.\n"+
			"Got SQL:\n%s", strQ)
	}
}
