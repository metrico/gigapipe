package promql_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/promql/promql_parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"testing"
	"time"
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
