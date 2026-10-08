package planner

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/storage"
)

// Under Compat_4_0_19 the keys sit 1ms before the evaluation points.
func TestDownsampleGridCompat4019(t *testing.T) {
	setting := &config.Cloki.Setting.ClokiReader.Compat_4_0_19
	saved := *setting
	t.Cleanup(func() { *setting = saved })
	for _, compat := range []bool{false, true} {
		*setting = compat
		p := &DownsampleGridPlanner{
			Fp:    &StreamSelectPlanner{LabelNames: []string{"__name__"}, Ops: []string{"="}, Values: []string{"gx"}},
			Hints: &storage.SelectHints{Step: 300_000},
			Grid:  Grid{StepMs: 300_000},
		}
		q, err := p.Process(&shared.PlannerContext{Metrics15sDistTableName: "metrics_15s", SamplesDistTableName: "samples_v3",
			TimeSeriesGinDistTableName: "time_series_gin", Type: 2})
		if err != nil {
			t.Fatal(err)
		}
		s, err := q.String(sql.DefaultCtx())
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(s, "key_ms - 1 as timestamp_ms"); got != compat {
			t.Errorf("compat %v: shifted keys %v in %s", compat, got, s)
		}
	}
}
