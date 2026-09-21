package service

import (
	"os"
	"regexp"
	"strconv"
	"testing"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler/planner"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/storage"
)

// TestMain initializes the package-global config.Cloki once before any test in
// this package runs: DownsampleHintsPlanner.Process reads
// config.Cloki.Setting.ClokiReader.Compat_4_0_19 directly, and is otherwise nil
// in a plain `go test` of this package.
func TestMain(m *testing.M) {
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	os.Exit(m.Run())
}

// capStubProducer stands in for the per-step group-by a real Main planner hands
// DownsampleHintsPlanner: one row per (fingerprint, bucket) with placeholder
// val/timestamp_ms columns for patchField to rewrite.
type capStubProducer struct{}

func (capStubProducer) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("0", "val"),
		sql.NewSimpleCol("0", "timestamp_ms")).
		From(sql.NewRawObject("metrics_15s as samples")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")), nil
}

// TestBucketCapAgreesAcrossLayers pins the invariant that makes the two cap
// sites one decision rather than two: whatever bucket width the request layer
// settles on for a range function, the planner must bucket at exactly that and
// not silently substitute its own.
//
// deriv is the function where both caps run on the same query: it has no
// ClickHouse pushdown of its own, so it reaches DownsampleHintsPlanner, and it
// is in rateFunctions, so adjustHintsForRate has already capped its step by the
// time it gets there.
//
// At a range under 30s the two disagree. adjustHintsForRate floors its cap at
// 15s because metrics_15s rows are stamped on a 15s grid
// (ctrl/qryn/maintenance/metrics15s.go), so a finer bucket cannot hold a second
// row; DownsampleHintsPlanner's own cap has no floor and overrides that back
// down to range/2, asking ClickHouse for buckets narrower than the table's own
// resolution.
func TestBucketCapAgreesAcrossLayers(t *testing.T) {
	hints := &storage.SelectHints{Func: "deriv", Range: 20000, Step: 3600000}

	c := &CLokiQuerier{}
	c.adjustHintsForRate(hints)

	p := &planner.DownsampleHintsPlanner{Main: capStubProducer{}, Hints: hints}
	req, err := p.Process(&shared.PlannerContext{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := req.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}

	// What this pins is the width, so it reads the width back out of the
	// rendered column rather than matching the column's whole text. How a
	// bucket is keyed -- by the floor of its samples' timestamps or by the
	// ceiling -- is a separate rule, owned by the planner and guarded there;
	// spelling the full expression out here would pin that too, by accident,
	// and this test would fail for a change it has no opinion about.
	m := regexp.MustCompile(`intDiv\(samples\.timestamp_ns[^,]*, (\d+)[^)]*\) \* (\d+)`).FindStringSubmatch(got)
	if m == nil {
		t.Fatalf("no bucket column in the rendered query:\n%s", got)
	}
	divisorNs, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	multiplierMs, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if divisorNs != hints.Step*1000000 || multiplierMs != hints.Step {
		t.Errorf("request layer settled on step=%dms, planner must bucket at that width.\n"+
			"want: divisor %dns, multiplier %dms\ngot:  divisor %dns, multiplier %dms\n%s",
			hints.Step, hints.Step*1000000, hints.Step, divisorNs, multiplierMs, got)
	}
}

// TestAdjustHintsForRate covers what the request layer settles the step to, for
// each shape that reaches it.
//
// The cases that matter for routing: useRawData in transpileLabelMatchers
// rejects any step under 15000, the metrics_15s grid. A change function whose
// range is small enough that half of it falls under that grid cannot be served
// from the downsampled table at all -- no bucket width on a table stamped every
// 15s can put two distinct samples inside a 20s window -- so the cap dropping
// the step below the grid is what routes it to raw samples, where the engine
// computes it from real timestamps instead.
func TestAdjustHintsForRate(t *testing.T) {
	const grid = int64(15000)
	for _, tc := range []struct {
		name     string
		fn       string
		step, ry int64
		want     int64
		wantRaw  bool
	}{
		{"change function coarser than half its range is capped", "rate", 3600000, 300000, 150000, false},
		{"change function already fine enough is left alone", "rate", 30000, 300000, 30000, false},
		{"irate is a change function too", "irate", 3600000, 300000, 150000, false},
		{"idelta is a change function too", "idelta", 3600000, 300000, 150000, false},
		{"range too small for the 15s grid routes to raw", "deriv", 3600000, 20000, 10000, true},
		{"reducers keep the query's own step", "sum_over_time", 3600000, 300000, 3600000, false},
		{"unaccelerated functions keep the query's own step", "quantile_over_time", 3600000, 300000, 3600000, false},
		{"instant query has no step of its own", "rate", 0, 300000, 150000, false},
		{"bare instant query falls back to the grid", "", 0, 0, 15000, false},
		{"change function over a subquery reports no range", "rate", 3600000, 0, 15000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hints := &storage.SelectHints{Func: tc.fn, Step: tc.step, Range: tc.ry}
			(&CLokiQuerier{}).adjustHintsForRate(hints)
			if hints.Step != tc.want {
				t.Errorf("step: got %d, want %d", hints.Step, tc.want)
			}
			if gotRaw := hints.Step < grid; gotRaw != tc.wantRaw {
				t.Errorf("routes to raw samples: got %v, want %v (step %d vs grid %d)",
					gotRaw, tc.wantRaw, hints.Step, grid)
			}
		})
	}
}
