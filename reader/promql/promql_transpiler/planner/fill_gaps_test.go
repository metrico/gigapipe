package planner

import (
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	dbversion "github.com/metrico/qryn/v4/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// stubProducer yields a grouped (fingerprint, timestamp_ms, val) select at real
// buckets, standing in for whatever a real producer (a bucket read, an inner
// aggregation) would hand FillGapsPlanner.
type stubProducer struct{}

func (stubProducer) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().Select(
		sql.NewSimpleCol("fingerprint", "fingerprint"),
		sql.NewSimpleCol("intDiv(timestamp_ns, 60000000000) * 60000", "timestamp_ms"),
		sql.NewSimpleCol("sum(sum)", "val")).
		From(sql.NewRawObject("metrics_15s")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ms")), nil
}

func fillTestCtx(staleness bool) *shared.PlannerContext {
	now := time.Unix(1700000000, 0)
	var ver dbversion.VersionInfo
	if staleness {
		ver = dbversion.VersionInfo{dbversion.CapStaleness: 0}
	}
	return &shared.PlannerContext{
		From:        now.Add(-time.Hour),
		To:          now,
		Step:        time.Minute,
		VersionInfo: ver,
	}
}

func renderFill(t *testing.T, staleness bool) string {
	t.Helper()
	p := &FillGapsPlanner{Main: stubProducer{}, Duration: 5 * time.Minute, ValueCols: []string{"val"}}
	req, err := p.Process(fillTestCtx(staleness))
	if err != nil {
		t.Fatal(err)
	}
	str, err := req.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	return str
}

// TestFillGapsPlannerStaleness wraps a producer and asserts the native STALENESS
// densification on a capable server.
func TestFillGapsPlannerStaleness(t *testing.T) {
	got := renderFill(t, true)
	if !strings.Contains(got, "WITH FILL TO 1700000000000 STEP 60000 STALENESS 300000") {
		t.Errorf("missing staleness fill:\n%s", got)
	}
	if !strings.Contains(got, "1 as source") {
		t.Errorf("real rows must be tagged source = 1:\n%s", got)
	}
}

// TestFillGapsPlannerArrayJoin wraps the same producer and asserts the arrayJoin
// range-expansion fallback on a server without STALENESS.
func TestFillGapsPlannerArrayJoin(t *testing.T) {
	got := renderFill(t, false)
	if strings.Contains(got, "STALENESS") {
		t.Errorf("STALENESS must not be emitted on an incapable server:\n%s", got)
	}
	for _, w := range []string{
		"leadInFrame(toInt64(timestamp_ms), 1, toInt64(timestamp_ms) + toInt64(300000))",
		"arrayJoin(range(bucket_ms, least(bucket_ms + toInt64(300000), next_ms, toInt64(1700000000000)), toInt64(60000)))",
		"toUInt8(timestamp_ms = bucket_ms) as source",
	} {
		if !strings.Contains(got, w) {
			t.Errorf("missing %q in arrayJoin fill:\n%s", w, got)
		}
	}
}
