package clickhouse_planner

import (
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func mockFingerprints() sql.ISelect {
	return sql.NewSelect().
		Select(sql.NewSimpleCol("fingerprint", "fingerprint")).
		From(sql.NewSimpleCol("time_series_gin", "gin"))
}

func exemplarsSQL(t *testing.T, isCluster bool) string {
	t.Helper()
	planner := &ExemplarsPlanner{Fingerprints: staticPlanner{mockFingerprints()}}
	req, err := planner.Process(&shared.PlannerContext{
		IsCluster:               isCluster,
		From:                    time.Unix(1700000000, 0),
		To:                      time.Unix(1700003600, 0),
		Type:                    shared.SAMPLES_TYPE_METRICS,
		SamplesDistTableName:    "samples_v3",
		TimeSeriesDistTableName: "time_series",
	})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	str, err := req.String(newCtx())
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return str
}

// An exemplar is a metric sample whose string column is not empty. That filter
// is the entire selectivity of this query -- without it the planner reads
// every metric sample in the window and calls them all exemplars.
func TestExemplarsPlannerSelectsOnlySamplesCarryingATraceID(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		got := exemplarsSQL(t, cluster)
		if !strings.Contains(got, "(string) != ('')") {
			t.Errorf("cluster=%v: no trace-id filter in:\n%s", cluster, got)
		}
	}
}

func TestExemplarsPlannerBoundsTheTimeWindow(t *testing.T) {
	got := exemplarsSQL(t, false)
	for _, want := range []string{
		"(timestamp_ns) >= (1700000000000000000)",
		"(timestamp_ns) < (1700003600000000000)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

// The service groups rows into series as it streams them, which is only
// correct while rows of one series arrive together.
func TestExemplarsPlannerOrdersByFingerprintSoSeriesStayContiguous(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		got := exemplarsSQL(t, cluster)
		idx := strings.Index(got, "ORDER BY")
		if idx < 0 {
			t.Fatalf("cluster=%v: no ORDER BY in:\n%s", cluster, got)
		}
		if !strings.Contains(got[idx:], "fingerprint") {
			t.Errorf("cluster=%v: ORDER BY does not lead with fingerprint:\n%s", cluster, got[idx:])
		}
	}
}

// Many exemplars map onto one series. ANY INNER JOIN keeps a single pair per
// key, which would return the first exemplar of each series and silently drop
// the rest -- the failure this join type exists to avoid.
func TestExemplarsPlannerKeepsEveryExemplarOfASeries(t *testing.T) {
	got := exemplarsSQL(t, false)
	if !strings.Contains(strings.ToUpper(got), "ANY LEFT") {
		t.Errorf("expected ANY LEFT join, got:\n%s", got)
	}
	if strings.Contains(strings.ToUpper(got), "ANY INNER") {
		t.Errorf("ANY INNER JOIN would drop all but one exemplar per series:\n%s", got)
	}
}

// The fingerprint bound must stay a plain IN even on a cluster.
//
// `fp_sel` reads the local time_series_gin table. Sharding is by fingerprint,
// so each shard resolves it correctly for its own samples. GLOBAL IN would
// evaluate the subquery once on the initiator, against only the initiator's
// local gin rows, and silently drop every series living on another shard --
// no error, just missing exemplars. Caught end to end on the two-shard e2e
// cluster, where it returned three of four series.
func TestExemplarsPlannerDoesNotGlobalInTheLocalGinSubquery(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		got := exemplarsSQL(t, cluster)
		if strings.Contains(got, "GLOBAL IN") {
			t.Errorf("cluster=%v: GLOBAL IN resolves the local gin subquery on the initiator only:\n%s",
				cluster, got)
		}
		if !strings.Contains(got, "fingerprint IN (fp_sel)") {
			t.Errorf("cluster=%v: fingerprints not bound to fp_sel by a plain IN:\n%s", cluster, got)
		}
	}
}

// Cluster mode must not need a different query shape here. Nothing in this
// planner joins two distributed tables through an IN, which is the thing
// distributed_product_mode=deny rejects with error 288, so a single path is
// correct for both and there is no second shape to keep in step.
func TestExemplarsPlannerUsesOneShapeForClusterAndSingleNode(t *testing.T) {
	if single, cluster := exemplarsSQL(t, false), exemplarsSQL(t, true); single != cluster {
		t.Errorf("cluster and single-node SQL diverged:\n--- single ---\n%s\n--- cluster ---\n%s",
			single, cluster)
	}
}

// Logs live in the same samples table, so without the type filter a log line
// would be returned as a metric exemplar.
func TestExemplarsPlannerRestrictsToMetricSeries(t *testing.T) {
	got := exemplarsSQL(t, false)
	if !strings.Contains(got, "type IN") {
		t.Errorf("no sample-type filter in:\n%s", got)
	}
}
