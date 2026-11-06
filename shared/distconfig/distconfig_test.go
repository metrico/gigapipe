package distconfig

import "testing"

func TestCrossClusterWarningNamesTheSettingAndWhatReadsLocally(t *testing.T) {
	want := `CLICKHOUSE_READ_DIST_SUFFIX="_dist_reads" is set without CLICKHOUSE_READ_CLUSTER: ` +
		`structural queries, compare() and TraceQL metrics with no spanset operator (&&, ||, structural), scalar filter or by() stage will read only the local cluster (CLUSTER_NAME). ` +
		`Set CLICKHOUSE_READ_CLUSTER to the cluster the read tables span.`
	if got := CrossClusterWarning("_dist_reads", ""); got != want {
		t.Errorf("CrossClusterWarning = %q, want %q", got, want)
	}
}

func TestCrossClusterWarningStaysSilent(t *testing.T) {
	for _, c := range []struct{ suffix, readCluster string }{
		{"_dist", ""},
		{"_dist", "reads"},
		{"_dist_reads", "reads"},
		{"", ""},
		{"", "reads"},
	} {
		if got := CrossClusterWarning(c.suffix, c.readCluster); got != "" {
			t.Errorf("suffix %q, read cluster %q: warning = %q, want none", c.suffix, c.readCluster, got)
		}
	}
}
