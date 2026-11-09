package distconfig

import (
	"fmt"
	"os"
	"sync"
)

var (
	distSuffix  string
	readCluster string
	once        sync.Once
)

// Init sets the distributed table suffix and the read cluster. Call once at startup.
func Init() {
	once.Do(func() {
		readCluster = os.Getenv("CLICKHOUSE_READ_CLUSTER")
		if v := os.Getenv("CLICKHOUSE_READ_DIST_SUFFIX"); v != "" {
			distSuffix = v
		} else {
			distSuffix = "_dist"
		}
	})
}

// Suffix returns the distributed table suffix (e.g. "_dist").
func Suffix() string {
	return distSuffix
}

// ReadCluster returns the cluster the read-path distributed tables span when
// it is set apart from the home cluster; empty otherwise.
func ReadCluster() string {
	return readCluster
}

// CrossClusterWarning returns the start-up warning for a custom read suffix
// with no read cluster, or "" when none applies. An empty suffix means Init
// has not run.
func CrossClusterWarning(suffix, readCluster string) string {
	if suffix == "" || suffix == "_dist" || readCluster != "" {
		return ""
	}
	return fmt.Sprintf("CLICKHOUSE_READ_DIST_SUFFIX=%q is set without CLICKHOUSE_READ_CLUSTER: "+
		"structural queries, compare() and TraceQL metrics with no spanset operator (&&, ||, structural), scalar filter or by() stage will read only the local cluster (CLUSTER_NAME); a lone {} reads every cluster except for rate() and count_over_time() and exemplars other than histogram_over_time()'s under a custom by label, and the range series of a quantile_over_time() of several levels. "+
		"Set CLICKHOUSE_READ_CLUSTER to the cluster the read tables span.", suffix)
}
