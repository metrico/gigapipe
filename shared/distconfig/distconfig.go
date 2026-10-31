package distconfig

import (
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
