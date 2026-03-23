package distconfig

import (
	"os"
	"sync"
)

var (
	distSuffix string
	once       sync.Once
)

// Init sets the distributed table suffix. Call once at startup.
func Init() {
	once.Do(func() {
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
