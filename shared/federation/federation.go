// Package federation gates the optional multi-tenant ("federated") mode behind
// the FEDERATED environment variable. When enabled, every logs/metrics/rules
// ClickHouse table carries a non-empty `oid` tenancy column, writes require an
// X-Scope-OrgID header, and reads are constrained to the requesting tenant(s).
//
// It mirrors shared/distconfig: a process-wide flag resolved once at startup so
// ctrl, writer, reader and ruler can consult it without threading config
// through every call site or modifying the external cloki-config module.
package federation

import (
	"os"
	"slices"
	"strings"
	"sync"
)

var (
	enabled bool
	once    sync.Once
)

// parseBool mirrors cmd/gigapipe/main.go:boolEnv truthy values. Unknown values
// are treated as false so a typo can never silently enable tenancy.
func parseBool(v string) bool {
	return slices.Contains([]string{"true", "1", "yes", "y"}, strings.ToLower(strings.TrimSpace(v)))
}

// Init resolves the FEDERATED flag from the environment. Call once at startup,
// before any subsystem consults Enabled().
func Init() {
	once.Do(func() {
		enabled = parseBool(os.Getenv("FEDERATED"))
	})
}

// Enabled reports whether federated (multi-tenant) mode is on.
func Enabled() bool {
	return enabled
}
