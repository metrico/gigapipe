package maintenance

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/maintenance"
	"github.com/metrico/qryn/v5/shared/envalias"
)

const migrateMetricsEnv = "MIGRATE_METRICS_"

// MetricMigrationOptionsFromEnv reads the MIGRATE_METRICS_* variables, each also accepted with
// the GIGAPIPE_ prefix, which wins when both are set. An unset variable leaves its field zero,
// for withDefaults to fill. A malformed value returns a *MigrationRefusedError.
func MetricMigrationOptionsFromEnv(lookup func(string) (string, bool)) (MetricMigrationOptions, error) {
	r := envReader{lookup: lookup, names: map[string]string{}}
	var o MetricMigrationOptions
	r.duration("LOOKBACK", &o.Lookback)
	r.duration("MARGIN", &o.Margin)
	r.integer("SPLIT", &o.Split)
	r.integer("MAX_SPLIT", &o.MaxSplit)
	r.integer("MAX_THREADS", &o.MaxThreads)
	r.bytes("MAX_MEMORY", &o.MaxMemory)
	r.duration("MAX_EXECUTION_TIME", &o.MaxExecutionTime)
	r.bytes("EXTERNAL_SORT", &o.ExternalSort)
	r.bytes("EXTERNAL_GROUP_BY", &o.ExternalGroupBy)
	r.bytes("MAX_SPILL", &o.MaxSpill)
	r.duration("PAUSE", &o.Pause)
	r.fraction("BACKOFF_PARTS", &o.BackoffParts)
	r.integer("BACKOFF_MERGES", &o.BackoffMerges)
	r.fraction("BACKOFF_MEMORY", &o.BackoffMemory)
	if r.err != nil {
		return MetricMigrationOptions{}, r.err
	}
	if d := o.withDefaults(); d.MaxSplit < d.Split {
		return MetricMigrationOptions{}, &MigrationRefusedError{Reason: fmt.Sprintf(
			"%s: expected at least %s", r.setting("MAX_SPLIT", d.MaxSplit), r.setting("SPLIT", d.Split))}
	}
	return o, nil
}

type envReader struct {
	lookup func(string) (string, bool)
	names  map[string]string
	err    error
}

// setting shows a value under the variable it was read from, or marked as the default.
func (r *envReader) setting(name string, value any) string {
	if n, ok := r.names[name]; ok {
		return fmt.Sprintf("%s=%v", n, value)
	}
	return fmt.Sprintf("%s%s=%v (default)", migrateMetricsEnv, name, value)
}

// value returns the variable's value and the name it was read under; empty counts as unset.
func (r *envReader) value(name string) (string, string, bool) {
	if r.err != nil {
		return "", "", false
	}
	for _, n := range []string{envalias.Prefix + migrateMetricsEnv + name, migrateMetricsEnv + name} {
		if v, ok := r.lookup(n); ok && v != "" {
			r.names[name] = n
			return v, n, true
		}
	}
	return "", "", false
}

func (r *envReader) refuse(name, value, expected string) {
	r.err = &MigrationRefusedError{Reason: fmt.Sprintf("%s=%q: expected %s", name, value, expected)}
}

func (r *envReader) duration(name string, dst *time.Duration) {
	v, n, ok := r.value(name)
	if !ok {
		return
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		r.refuse(n, v, "a positive duration such as 30s, 5m or 1h")
		return
	}
	*dst = d
}

func (r *envReader) integer(name string, dst *int) {
	v, n, ok := r.value(name)
	if !ok {
		return
	}
	i, err := strconv.Atoi(v)
	if err != nil || i <= 0 {
		r.refuse(n, v, "a positive integer")
		return
	}
	*dst = i
}

func (r *envReader) bytes(name string, dst *uint64) {
	v, n, ok := r.value(name)
	if !ok {
		return
	}
	b, err := parseBytes(v)
	if err != nil || b == 0 {
		r.refuse(n, v, "a positive byte size such as 536870912, 512MiB or 2GB")
		return
	}
	*dst = b
}

func (r *envReader) fraction(name string, dst *float64) {
	v, n, ok := r.value(name)
	if !ok {
		return
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || !(f > 0 && f <= 1) {
		r.refuse(n, v, "a fraction in (0, 1] such as 0.5")
		return
	}
	*dst = f
}

var byteUnits = []struct {
	suffix string
	size   uint64
}{
	{"KiB", kib}, {"MiB", mib}, {"GiB", gib}, {"TiB", 1024 * gib},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
}

// parseBytes reads a byte count: plain bytes, or an integer with an IEC (KiB…TiB) or
// decimal (KB…TB) unit.
func parseBytes(s string) (uint64, error) {
	num, unit := s, uint64(1)
	for _, u := range byteUnits {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			num, unit = n, u.size
			break
		}
	}
	n, err := strconv.ParseUint(num, 10, 64)
	if err != nil {
		return 0, err
	}
	if n > math.MaxUint64/unit {
		return 0, fmt.Errorf("%s overflows", s)
	}
	return n * unit, nil
}

// migratorReadTimeout lets a statement run to its max_execution_time before the client gives up.
func migratorReadTimeout(o MetricMigrationOptions) time.Duration {
	return max(time.Hour, o.withDefaults().MaxExecutionTime+5*time.Minute)
}

// openMigratorConn connects with the instance's settings and the migrator's read timeout.
func openMigratorConn(dbObject *config.ClokiBaseDataBase, o MetricMigrationOptions) (clickhouse.Conn, error) {
	return maintenance.ConnectV2ReadTimeout(dbObject, true, migratorReadTimeout(o))
}
