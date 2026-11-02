package maintenance

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/metrico/cloki-config/config"
)

func envLookup(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func TestEnvUnsetGivesTheSpecDefaults(t *testing.T) {
	opts, err := MetricMigrationOptionsFromEnv(envLookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	got := opts.withDefaults()
	want := []struct {
		name      string
		got, want any
	}{
		{"LOOKBACK", got.Lookback, time.Hour},
		{"MARGIN", got.Margin, 5 * time.Minute},
		{"SPLIT", got.Split, 1},
		{"MAX_SPLIT", got.MaxSplit, 16},
		{"MAX_THREADS", got.MaxThreads, 2},
		{"MAX_MEMORY", got.MaxMemory, uint64(2 << 30)},
		{"MAX_EXECUTION_TIME", got.MaxExecutionTime, 5 * time.Minute},
		{"EXTERNAL_SORT", got.ExternalSort, uint64(512 << 20)},
		{"EXTERNAL_GROUP_BY", got.ExternalGroupBy, uint64(512 << 20)},
		{"MAX_SPILL", got.MaxSpill, uint64(20 << 30)},
		{"PAUSE", got.Pause, time.Second},
		{"BACKOFF_PARTS", got.BackoffParts, 0.5},
		{"BACKOFF_MERGES", got.BackoffMerges, 8},
		{"BACKOFF_MEMORY", got.BackoffMemory, 0.8},
	}
	for _, w := range want {
		if w.got != w.want {
			t.Errorf("%s = %v, want %v", w.name, w.got, w.want)
		}
	}
}

func TestEnvReadsEveryVariableWithUnits(t *testing.T) {
	opts, err := MetricMigrationOptionsFromEnv(envLookup(map[string]string{
		"MIGRATE_METRICS_LOOKBACK":           "2h",
		"MIGRATE_METRICS_MARGIN":             "10m",
		"MIGRATE_METRICS_SPLIT":              "4",
		"MIGRATE_METRICS_MAX_SPLIT":          "32",
		"MIGRATE_METRICS_MAX_THREADS":        "8",
		"MIGRATE_METRICS_MAX_MEMORY":         "4GiB",
		"MIGRATE_METRICS_MAX_EXECUTION_TIME": "1h30m",
		"MIGRATE_METRICS_EXTERNAL_SORT":      "256MB",
		"MIGRATE_METRICS_EXTERNAL_GROUP_BY":  "1048576",
		"MIGRATE_METRICS_MAX_SPILL":          "1TiB",
		"MIGRATE_METRICS_PAUSE":              "250ms",
		"MIGRATE_METRICS_BACKOFF_PARTS":      "0.25",
		"MIGRATE_METRICS_BACKOFF_MERGES":     "4",
		"MIGRATE_METRICS_BACKOFF_MEMORY":     "1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := MetricMigrationOptions{
		Lookback: 2 * time.Hour, Margin: 10 * time.Minute, Split: 4, MaxSplit: 32, MaxThreads: 8,
		MaxMemory: 4 << 30, MaxExecutionTime: 90 * time.Minute, ExternalSort: 256_000_000,
		ExternalGroupBy: 1 << 20, MaxSpill: 1 << 40, Pause: 250 * time.Millisecond,
		BackoffParts: 0.25, BackoffMerges: 4, BackoffMemory: 1,
	}
	if !reflect.DeepEqual(opts, want) {
		t.Fatalf("got %+v\nwant %+v", opts, want)
	}
}

func TestEnvAcceptsTheGigapipePrefix(t *testing.T) {
	opts, err := MetricMigrationOptionsFromEnv(envLookup(map[string]string{
		"GIGAPIPE_MIGRATE_METRICS_MAX_MEMORY": "1GiB",
		"GIGAPIPE_MIGRATE_METRICS_PAUSE":      "5s",
		"MIGRATE_METRICS_PAUSE":               "9s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if opts.MaxMemory != 1<<30 {
		t.Errorf("MaxMemory = %d", opts.MaxMemory)
	}
	if opts.Pause != 5*time.Second {
		t.Errorf("Pause = %s, want the GIGAPIPE_ value", opts.Pause)
	}
}

func TestEnvRefusesMalformedValuesNamingTheVariable(t *testing.T) {
	cases := []struct {
		name, value, expect string
	}{
		{"MIGRATE_METRICS_LOOKBACK", "1 hour", "duration"},
		{"MIGRATE_METRICS_MARGIN", "0s", "positive duration"},
		{"MIGRATE_METRICS_MAX_EXECUTION_TIME", "-5m", "positive duration"},
		{"MIGRATE_METRICS_PAUSE", "0", "positive duration"},
		{"MIGRATE_METRICS_SPLIT", "two", "positive integer"},
		{"MIGRATE_METRICS_MAX_SPLIT", "0", "positive integer"},
		{"MIGRATE_METRICS_MAX_THREADS", "-1", "positive integer"},
		{"MIGRATE_METRICS_BACKOFF_MERGES", "1.5", "positive integer"},
		{"MIGRATE_METRICS_MAX_MEMORY", "2GB/s", "byte size"},
		{"MIGRATE_METRICS_EXTERNAL_SORT", "lots", "byte size"},
		{"MIGRATE_METRICS_EXTERNAL_GROUP_BY", "0MiB", "byte size"},
		{"MIGRATE_METRICS_MAX_SPILL", "99999999999TiB", "byte size"},
		{"MIGRATE_METRICS_BACKOFF_PARTS", "half", "fraction"},
		{"MIGRATE_METRICS_BACKOFF_MEMORY", "1.2", "fraction"},
		{"MIGRATE_METRICS_BACKOFF_PARTS", "0", "fraction"},
		{"GIGAPIPE_MIGRATE_METRICS_MAX_MEMORY", "2 gigs", "byte size"},
	}
	for _, c := range cases {
		_, err := MetricMigrationOptionsFromEnv(envLookup(map[string]string{c.name: c.value}))
		var refused *MigrationRefusedError
		if !errors.As(err, &refused) {
			t.Errorf("%s=%q: err %v, want refused", c.name, c.value, err)
			continue
		}
		if !strings.HasPrefix(refused.Reason, c.name) || !strings.Contains(refused.Reason, c.expect) {
			t.Errorf("%s=%q: reason %q lacks the variable or %q", c.name, c.value, refused.Reason, c.expect)
		}
	}
}

func TestEnvRefusesMaxSplitBelowSplit(t *testing.T) {
	cases := []struct {
		vars map[string]string
		want string
	}{
		{map[string]string{"MIGRATE_METRICS_SPLIT": "128"},
			"MIGRATE_METRICS_MAX_SPLIT=16 (default): expected at least MIGRATE_METRICS_SPLIT=128"},
		{map[string]string{"GIGAPIPE_MIGRATE_METRICS_MAX_SPLIT": "4", "MIGRATE_METRICS_SPLIT": "8"},
			"GIGAPIPE_MIGRATE_METRICS_MAX_SPLIT=4: expected at least MIGRATE_METRICS_SPLIT=8"},
	}
	for _, c := range cases {
		_, err := MetricMigrationOptionsFromEnv(envLookup(c.vars))
		var refused *MigrationRefusedError
		if !errors.As(err, &refused) || refused.Reason != c.want {
			t.Errorf("%v: err %v, want %q", c.vars, err, c.want)
		}
	}
}

func TestPrepareMetricMigrationRefusesABadEnv(t *testing.T) {
	t.Setenv("MIGRATE_METRICS_MAX_MEMORY", "2 gigs")
	m, err := PrepareMetricMigration([]config.ClokiBaseDataBase{{Name: "db", Host: "127.0.0.1", Port: 1}})
	var refused *MigrationRefusedError
	if !errors.As(err, &refused) || m != nil {
		t.Fatalf("migration %v, err %v", m, err)
	}
}

func TestEnvHasNoMigratorDSN(t *testing.T) {
	opts, err := MetricMigrationOptionsFromEnv(envLookup(map[string]string{
		"MIGRATE_METRICS_DSN": "clickhouse://mig:secret@",
	}))
	if err != nil || !reflect.DeepEqual(opts, MetricMigrationOptions{}) {
		t.Fatalf("opts %+v, err %v", opts, err)
	}
}

func TestMigratorReadTimeoutCoversTheExecutionLimit(t *testing.T) {
	if got := migratorReadTimeout(MetricMigrationOptions{MaxExecutionTime: 30 * time.Minute}); got != time.Hour {
		t.Errorf("30m: %s", got)
	}
	if got := migratorReadTimeout(MetricMigrationOptions{MaxExecutionTime: 2 * time.Hour}); got <= 2*time.Hour {
		t.Errorf("2h: %s", got)
	}
}

func TestEnvLimitsAndThresholdsReachTheirConsumers(t *testing.T) {
	opts, err := MetricMigrationOptionsFromEnv(envLookup(map[string]string{
		"MIGRATE_METRICS_MAX_THREADS":        "8",
		"MIGRATE_METRICS_MAX_MEMORY":         "4GiB",
		"MIGRATE_METRICS_MAX_EXECUTION_TIME": "90s",
		"MIGRATE_METRICS_EXTERNAL_SORT":      "256MB",
		"MIGRATE_METRICS_EXTERNAL_GROUP_BY":  "1048576",
		"MIGRATE_METRICS_MAX_SPILL":          "1TiB",
		"MIGRATE_METRICS_BACKOFF_PARTS":      "0.25",
		"MIGRATE_METRICS_BACKOFF_MERGES":     "4",
		"MIGRATE_METRICS_BACKOFF_MEMORY":     "0.5",
	}))
	if err != nil {
		t.Fatal(err)
	}
	opts = opts.withDefaults()
	got := (&migration{opts: opts}).limitSettings()
	for k, v := range map[string]any{
		"max_threads":                               8,
		"max_memory_usage":                          uint64(4 << 30),
		"max_execution_time":                        90,
		"max_bytes_before_external_sort":            uint64(256_000_000),
		"max_bytes_before_external_group_by":        uint64(1 << 20),
		"max_temporary_data_on_disk_size_for_query": uint64(1 << 40),
	} {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	g := newGatedExecutor(nil, opts).(*gatedExecutor)
	for _, c := range []struct {
		signal      string
		limit, want float64
	}{{"parts", 300, 75}, {"memory", 1000, 500}, {"merges", 0, 4}} {
		p := pressure{signal: c.signal}
		g.over(&p, c.limit)
		if p.threshold != c.want {
			t.Errorf("%s threshold = %v, want %v", c.signal, p.threshold, c.want)
		}
	}
}
