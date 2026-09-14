package samplesconfig

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	reset(t)
	if err := load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if SplitBySignal() {
		t.Error("split must default to off")
	}
	if !MetricsAggrEnabled() {
		t.Error("aggregation must default to on")
	}
	if got := MetricsAggrInterval(); got != 15*time.Second {
		t.Errorf("interval = %v, want 15s", got)
	}
	if got := MetricsAggrIntervalMs(); got != 15000 {
		t.Errorf("interval ms = %d, want 15000", got)
	}
	if got := MetricsAggrDays(); got != 0 {
		t.Errorf("days = %d, want 0 (inherit)", got)
	}
	if got := MetricsOrdering(); got != "" {
		t.Errorf("ordering = %q, want empty (inherit)", got)
	}
}

func TestLoadOverrides(t *testing.T) {
	reset(t)
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	t.Setenv("METRICS_AGGR_ENABLED", "false")
	t.Setenv("METRICS_AGGR_INTERVAL", "60s")
	t.Setenv("METRICS_AGGR_DAYS", "90")
	t.Setenv("ADVANCED_METRICS_ORDERING", "fingerprint, timestamp_ns")
	if err := load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if !SplitBySignal() {
		t.Error("split must be on")
	}
	if MetricsAggrEnabled() {
		t.Error("aggregation must be off")
	}
	if got := MetricsAggrInterval(); got != time.Minute {
		t.Errorf("interval = %v, want 1m", got)
	}
	if got := MetricsAggrIntervalMs(); got != 60000 {
		t.Errorf("interval ms = %d, want 60000", got)
	}
	if got := MetricsAggrDays(); got != 90 {
		t.Errorf("days = %d, want 90", got)
	}
	if got := MetricsOrdering(); got != "fingerprint, timestamp_ns" {
		t.Errorf("ordering = %q", got)
	}
}

// A malformed value must be fatal, not silently ignored: a typo in the interval
// would otherwise build the aggregate at the wrong granularity, and the error
// would only surface as wrong query results much later.
func TestLoadRejectsMalformed(t *testing.T) {
	for _, tc := range []struct{ key, val string }{
		{"SAMPLES_SPLIT_BY_SIGNAL", "maybe"},
		{"METRICS_AGGR_ENABLED", "sometimes"},
		{"METRICS_AGGR_INTERVAL", "15 seconds"},
		{"METRICS_AGGR_INTERVAL", "0s"},
		{"METRICS_AGGR_INTERVAL", "-5s"},
		{"METRICS_AGGR_INTERVAL", "100us"},
		{"METRICS_AGGR_INTERVAL", "1500ms"},
		{"METRICS_AGGR_DAYS", "many"},
		{"METRICS_AGGR_DAYS", "-1"},
	} {
		t.Run(tc.key+"="+tc.val, func(t *testing.T) {
			reset(t)
			t.Setenv(tc.key, tc.val)
			if err := load(); err == nil {
				t.Errorf("%s=%q accepted, want error", tc.key, tc.val)
			}
		})
	}
}

func TestTableNames(t *testing.T) {
	reset(t)
	if err := load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := SamplesTable(false); got != "samples_v3" {
		t.Errorf("logs samples table = %q", got)
	}
	if got := SamplesTable(true); got != "samples_v3" {
		t.Errorf("metrics samples table = %q", got)
	}
	if got := AggrTable(false); got != "metrics_15s" {
		t.Errorf("logs aggr table = %q", got)
	}
	if got := AggrTable(true); got != "metrics_15s" {
		t.Errorf("metrics aggr table = %q", got)
	}

	reset(t)
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "1")
	if err := load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, tc := range []struct {
		got, want string
	}{
		{SamplesTable(false), "samples_logs"},
		{SamplesTable(true), "samples_metrics"},
		{AggrTable(false), "logs_aggr"},
		{AggrTable(true), "metrics_aggr"},
	} {
		if tc.got != tc.want {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}
}

// MetricsAggrAvailable answers "is there a metrics preaggregate to read", which
// is the question the read path asks. Without the split there always is one
// (metrics_15s); with it, only while METRICS_AGGR_ENABLED holds.
func TestMetricsAggrAvailable(t *testing.T) {
	for _, tc := range []struct {
		split, enabled, want bool
	}{
		{false, true, true},
		{false, false, true},
		{true, true, true},
		{true, false, false},
	} {
		reset(t)
		t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", boolStr(tc.split))
		t.Setenv("METRICS_AGGR_ENABLED", boolStr(tc.enabled))
		if err := load(); err != nil {
			t.Fatalf("load: %v", err)
		}
		if got := MetricsAggrAvailable(); got != tc.want {
			t.Errorf("split=%v enabled=%v: available = %v, want %v",
				tc.split, tc.enabled, got, tc.want)
		}
	}
}

// Init reports the same result to every caller, not just the first: it runs
// load exactly once, so a later caller must not read a failed load as success.
func TestInitReportsTheLoadErrorToEveryCaller(t *testing.T) {
	reset(t)
	t.Setenv("METRICS_AGGR_INTERVAL", "not a duration")

	first := Init()
	if first == nil {
		t.Fatal("first Init accepted a malformed interval")
	}
	if second := Init(); second == nil {
		t.Error("second Init returned nil after a failed load")
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// reset clears every variable the package reads so each case starts from the
// documented defaults regardless of the ambient environment.
func reset(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"SAMPLES_SPLIT_BY_SIGNAL", "METRICS_AGGR_ENABLED", "METRICS_AGGR_INTERVAL",
		"METRICS_AGGR_DAYS", "ADVANCED_METRICS_ORDERING",
	} {
		t.Setenv(k, "")
	}
}
