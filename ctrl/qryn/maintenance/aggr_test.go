package maintenance

import (
	"strings"
	"testing"
	"time"
)

// The stored state must survive a round trip: SyncMetricsAggrMV compares the
// desired string against the stored one and only reads the interval back when
// it needs to validate a change.
func TestAggrStateRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		enabled  bool
		interval time.Duration
	}{
		{true, 15 * time.Second},
		{false, time.Minute},
		{true, 5 * time.Minute},
	} {
		state := aggrStateString(tc.enabled, tc.interval)
		got, ok := parseAggrInterval(state)
		if !ok {
			t.Fatalf("parseAggrInterval(%q) failed", state)
		}
		if got != tc.interval {
			t.Errorf("%q: interval = %v, want %v", state, got, tc.interval)
		}
	}
}

// A state string this code did not write is treated as absent, which makes the
// caller recreate the view instead of trusting an unknown granularity.
func TestParseAggrIntervalRejectsGarbage(t *testing.T) {
	for _, s := range []string{"", "enabled=true", "interval=abc", "interval=", "nonsense"} {
		if _, ok := parseAggrInterval(s); ok {
			t.Errorf("parseAggrInterval(%q) accepted", s)
		}
	}
}

// A state string this code did not write is treated as absent, mirroring
// parseAggrInterval: the caller must not assume "was disabled" when it really
// means "no prior run".
func TestParseAggrEnabledRoundTrip(t *testing.T) {
	for _, tc := range []bool{true, false} {
		state := aggrStateString(tc, 15*time.Second)
		got, ok := parseAggrEnabled(state)
		if !ok {
			t.Fatalf("parseAggrEnabled(%q) failed", state)
		}
		if got != tc {
			t.Errorf("%q: enabled = %v, want %v", state, got, tc)
		}
	}
}

func TestParseAggrEnabledRejectsGarbage(t *testing.T) {
	for _, s := range []string{"", "interval=15", "enabled=maybe", "nonsense"} {
		if _, ok := parseAggrEnabled(s); ok {
			t.Errorf("parseAggrEnabled(%q) accepted", s)
		}
	}
}

// Growing the bucket by a whole factor is safe: stored rows are aggregate
// states and the read path re-buckets them onto the coarser grid. Anything else
// leaves buckets coarser than the interval the read path assumes -- a 15s step
// over 60s buckets finds the whole minute in the first sub-bucket and nothing
// in the other three -- so it must be refused, not silently applied.
func TestCheckIntervalChange(t *testing.T) {
	for _, tc := range []struct {
		name    string
		old     time.Duration
		new     time.Duration
		wantErr bool
	}{
		{"no previous state", 0, 15 * time.Second, false},
		{"unchanged", 15 * time.Second, 15 * time.Second, false},
		{"grow by 4", 15 * time.Second, time.Minute, false},
		{"grow by 3", 15 * time.Second, 45 * time.Second, false},
		{"grow, not a multiple", 15 * time.Second, 20 * time.Second, true},
		{"shrink", time.Minute, 15 * time.Second, true},
		{"shrink, not a multiple", time.Minute, 25 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkIntervalChange(tc.old, tc.new)
			if tc.wantErr && err == nil {
				t.Errorf("checkIntervalChange(%v, %v) = nil, want error", tc.old, tc.new)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("checkIntervalChange(%v, %v) = %v, want nil", tc.old, tc.new, err)
			}
		})
	}
}

// METRICS_AGGR_DAYS = 0 means inherit the samples retention; the db config is
// the only place that value exists, so resolution happens at the call site.
func TestAggrDropDays(t *testing.T) {
	for _, tc := range []struct {
		configured, fallback, want int
	}{
		{0, 7, 7},
		{90, 7, 90},
		{1, 30, 1},
	} {
		if got := aggrDropDays(tc.configured, tc.fallback); got != tc.want {
			t.Errorf("aggrDropDays(%d, %d) = %d, want %d",
				tc.configured, tc.fallback, got, tc.want)
		}
	}
}

// The view must read samples_metrics, write metrics_aggr, bucket at the
// configured width and carry no bytes column (metrics have no string to measure).
func TestMetricsAggrMVRenders(t *testing.T) {
	env := scriptEnv("qryn", "", "", "timestamp_ns", 7, false, false)
	env["AGGR_INTERVAL_NS"] = "60000000000"
	got := render(t, metricsAggrMVQuery, env)
	for _, want := range []string{
		"qryn.metrics_aggr_mv", "TO qryn.metrics_aggr",
		"FROM qryn.samples_metrics", "60000000000",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered view missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "bytes") {
		t.Errorf("metrics_aggr_mv must not select bytes:\n%s", got)
	}
	if strings.Contains(got, "15000000000") {
		t.Errorf("metrics_aggr_mv has a hardcoded interval:\n%s", got)
	}
}
