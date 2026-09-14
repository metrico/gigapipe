package dbversion

import "testing"

func TestSupportsStaleness(t *testing.T) {
	for _, c := range []struct {
		version string
		want    bool
	}{
		{"24.9.1.3278", false},
		{"24.10.0.0", false},
		{"24.11.1.0", true},
		{"25.3.14.14", true},
		{"23.8.1.1", false},
		{"26.1.0.0", true},
		{"24.11", true},
		{"24.9", false},
		{"garbage", false},
		{"", false},
		{"24", false},
	} {
		if got := supportsStaleness(c.version); got != c.want {
			t.Errorf("supportsStaleness(%q) = %v, want %v", c.version, got, c.want)
		}
	}
}

func TestHasCapability(t *testing.T) {
	v := VersionInfo{CapStaleness: 0}
	if !v.HasCapability(CapStaleness) {
		t.Error("expected capability present")
	}
	empty := VersionInfo{}
	if empty.HasCapability(CapStaleness) {
		t.Error("expected capability absent on empty version info")
	}
}

func TestMetrics15sAvailable(t *testing.T) {
	sec := int64(1_700_000_000)
	ns := sec * 1_000_000_000

	cases := []struct {
		name   string
		v      VersionInfo
		fromNS int64
		want   bool
	}{
		{"table absent", VersionInfo{}, ns, false},
		{"table present, no marker", VersionInfo{CapMetrics15s: 0}, ns, true},
		{"window after enable", VersionInfo{CapMetrics15s: 0, MarkerMetrics15s: sec - 100}, ns, true},
		{"window before enable", VersionInfo{CapMetrics15s: 0, MarkerMetrics15s: sec + 100}, ns, false},
		{"disabled sentinel", VersionInfo{CapMetrics15s: 0, MarkerMetrics15s: 4102444800}, ns, false},
	}
	for _, c := range cases {
		if got := c.v.Metrics15sAvailable(c.fromNS); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
