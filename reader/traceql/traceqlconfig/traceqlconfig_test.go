package traceqlconfig

import (
	"testing"
	"time"
)

func TestAncestryRoundsDefaultTo20(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if got := AncestryRounds(); got != 20 {
		t.Errorf("AncestryRounds() = %d, want 20", got)
	}
}

func TestAncestryRoundsFromTheEnvironment(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "3")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if got := AncestryRounds(); got != 3 {
		t.Errorf("AncestryRounds() = %d, want 3", got)
	}
}

func TestAncestryRoundsRejectsInvalidValues(t *testing.T) {
	for _, v := range []string{"0", "-1", "33", "x", "1.5"} {
		t.Setenv(AncestryRoundsEnv, v)
		if err := Init(); err == nil {
			t.Errorf("%s=%s: no error", AncestryRoundsEnv, v)
		}
	}
}

func TestPortionSpansDefaultTo5000000(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "")
	t.Setenv(PortionSpansEnv, "")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if got := PortionSpans(); got != 5000000 {
		t.Errorf("PortionSpans() = %d, want 5000000", got)
	}
}

func TestPortionSpansFromTheEnvironment(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "")
	t.Setenv(PortionSpansEnv, "7")
	t.Cleanup(func() { portionSpans = DefaultPortionSpans })
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if got := PortionSpans(); got != 7 {
		t.Errorf("PortionSpans() = %d, want 7", got)
	}
}

func TestPortionSpansRejectsInvalidValues(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "")
	for _, v := range []string{"0", "-1", "x", "1.5", "2.5e6"} {
		t.Setenv(PortionSpansEnv, v)
		if err := Init(); err == nil {
			t.Errorf("%s=%s: no error", PortionSpansEnv, v)
		}
	}
}

func TestSearchMaxDurationDefaultsTo168h(t *testing.T) {
	t.Setenv(SearchMaxDurationEnv, "")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if got := SearchMaxDuration(); got != 168*time.Hour {
		t.Errorf("SearchMaxDuration() = %s, want 168h", got)
	}
}

func TestSearchMaxDurationFromTheEnvironment(t *testing.T) {
	t.Cleanup(func() { searchMaxDuration = DefaultSearchMaxDuration })
	for v, want := range map[string]time.Duration{"24h": 24 * time.Hour, "0": 0, "0s": 0} {
		t.Setenv(SearchMaxDurationEnv, v)
		if err := Init(); err != nil {
			t.Fatalf("%s=%s: %v", SearchMaxDurationEnv, v, err)
		}
		if got := SearchMaxDuration(); got != want {
			t.Errorf("%s=%s: SearchMaxDuration() = %s, want %s", SearchMaxDurationEnv, v, got, want)
		}
	}
}

func TestSearchMaxDurationRejectsInvalidValues(t *testing.T) {
	for _, v := range []string{"-1h", "x", "7", "1.5"} {
		t.Setenv(SearchMaxDurationEnv, v)
		if err := Init(); err == nil {
			t.Errorf("%s=%s: no error", SearchMaxDurationEnv, v)
		}
	}
}

func TestAllSettingsAreRead(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "3")
	t.Setenv(PortionSpansEnv, "7")
	t.Setenv(SearchMaxDurationEnv, "24h")
	t.Cleanup(func() {
		ancestryRounds = DefaultAncestryRounds
		portionSpans = DefaultPortionSpans
		searchMaxDuration = DefaultSearchMaxDuration
	})
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if AncestryRounds() != 3 || PortionSpans() != 7 || SearchMaxDuration() != 24*time.Hour {
		t.Errorf("AncestryRounds() = %d, PortionSpans() = %d, SearchMaxDuration() = %s, want 3, 7 and 24h",
			AncestryRounds(), PortionSpans(), SearchMaxDuration())
	}
}
