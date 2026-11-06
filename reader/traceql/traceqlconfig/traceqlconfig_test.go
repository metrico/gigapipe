package traceqlconfig

import (
	"testing"
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

func TestPortionSpansDefaultTo2500000(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "")
	t.Setenv(PortionSpansEnv, "")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if got := PortionSpans(); got != 2500000 {
		t.Errorf("PortionSpans() = %d, want 2500000", got)
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

func TestAllSettingsAreRead(t *testing.T) {
	t.Setenv(AncestryRoundsEnv, "3")
	t.Setenv(PortionSpansEnv, "7")
	t.Cleanup(func() {
		ancestryRounds = DefaultAncestryRounds
		portionSpans = DefaultPortionSpans
	})
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if AncestryRounds() != 3 || PortionSpans() != 7 {
		t.Errorf("AncestryRounds() = %d, PortionSpans() = %d, want 3 and 7", AncestryRounds(), PortionSpans())
	}
}
