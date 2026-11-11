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
