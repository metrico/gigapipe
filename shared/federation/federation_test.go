package federation

import "testing"

func TestParseBool(t *testing.T) {
	truthy := []string{"true", "1", "yes", "y", "TRUE", " Yes ", "Y"}
	for _, v := range truthy {
		if !parseBool(v) {
			t.Errorf("parseBool(%q) = false, want true", v)
		}
	}
	falsy := []string{"", "0", "no", "n", "false", "off", "nope", "2"}
	for _, v := range falsy {
		if parseBool(v) {
			t.Errorf("parseBool(%q) = true, want false", v)
		}
	}
}

func TestEnabledDefaultsFalse(t *testing.T) {
	// Init has not been called (or FEDERATED unset): default must be off so a
	// typo can never silently enable tenancy.
	if Enabled() {
		t.Fatal("Enabled() must default to false")
	}
}
