package helputils

import "testing"

// TestFingerprintLabelsDJBHashPrometheusIsStable pins the exact output of the
// DJB hash used for the fingerprint of rows in the settings table, which is
// persisted. The expected values were taken from the implementation as it is
// today: the values are the contract. writer/utils/helputils keeps a copy of
// this function that is pinned to the same values. If this test fails, fix the
// code, do not update the expected values.
func TestFingerprintLabelsDJBHashPrometheusIsStable(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want uint32
	}{
		{"nil", nil, 0x00000000},
		{"empty", []byte{}, 0x00001505},
		{"one byte", []byte("a"), 0x0002b5c4},
		{"label pair", []byte("job=api"), 0x9abf7fc7},
		{"non-ASCII", []byte("→"), 0x0b84ee53},
		{"high byte", []byte{0xff}, 0x0002b55a},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FingerprintLabelsDJBHashPrometheus(tc.data); got != tc.want {
				t.Fatalf("FingerprintLabelsDJBHashPrometheus(%q) = 0x%08x, want 0x%08x", tc.data, got, tc.want)
			}
		})
	}
}
