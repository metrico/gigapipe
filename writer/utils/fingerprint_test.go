package utils

import "testing"

// TestFingerprintLabelsIsStable pins the exact output of FingerprintLabels.
// The expected values were taken from the implementation as it is today: the
// values are the contract. If this test fails, the hash changed and label sets
// would map to different fingerprints than before. Fix the code, do not update
// the expected values.
func TestFingerprintLabelsIsStable(t *testing.T) {
	cases := []struct {
		name   string
		labels [][]string
		want   uint64
	}{
		{"two labels", [][]string{{"job", "api"}, {"host", "h1"}}, 0x00e2acc8d95a3d77},
		{"no labels", [][]string{}, 0xcbf29ce484222325},
		{"empty value", [][]string{{"a", ""}}, 0xb2c155d406189be8},
		{"non-ASCII value", [][]string{{"a", "→"}}, 0xdc337d243fe6584a},
		{"three labels", [][]string{{"__name__", "http_requests_total"}, {"method", "GET"}, {"service_name", "checkout"}}, 0xe6652cab585a63e6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FingerprintLabels(tc.labels); got != tc.want {
				t.Fatalf("FingerprintLabels(%v) = 0x%016x, want 0x%016x", tc.labels, got, tc.want)
			}
		})
	}
}
