package unmarshal

import (
	"testing"

	clconfig "github.com/metrico/cloki-config"
	clokiconfig "github.com/metrico/cloki-config/config"
	clcwriter "github.com/metrico/cloki-config/config/writer"
	"github.com/metrico/qryn/v5/writer/config"
)

// useFingerprintType installs a global config selecting the fingerprint
// algorithm, since fingerprintLabels reads config.Cloki.Setting.FingerPrintType.
func useFingerprintType(t *testing.T, tp uint) {
	t.Helper()
	old := config.Cloki
	config.Cloki = &clconfig.ClokiConfig{Setting: &clokiconfig.ClokiBaseSettingServer{FingerPrintType: tp}}
	t.Cleanup(func() { config.Cloki = old })
}

var fingerprintModes = []struct {
	name string
	tp   uint
}{
	{"cityhash", clcwriter.FINGERPRINT_CityHash},
	{"bernstein", clcwriter.FINGERPRINT_Bernstein},
}

// TestFingerprintLabelsIsStable pins the exact fingerprint of a label set in
// both modes. The fingerprint is persisted in time_series and every samples
// table and joins them, so the expected values were taken from the
// implementation as it is today: the values are the contract. If this test
// fails, newly written series no longer match the series already stored and
// historical data stops being returned by queries. Fix the code, do not update
// the expected values.
func TestFingerprintLabelsIsStable(t *testing.T) {
	cases := []struct {
		name      string
		labels    [][]string
		cityhash  uint64
		bernstein uint64
	}{
		{"two labels", [][]string{{"job", "api"}, {"host", "h1"}}, 0x3c8baa80ef1873d7, 0x5bacc530},
		{"no labels", [][]string{}, 0xf87ec1fb308f668a, 0xc86ed204},
		{"empty value", [][]string{{"a", ""}}, 0xec857352ff94abe1, 0x338f714a},
		{"non-ASCII value", [][]string{{"a", "→"}}, 0x6bbf79236d2d7319, 0x0b2f571d},
		{"three labels", [][]string{{"__name__", "http_requests_total"}, {"method", "GET"}, {"service_name", "checkout"}}, 0x160c2f6094a85f5d, 0xd477306c},
	}
	for _, mode := range fingerprintModes {
		t.Run(mode.name, func(t *testing.T) {
			useFingerprintType(t, mode.tp)
			for _, tc := range cases {
				want := tc.cityhash
				if mode.tp == clcwriter.FINGERPRINT_Bernstein {
					want = tc.bernstein
				}
				if got := fingerprintLabels(tc.labels); got != want {
					t.Errorf("%s: fingerprintLabels(%v) = 0x%016x, want 0x%016x", tc.name, tc.labels, got, want)
				}
			}
		})
	}
}

// TestFingerprintLabelsIgnoresLabelOrder checks that the same label set gives
// the same fingerprint whatever order the labels arrive in. Callers do not sort
// labels before hashing, so the order-independent combination (sum, xor,
// product) is what keeps one series from being split into several.
func TestFingerprintLabelsIgnoresLabelOrder(t *testing.T) {
	orders := [][][]string{
		{{"__name__", "http_requests_total"}, {"method", "GET"}, {"service_name", "checkout"}},
		{{"service_name", "checkout"}, {"__name__", "http_requests_total"}, {"method", "GET"}},
		{{"method", "GET"}, {"service_name", "checkout"}, {"__name__", "http_requests_total"}},
	}
	for _, mode := range fingerprintModes {
		t.Run(mode.name, func(t *testing.T) {
			useFingerprintType(t, mode.tp)
			want := fingerprintLabels(orders[0])
			for _, labels := range orders[1:] {
				if got := fingerprintLabels(labels); got != want {
					t.Errorf("fingerprintLabels(%v) = 0x%016x, want 0x%016x (same labels, different order)", labels, got, want)
				}
			}
		})
	}
}

// TestFingerprintLabelsSeparatesNameAndValue checks that moving characters
// between a label name and its value gives a different series.
func TestFingerprintLabelsSeparatesNameAndValue(t *testing.T) {
	for _, mode := range fingerprintModes {
		t.Run(mode.name, func(t *testing.T) {
			useFingerprintType(t, mode.tp)
			a := fingerprintLabels([][]string{{"ab", "c"}})
			b := fingerprintLabels([][]string{{"a", "bc"}})
			if a == b {
				t.Fatalf(`{ab="c"} and {a="bc"} share fingerprint 0x%016x`, a)
			}
		})
	}
}
