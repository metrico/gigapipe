package model

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/value"
)

// sanity: our understanding of the marker encoding matches prometheus.
func TestStaleNaNRoundTrip(t *testing.T) {
	v := math.Float64frombits(value.StaleNaN)
	if !value.IsStaleNaN(v) {
		t.Fatal("Float64frombits(value.StaleNaN) is not detected as a stale marker")
	}
}
