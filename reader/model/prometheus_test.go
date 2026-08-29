package model

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// drain walks a chunkenc.Iterator from the start via Next()/At() and collects
// every (timestamp, value) pair it yields until ValNone. It is the same access
// pattern the Prometheus engine uses when it has no specific Seek target.
func drain(it chunkenc.Iterator) []Sample {
	var out []Sample
	for it.Next() != chunkenc.ValNone {
		ts, v := it.At()
		out = append(out, Sample{TimestampMs: ts, Value: v})
	}
	return out
}

// newProlongIt builds the prolonging iterator the way SeriesV2.Iterator does
// for a series with Prolong=true.
func newProlongIt(samples []Sample, stepMs int64) chunkenc.Iterator {
	s := &SeriesV2{Samples: samples, Prolong: true, StepMs: stepMs}
	return s.Iterator(nil)
}

// TestProlongSeriesIt_EmitsStaleMarkerAtLookbackBoundary is the core desired
// behavior for issue #931. A prolonging iterator (instant selector / rate) must
// yield the real samples untouched and terminate the series with a single
// Prometheus stale marker at lastSample + LookbackDeltaMs, instead of forward-
// filling the value on the step grid for 5 minutes.
//
// Placing the marker at the lookback boundary lets the engine carry the last
// value forward across its own 5m lookback and then stops it exactly there, so
// total visibility is ~5m (matching Prometheus) rather than the ~10m that the
// old fill + engine-lookback stacking produced.
func TestProlongSeriesIt_EmitsStaleMarkerAtLookbackBoundary(t *testing.T) {
	const step = int64(15000)
	samples := []Sample{
		{TimestampMs: 100000, Value: 1},
		{TimestampMs: 115000, Value: 2},
		{TimestampMs: 130000, Value: 3},
	}

	got := drain(newProlongIt(samples, step))

	// Expect the three real samples, followed by exactly one stale marker at the
	// lookback boundary past the last real sample, then nothing.
	if len(got) != len(samples)+1 {
		t.Fatalf("expected %d samples (3 real + 1 stale marker), got %d: %+v",
			len(samples)+1, len(got), got)
	}
	for i, want := range samples {
		if got[i].TimestampMs != want.TimestampMs || got[i].Value != want.Value {
			t.Fatalf("sample %d: expected %+v, got %+v", i, want, got[i])
		}
	}
	marker := got[len(got)-1]
	wantTs := samples[len(samples)-1].TimestampMs + LookbackDeltaMs
	if marker.TimestampMs != wantTs {
		t.Errorf("stale marker timestamp: expected %d (last real + lookback), got %d",
			wantTs, marker.TimestampMs)
	}
	if !value.IsStaleNaN(marker.Value) {
		t.Errorf("expected final sample to be a stale marker (value.IsStaleNaN), got value=%v", marker.Value)
	}
}

// TestProlongSeriesIt_NoStepGridForwardFill guards against the old behavior: the
// iterator used to forward-fill the last value on the step grid for up to 5
// minutes (300000ms), producing ~20 filled samples at a 15s step. After the fix
// it must emit only the real sample plus a single stale marker at the lookback
// boundary, never a long tail of carried-forward values.
func TestProlongSeriesIt_NoStepGridForwardFill(t *testing.T) {
	const step = int64(15000)
	samples := []Sample{
		{TimestampMs: 100000, Value: 42},
	}

	got := drain(newProlongIt(samples, step))

	if len(got) != 2 {
		t.Fatalf("expected exactly 2 emitted samples (1 real + 1 stale marker), got %d: %+v\n"+
			"a count near 21 means step-grid forward-fill is still active", len(got), got)
	}
	if got[0].TimestampMs != 100000 || got[0].Value != 42 {
		t.Fatalf("first sample should be the real one, got %+v", got[0])
	}
	if got[1].TimestampMs != 100000+LookbackDeltaMs || !value.IsStaleNaN(got[1].Value) {
		t.Fatalf("second sample should be a stale marker at the lookback boundary, got %+v", got[1])
	}
}

// TestProlongSeriesIt_SparseGapBridged verifies that a gap SMALLER than the
// lookback window is NOT marked stale: the engine bridges it via its own
// lookback, exactly as upstream Prometheus does for sparse series. Only a single
// trailing marker (at the lookback boundary past the last sample) is emitted.
func TestProlongSeriesIt_SparseGapBridged(t *testing.T) {
	const step = int64(15000)
	// Samples 2 minutes apart: a gap far larger than one step but well within
	// the 5m lookback window, so it must be bridged (no interior marker).
	samples := []Sample{
		{TimestampMs: 0, Value: 1},
		{TimestampMs: 120000, Value: 2},
		{TimestampMs: 240000, Value: 3},
	}

	got := drain(newProlongIt(samples, step))

	markers := 0
	for _, s := range got {
		if value.IsStaleNaN(s.Value) {
			markers++
		}
	}
	if markers != 1 {
		t.Fatalf("sparse gaps within lookback must be bridged; expected exactly 1 trailing "+
			"marker, got %d: %+v", markers, got)
	}
	last := got[len(got)-1]
	if !value.IsStaleNaN(last.Value) || last.TimestampMs != 240000+LookbackDeltaMs {
		t.Fatalf("expected the single marker at the lookback boundary past the last sample, got %+v", last)
	}
}

// TestProlongSeriesIt_StaleMarkerBetweenGaps verifies that when there is a real
// gap LARGER than the lookback window between two clusters of samples, a stale
// marker terminates the first cluster (at the lookback boundary past its last
// sample) before the next real sample resumes.
func TestProlongSeriesIt_StaleMarkerBetweenGaps(t *testing.T) {
	const step = int64(15000)
	// Gap between 130000 and 700000 is 570000ms > LookbackDeltaMs (300000ms).
	samples := []Sample{
		{TimestampMs: 100000, Value: 1},
		{TimestampMs: 115000, Value: 2},
		{TimestampMs: 130000, Value: 3},
		{TimestampMs: 700000, Value: 9},
	}

	got := drain(newProlongIt(samples, step))

	var markerIdx []int
	for i, s := range got {
		if value.IsStaleNaN(s.Value) {
			markerIdx = append(markerIdx, i)
		}
	}
	// One marker terminating the first cluster (130000 + lookback) and one at
	// the end (700000 + lookback).
	if len(markerIdx) != 2 {
		t.Fatalf("expected 2 stale markers (one per cluster end), got %d: %+v", len(markerIdx), got)
	}
	if got[markerIdx[0]].TimestampMs != 130000+LookbackDeltaMs {
		t.Errorf("first stale marker: expected ts %d, got %d", 130000+LookbackDeltaMs, got[markerIdx[0]].TimestampMs)
	}
	if got[markerIdx[1]].TimestampMs != 700000+LookbackDeltaMs {
		t.Errorf("second stale marker: expected ts %d, got %d", 700000+LookbackDeltaMs, got[markerIdx[1]].TimestampMs)
	}
	// The interior marker must fall strictly before the resuming real sample so
	// ordering is preserved.
	if got[markerIdx[0]].TimestampMs >= 700000 {
		t.Errorf("interior marker at %d must precede the resuming sample at 700000",
			got[markerIdx[0]].TimestampMs)
	}
}

// TestProlongSeriesIt_ComesBackAfterStale is the "series reappears" case: a
// series goes stale (a >lookback gap produces a marker) and then a new sample
// arrives. The marker sits in the gap between the two real samples; the old run
// is terminated at its lookback boundary and the new sample starts a fresh run
// with its own trailing marker. This mirrors Prometheus: after a series is
// marked stale, a later sample simply begins reporting again.
func TestProlongSeriesIt_ComesBackAfterStale(t *testing.T) {
	const step = int64(15000)
	// Sample at T=0, then a >lookback gap, then it "comes back" at T=600000.
	samples := []Sample{
		{TimestampMs: 0, Value: 1},
		{TimestampMs: 600000, Value: 2}, // 600000 > 0 + LookbackDeltaMs(300000): a stale gap
	}

	got := drain(newProlongIt(samples, step))

	// Expected sequence: real(0,1), marker(300000), real(600000,2), marker(900000).
	if len(got) != 4 {
		t.Fatalf("expected real, marker, real, marker (4 entries), got %d: %+v", len(got), got)
	}
	if got[0].TimestampMs != 0 || got[0].Value != 1 {
		t.Fatalf("entry 0 should be the first real sample, got %+v", got[0])
	}
	if got[1].TimestampMs != 0+LookbackDeltaMs || !value.IsStaleNaN(got[1].Value) {
		t.Fatalf("entry 1 should be a stale marker terminating the first run at the lookback boundary, got %+v", got[1])
	}
	if got[2].TimestampMs != 600000 || got[2].Value != 2 {
		t.Fatalf("entry 2 should be the reappearing real sample, got %+v", got[2])
	}
	if got[3].TimestampMs != 600000+LookbackDeltaMs || !value.IsStaleNaN(got[3].Value) {
		t.Fatalf("entry 3 should be the trailing marker for the new run, got %+v", got[3])
	}
	// Sanity: the first marker must not overlap the reappearing sample.
	if got[1].TimestampMs >= got[2].TimestampMs {
		t.Fatalf("stale marker at %d must precede the reappearing sample at %d",
			got[1].TimestampMs, got[2].TimestampMs)
	}
}

// TestProlongSeriesIt_EmptySeries verifies no marker (and no panic) for an empty
// series.
func TestProlongSeriesIt_EmptySeries(t *testing.T) {
	got := drain(newProlongIt([]Sample{}, 15000))
	if len(got) != 0 {
		t.Fatalf("expected no samples for empty series, got %+v", got)
	}
}

// sanity: our understanding of the marker encoding matches prometheus.
func TestStaleNaNRoundTrip(t *testing.T) {
	v := math.Float64frombits(value.StaleNaN)
	if !value.IsStaleNaN(v) {
		t.Fatal("Float64frombits(value.StaleNaN) is not detected as a stale marker")
	}
}

// TestProlongSeriesIt_SeekThenNextEmitsMarkerOnce covers the engine's actual
// access pattern: the PromQL engine wraps series iterators in a memoized
// iterator that calls Seek() to position, then Next() to read forward. When
// Seek lands on the last sample of a run, the following Next() must emit the
// stale marker exactly once (at the lookback boundary), then ValNone.
func TestProlongSeriesIt_SeekThenNextEmitsMarkerOnce(t *testing.T) {
	const step = int64(15000)
	it := newProlongIt([]Sample{{TimestampMs: 600000, Value: 42}}, step)

	// The engine seeks to mint (stepT - lookback), which is <= the sample.
	if it.Seek(300000) == chunkenc.ValNone {
		t.Fatal("Seek(300000) should land on the sample at 600000")
	}
	if ts, v := it.At(); ts != 600000 || v != 42 {
		t.Fatalf("after Seek: expected (600000, 42), got (%d, %v)", ts, v)
	}

	// Next -> stale marker at the lookback boundary.
	if it.Next() == chunkenc.ValNone {
		t.Fatal("expected a stale marker after the last real sample")
	}
	ts, v := it.At()
	if ts != 600000+LookbackDeltaMs {
		t.Errorf("stale marker timestamp: expected %d, got %d", 600000+LookbackDeltaMs, ts)
	}
	if !value.IsStaleNaN(v) {
		t.Errorf("expected a stale marker, got value=%v", v)
	}

	// Next -> exhausted; the marker is emitted exactly once.
	if it.Next() != chunkenc.ValNone {
		t.Fatal("expected ValNone after the stale marker (marker must be emitted only once)")
	}
}

// TestProlongSeriesIt_ReSeekResetsStaleMarker guards against a stale marker
// leaking across a re-Seek. The memoized iterator may Seek backwards/forwards
// repeatedly; a marker scheduled at one position must not surface after seeking
// to a different real sample.
func TestProlongSeriesIt_ReSeekResetsStaleMarker(t *testing.T) {
	const step = int64(15000)
	it := newProlongIt([]Sample{
		{TimestampMs: 100000, Value: 1},
		{TimestampMs: 115000, Value: 2},
		{TimestampMs: 130000, Value: 3},
	}, step)

	// Seek to the last sample: this schedules a trailing stale marker.
	if it.Seek(130000) == chunkenc.ValNone {
		t.Fatal("Seek(130000) should land on the last sample")
	}
	if ts, _ := it.At(); ts != 130000 {
		t.Fatalf("expected to land on 130000, got %d", ts)
	}

	// Re-seek to an interior sample: must return that real sample, not a
	// leftover stale marker from the previous position.
	if it.Seek(115000) == chunkenc.ValNone {
		t.Fatal("re-Seek(115000) should land on the interior sample")
	}
	ts, v := it.At()
	if ts != 115000 || v != 2 {
		t.Fatalf("re-Seek should return the real sample (115000, 2), got (%d, %v)", ts, v)
	}
	if value.IsStaleNaN(v) {
		t.Fatal("re-Seek returned a leaked stale marker instead of the real sample")
	}
}
