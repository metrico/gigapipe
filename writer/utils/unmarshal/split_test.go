package unmarshal

import (
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	clconfig "github.com/metrico/cloki-config"
	clokiconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/shared/samplesconfig"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
)

// setSplitBySignal drives the samplesconfig loader directly, the same way
// controller.reloadSamplesConfig does, and always leaves the package back at
// its default so this test's env choice cannot leak into another test.
func setSplitBySignal(t *testing.T, on string) {
	t.Helper()
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", on)
	if err := samplesconfig.Reload(); err != nil {
		t.Fatalf("samplesconfig reload: %v", err)
	}
	t.Cleanup(func() {
		t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "false")
		_ = samplesconfig.Reload()
	})
}

// A loki-style entry carrying both a line and a value produces type 3.
// TestPushEntryDualTypeCollapse pins the two behaviours of the tp==3 gate this
// package now has: kept as 3 for a split store, collapsed to 0 for a shared
// one -- the guard the shared table's `type IN (wanted, 0)` reads depend on.
func TestPushEntryDualTypeCollapse(t *testing.T) {
	body := `{"streams":[{"stream":{"job":"t"},"entries":[` +
		`{"ts":"1600000000000000000","line":"hello","value":5}]}]}`

	decode := func() uint8 {
		dec := &pushRequestDec{ctx: &ParserCtx{bodyReader: strings.NewReader(body)}}
		var got uint8
		dec.SetOnEntries(func(_ [][]string, _ []int64, _ []string, _ []float64, types []uint8) error {
			got = types[0]
			return nil
		})
		if err := dec.Decode(); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return got
	}

	setSplitBySignal(t, "true")
	if got := decode(); got != model.SAMPLE_TYPE_LOG_AND_METRIC {
		t.Errorf("split on: type = %d, want %d", got, model.SAMPLE_TYPE_LOG_AND_METRIC)
	}

	setSplitBySignal(t, "false")
	if got := decode(); got != model.SAMPLE_TYPE_UNDEF {
		t.Errorf("split off: type = %d, want %d", got, model.SAMPLE_TYPE_UNDEF)
	}
}

// TestOnEntriesFansDualTypeIntoTimeSeries is the panic guard: before the tps
// array was widened past index 2, a type-3 row indexed out of range here. It
// must also not just survive -- label lookups filter with `type IN (wanted,
// 0)`, so the row has to produce a time_series row of type 1 and one of type
// 2, never a single row of type 3.
func TestOnEntriesFansDualTypeIntoTimeSeries(t *testing.T) {
	old := config.Cloki
	config.Cloki = &clconfig.ClokiConfig{Setting: &clokiconfig.ClokiBaseSettingServer{}}
	t.Cleanup(func() { config.Cloki = old })

	fpCache := numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, nil)

	p := &parserDoer{
		ctx:   &ParserCtx{fpCache: fpCache},
		tsSpl: newTimeSeriesAndSamples(make(chan *model.ParserResponse, 1), ""),
	}

	err := p.onEntries(
		[][]string{{"job", "t"}},
		[]int64{1600000000000000000},
		[]string{"hello"},
		[]float64{5},
		[]uint8{model.SAMPLE_TYPE_LOG_AND_METRIC},
	)
	if err != nil {
		t.Fatalf("onEntries: %v", err)
	}

	types := p.tsSpl.ts.MType
	if !slices.Contains(types, uint8(model.SAMPLE_TYPE_LOG)) ||
		!slices.Contains(types, uint8(model.SAMPLE_TYPE_METRIC)) {
		t.Fatalf("time_series types = %v, want both %d and %d",
			types, model.SAMPLE_TYPE_LOG, model.SAMPLE_TYPE_METRIC)
	}
	if slices.Contains(types, uint8(model.SAMPLE_TYPE_LOG_AND_METRIC)) {
		t.Fatalf("time_series types = %v, must not contain %d", types, model.SAMPLE_TYPE_LOG_AND_METRIC)
	}
}
