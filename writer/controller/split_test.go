package controller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
)

// splitByType must preserve exactly what a query against the shared table saw.
// GetTypes builds `type IN (wanted, 0)`, so a dual-typed row cannot be written
// as 3 -- it would match neither signal -- and an undefined row must stay 0 in
// both halves, where it keeps matching both.
func TestSplitByType(t *testing.T) {
	src := &model.TimeSamplesData{
		MFingerprint: []uint64{10, 20, 30, 40},
		MTimestampNS: []int64{1, 2, 3, 4},
		MMessage:     []string{"log", "", "both", "undef"},
		MValue:       []float64{0, 2.5, 3.5, 4.5},
		MTTLDays:     []uint16{7, 7, 7, 7},
		MType: []uint8{
			model.SAMPLE_TYPE_LOG,
			model.SAMPLE_TYPE_METRIC,
			model.SAMPLE_TYPE_LOG_AND_METRIC,
			model.SAMPLE_TYPE_UNDEF,
		},
		Size: 100,
	}

	logs, metrics := splitByType(src)

	if got := logs.MFingerprint; len(got) != 3 || got[0] != 10 || got[1] != 30 || got[2] != 40 {
		t.Errorf("log fingerprints = %v, want [10 30 40]", got)
	}
	if got := logs.MType; len(got) != 3 ||
		got[0] != model.SAMPLE_TYPE_LOG ||
		got[1] != model.SAMPLE_TYPE_LOG ||
		got[2] != model.SAMPLE_TYPE_UNDEF {
		t.Errorf("log types = %v, want [1 1 0]", got)
	}
	if got := logs.MMessage; len(got) != 3 || got[0] != "log" || got[1] != "both" || got[2] != "undef" {
		t.Errorf("log messages = %v", got)
	}

	if got := metrics.MFingerprint; len(got) != 3 || got[0] != 20 || got[1] != 30 || got[2] != 40 {
		t.Errorf("metric fingerprints = %v, want [20 30 40]", got)
	}
	if got := metrics.MType; len(got) != 3 ||
		got[0] != model.SAMPLE_TYPE_METRIC ||
		got[1] != model.SAMPLE_TYPE_METRIC ||
		got[2] != model.SAMPLE_TYPE_UNDEF {
		t.Errorf("metric types = %v, want [2 2 0]", got)
	}
	if got := metrics.MValue; len(got) != 3 || got[1] != 3.5 {
		t.Errorf("metric values = %v", got)
	}

	// Every column of a half must be the same length, or the columnar insert
	// builds a malformed block.
	for name, half := range map[string]*model.TimeSamplesData{"logs": logs, "metrics": metrics} {
		n := len(half.MFingerprint)
		if len(half.MTimestampNS) != n || len(half.MValue) != n ||
			len(half.MType) != n || len(half.MMessage) != n || len(half.MTTLDays) != n {
			t.Errorf("%s: ragged columns: fp=%d ts=%d val=%d tp=%d msg=%d ttl=%d",
				name, n, len(half.MTimestampNS), len(half.MValue),
				len(half.MType), len(half.MMessage), len(half.MTTLDays))
		}
	}
}

// samples_metrics has no string column, so a dual-typed row's message bytes
// must count toward the logs half's Size only -- the metrics half never
// writes them and must not flush early over bytes it doesn't send.
func TestSplitByTypeSizeCountsMessageOnceOnLogsHalf(t *testing.T) {
	src := &model.TimeSamplesData{
		MFingerprint: []uint64{1, 2},
		MTimestampNS: []int64{1, 2},
		MMessage:     []string{"twelve chars", ""},
		MValue:       []float64{0, 5},
		MTTLDays:     []uint16{7, 7},
		MType:        []uint8{model.SAMPLE_TYPE_LOG_AND_METRIC, model.SAMPLE_TYPE_UNDEF},
	}
	logs, metrics := splitByType(src)

	wantLogs := 26 + len("twelve chars") + 26
	if logs.Size != wantLogs {
		t.Errorf("logs.Size = %d, want %d", logs.Size, wantLogs)
	}
	wantMetrics := 26 + 26
	if metrics.Size != wantMetrics {
		t.Errorf("metrics.Size = %d, want %d (message bytes must not count)", metrics.Size, wantMetrics)
	}
}

// A half with no rows must come back nil so doPush skips it entirely rather
// than sending an empty insert.
func TestSplitByTypeEmptyHalf(t *testing.T) {
	src := &model.TimeSamplesData{
		MFingerprint: []uint64{1},
		MTimestampNS: []int64{1},
		MMessage:     []string{""},
		MValue:       []float64{7},
		MTTLDays:     []uint16{7},
		MType:        []uint8{model.SAMPLE_TYPE_METRIC},
	}
	logs, metrics := splitByType(src)
	if logs != nil {
		t.Errorf("log half must be nil, got %d rows", len(logs.MFingerprint))
	}
	if metrics == nil || len(metrics.MFingerprint) != 1 {
		t.Error("metric half must carry the single row")
	}
}

// With the split on, IngestParsed must send each half to its own service and
// nothing to the other.
func TestIngestParsedSplitsSamples(t *testing.T) {
	installFPCache(t, "n")
	installConfig(t)
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	reloadSamplesConfig(t)

	spl := &recorderSvc{}
	mtr := &recorderSvc{}
	samples := &model.TimeSamplesData{
		MFingerprint: []uint64{1, 2},
		MTimestampNS: []int64{1, 2},
		MMessage:     []string{"line", ""},
		MValue:       []float64{0, 5},
		MTTLDays:     []uint16{7, 7},
		MType:        []uint8{model.SAMPLE_TYPE_LOG, model.SAMPLE_TYPE_METRIC},
	}
	parser := func(_ context.Context, _ numbercache.ICache[uint64]) chan *model.ParserResponse {
		ch := make(chan *model.ParserResponse, 1)
		ch <- &model.ParserResponse{SamplesRequest: samples}
		close(ch)
		return ch
	}

	err := IngestParsed(context.Background(), parser,
		InsertServices{Spl: spl, Mtr: mtr, Node: "n"})
	if err != nil {
		t.Fatalf("IngestParsed: %v", err)
	}

	splReqs := spl.reqs()
	if len(splReqs) != 1 {
		t.Fatalf("samples service got %d requests, want 1", len(splReqs))
	}
	if got := splReqs[0].(*model.TimeSamplesData).MFingerprint; len(got) != 1 || got[0] != 1 {
		t.Errorf("log half fingerprints = %v, want [1]", got)
	}
	mtrReqs := mtr.reqs()
	if len(mtrReqs) != 1 {
		t.Fatalf("metrics service got %d requests, want 1", len(mtrReqs))
	}
	if got := mtrReqs[0].(*model.TimeSamplesData).MFingerprint; len(got) != 1 || got[0] != 2 {
		t.Errorf("metric half fingerprints = %v, want [2]", got)
	}
}

// doParse rebuilds InsertServices from context keys rather than from what
// ResolveLogServices returned, so the metric half only reaches its service if
// doParse reads the metrics key. Task 4 proved the middleware plants that key;
// this proves doParse reads it, which is the other half of the same defect.
func TestDoParseRoutesTheMetricHalf(t *testing.T) {
	installFPCache(t, "n")
	installConfig(t)
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	reloadSamplesConfig(t)

	spl := &recorderSvc{}
	mtr := &recorderSvc{}
	samples := &model.TimeSamplesData{
		MFingerprint: []uint64{1, 2},
		MTimestampNS: []int64{1, 2},
		MMessage:     []string{"line", ""},
		MValue:       []float64{0, 5},
		MTTLDays:     []uint16{7, 7},
		MType:        []uint8{model.SAMPLE_TYPE_LOG, model.SAMPLE_TYPE_METRIC},
	}
	parser := func(_ context.Context, _ io.Reader, _ numbercache.ICache[uint64]) chan *model.ParserResponse {
		ch := make(chan *model.ParserResponse, 1)
		ch <- &model.ParserResponse{SamplesRequest: samples}
		close(ch)
		return ch
	}

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx := req.Context()
	ctx = context.WithValue(ctx, utils.ContextKeySplService, service.IInsertServiceV2(spl))
	ctx = context.WithValue(ctx, utils.ContextKeyMtrService, service.IInsertServiceV2(mtr))
	ctx = context.WithValue(ctx, utils.ContextKeyNode, "n")
	req = req.WithContext(ctx)

	if err := doParse(req, parser); err != nil {
		t.Fatalf("doParse: %v", err)
	}
	if len(mtr.reqs()) != 1 {
		t.Fatalf("metrics service got %d requests through doParse, want 1", len(mtr.reqs()))
	}
	if got := mtr.reqs()[0].(*model.TimeSamplesData).MFingerprint; len(got) != 1 || got[0] != 2 {
		t.Errorf("metric half fingerprints = %v, want [2]", got)
	}
}

// With the split off nothing changes: one request, whole, to the samples
// service, and the metrics service untouched even when it is wired.
func TestIngestParsedNoSplitKeepsOneRequest(t *testing.T) {
	installFPCache(t, "n")
	installConfig(t)
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "false")
	reloadSamplesConfig(t)

	spl := &recorderSvc{}
	mtr := &recorderSvc{}
	samples := &model.TimeSamplesData{
		MFingerprint: []uint64{1, 2},
		MTimestampNS: []int64{1, 2},
		MMessage:     []string{"line", ""},
		MValue:       []float64{0, 5},
		MTTLDays:     []uint16{7, 7},
		MType:        []uint8{model.SAMPLE_TYPE_LOG, model.SAMPLE_TYPE_METRIC},
	}
	parser := func(_ context.Context, _ numbercache.ICache[uint64]) chan *model.ParserResponse {
		ch := make(chan *model.ParserResponse, 1)
		ch <- &model.ParserResponse{SamplesRequest: samples}
		close(ch)
		return ch
	}

	err := IngestParsed(context.Background(), parser,
		InsertServices{Spl: spl, Mtr: mtr, Node: "n"})
	if err != nil {
		t.Fatalf("IngestParsed: %v", err)
	}
	splReqs := spl.reqs()
	if len(splReqs) != 1 || splReqs[0] != samples {
		t.Errorf("samples service must get the original request unchanged")
	}
	if len(mtr.reqs()) != 0 {
		t.Error("metrics service must stay untouched without the split")
	}
}
