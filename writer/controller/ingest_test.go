package controller

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
	"unsafe"

	clconfig "github.com/metrico/cloki-config"
	clokiconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	"github.com/metrico/qryn/v5/writer/utils/promise"
)

// installConfig sets a minimal global config so doPush can read retry settings
// (config.Cloki.Setting.SYSTEM_SETTINGS) without a nil-pointer panic. One retry
// attempt with no delay keeps the push path fast and deterministic.
func installConfig(t *testing.T) {
	t.Helper()
	old := config.Cloki
	setting := &clokiconfig.ClokiBaseSettingServer{}
	setting.SYSTEM_SETTINGS.RetryAttempts = 1
	setting.SYSTEM_SETTINGS.RetryTimeoutS = 0
	config.Cloki = &clconfig.ClokiConfig{Setting: setting}
	t.Cleanup(func() { config.Cloki = old })
}

// installFPCache registers a trivial per-node fingerprint cache for the node
// under test and restores the previous global on cleanup. IngestParsed resolves
// FPCache.DB(node) before running the parser, so a real (if trivial) cache must
// exist for the node.
func installFPCache(t *testing.T, node string) {
	t.Helper()
	old := FPCache
	FPCache = numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{node: {}})
	t.Cleanup(func() {
		FPCache.Stop()
		FPCache = old
	})
}

// recorderSvc is a minimal IInsertServiceV2 that records every request routed
// to it and returns an already-fulfilled promise so doPush completes at once.
type recorderSvc struct {
	mu       sync.Mutex
	received []helpers.SizeGetter
}

func (r *recorderSvc) Request(req helpers.SizeGetter, insertMode int) *promise.Promise[uint32] {
	r.mu.Lock()
	r.received = append(r.received, req)
	r.mu.Unlock()
	return promise.Fulfilled[uint32](nil, 0)
}

func (r *recorderSvc) reqs() []helpers.SizeGetter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]helpers.SizeGetter(nil), r.received...)
}

func (r *recorderSvc) Run()                        {}
func (r *recorderSvc) Stop()                       {}
func (r *recorderSvc) Ping() (time.Time, error)    { return time.Time{}, nil }
func (r *recorderSvc) GetState(insertMode int) int { return 0 }
func (r *recorderSvc) GetNodeName() string         { return "n" }
func (r *recorderSvc) Init()                       {}
func (r *recorderSvc) PlanFlush()                  {}

func TestIngestParsed_EmptyParserReturnsNil(t *testing.T) {
	installFPCache(t, "n")

	// A parser that emits no responses must complete without error and
	// without touching any (nil) service.
	parser := func(_ context.Context, _ io.Reader, _ numbercache.ICache[uint64]) chan *model.ParserResponse {
		ch := make(chan *model.ParserResponse)
		close(ch)
		return ch
	}
	err := IngestParsed(context.Background(), parser, InsertServices{Node: "n"})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// TestIngestParsed_PushRouting proves the field->service mapping: each
// ParserResponse sub-request is pushed to its matching service and ONLY that
// service, for ALL FIVE slots:
//
//	TimeSeriesRequest -> Ts
//	SamplesRequest    -> Spl
//	SpansAttrsRequest -> SpanAttrs
//	SpansRequest      -> Spans
//	ProfileRequest    -> Profile
//
// Every slot carries a distinct recognizable value and each recorder must see
// exactly its own request and no other. This guards against any cross-wiring
// (e.g. a Ts<->Spl or SpanAttrs<->Spans swap) in any slot.
func TestIngestParsed_PushRouting(t *testing.T) {
	installFPCache(t, "n")
	installConfig(t)

	// SamplesRequest MUST be a *model.TimeSamplesData: IngestParsed casts it in
	// doLogsPattern. The other four only need to be SizeGetters; distinct
	// *model.TimeSamplesData pointers give recognizable identities to assert on.
	tsReq := &model.TimeSamplesData{Size: 10}
	samplesReq := &model.TimeSamplesData{Size: 20}
	spanAttrsReq := &model.TimeSamplesData{Size: 30}
	spansReq := &model.TimeSamplesData{Size: 40}
	profileReq := &model.TimeSamplesData{Size: 50}

	parser := func(_ context.Context, _ io.Reader, _ numbercache.ICache[uint64]) chan *model.ParserResponse {
		ch := make(chan *model.ParserResponse, 1)
		ch <- &model.ParserResponse{
			TimeSeriesRequest: tsReq,
			SamplesRequest:    samplesReq,
			SpansAttrsRequest: spanAttrsReq,
			SpansRequest:      spansReq,
			ProfileRequest:    profileReq,
		}
		close(ch)
		return ch
	}

	ts := &recorderSvc{}
	spl := &recorderSvc{}
	spanAttrs := &recorderSvc{}
	spans := &recorderSvc{}
	profile := &recorderSvc{}

	err := IngestParsed(context.Background(), parser, InsertServices{
		Ts:        ts,
		Spl:       spl,
		SpanAttrs: spanAttrs,
		Spans:     spans,
		Profile:   profile,
		Node:      "n",
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Each recorder must have received exactly its own request and only that.
	cases := []struct {
		name string
		svc  *recorderSvc
		want helpers.SizeGetter
	}{
		{"Ts", ts, tsReq},
		{"Spl", spl, samplesReq},
		{"SpanAttrs", spanAttrs, spanAttrsReq},
		{"Spans", spans, spansReq},
		{"Profile", profile, profileReq},
	}
	for _, c := range cases {
		got := c.svc.reqs()
		if len(got) != 1 || got[0] != c.want {
			t.Fatalf("%s: expected exactly [%p], got %v", c.name, c.want, got)
		}
	}
}

// TestIngestParsed_ErrorPath proves that a ParserResponse carrying an Error is
// returned verbatim, and that trailing responses on the channel are drained so
// the parser goroutine cannot deadlock. The overall test timeout guards against
// a hang.
func TestIngestParsed_ErrorPath(t *testing.T) {
	installFPCache(t, "n")

	wantErr := errors.New("boom")

	parser := func(_ context.Context, _ io.Reader, _ numbercache.ICache[uint64]) chan *model.ParserResponse {
		// Buffered + trailing responses: IngestParsed returns on the first
		// error but must drain the rest without blocking the sender.
		ch := make(chan *model.ParserResponse, 3)
		ch <- &model.ParserResponse{Error: wantErr}
		ch <- &model.ParserResponse{SamplesRequest: &model.TimeSamplesData{Size: 1}}
		ch <- &model.ParserResponse{SpansRequest: &model.TimeSamplesData{Size: 2}}
		close(ch)
		return ch
	}

	done := make(chan error, 1)
	go func() {
		done <- IngestParsed(context.Background(), parser, InsertServices{Node: "n"})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected %v, got %v", wantErr, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("IngestParsed did not return within timeout (possible deadlock)")
	}
}
