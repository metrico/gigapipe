package controller

import (
	"context"
	"io"
	"testing"
	"time"
	"unsafe"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
)

func TestIngestParsed_EmptyParserReturnsNil(t *testing.T) {
	// IngestParsed resolves a per-node fingerprint cache via FPCache.DB(node)
	// before running the parser, so the test needs a real (if trivial) cache
	// registered for the node under test.
	old := FPCache
	FPCache = numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{"n": {}})
	defer func() {
		FPCache.Stop()
		FPCache = old
	}()

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
