package grpc

import (
	"context"
	"strconv"

	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils"
	"google.golang.org/grpc/metadata"
)

// asyncMode maps the x-async-insert metadata value to the int insert-mode,
// mirroring getAsyncMode in the HTTP middleware.
func asyncMode(v string) int {
	switch v {
	case "0":
		return service.INSERT_MODE_SYNC
	case "1":
		return service.INSERT_MODE_ASYNC
	default:
		return service.INSERT_MODE_DEFAULT
	}
}

// mdFirst returns the first value for key in md, or "" if absent.
func mdFirst(md metadata.MD, key string) string {
	if vs := md.Get(key); len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// dsnCtx extracts the tenant DSN from the incoming gRPC metadata (x-ch-dsn) and
// propagates the parity metadata keys into context under the same ContextKey*
// keys the HTTP middleware uses, returning the DSN and the enriched context.
//
// Handlers (Tasks 7-9) call dsnCtx(ctx) and then the signal-scoped resolver
// directly, e.g. controller.ResolveTraceServices(dsn); there is deliberately no
// shared all-signals resolver here (signal isolation).
func dsnCtx(ctx context.Context) (string, context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	dsn := mdFirst(md, "x-ch-dsn")
	ctx = context.WithValue(ctx, utils.ContextKeyDSN, dsn)
	ctx = context.WithValue(ctx, utils.ContextKeyMeta, mdFirst(md, "x-scope-meta"))
	// Mirror getAsyncMode: store the int insert-mode (not the raw string), so a
	// downstream reader type-asserting .(int) does not panic.
	ctx = context.WithValue(ctx, utils.ContextKeyAsync, asyncMode(mdFirst(md, "x-async-insert")))

	// Mirror WithOverallContextMiddleware: parse x-ttl-days to uint16, default
	// 0 on empty/parse error. Stored as uint16 (not string) because the write
	// path type-asserts it: builder.go does p.ttlDays = ttlDays.(uint16).
	ttlDays := uint16(0)
	if strTTLDays := mdFirst(md, "x-ttl-days"); strTTLDays != "" {
		if iTTLDays, err := strconv.ParseUint(strTTLDays, 10, 16); err == nil {
			ttlDays = uint16(iTTLDays)
		}
	}
	ctx = context.WithValue(ctx, utils.ContextKeyTTLDays, ttlDays)

	return dsn, ctx
}
