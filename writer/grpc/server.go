package grpc

import (
	"context"

	"github.com/metrico/qryn/v5/writer/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	// Register the gzip compressor codec. grpc-go compressors are wired up
	// purely via package init() side effects, and nothing else in this
	// binary's dependency graph pulls this package in. The OpenTelemetry
	// Collector's OTLP gRPC exporter compresses with gzip BY DEFAULT
	// (otlpexporter's factory sets clientCfg.Compression =
	// configcompression.TypeGzip), so without this import every export from
	// a default-configured collector fails with:
	//   Unimplemented: grpc: Decompressor is not installed for
	//   grpc-encoding "gzip"
	// This import looks unused to static analysis and to a casual reader —
	// do NOT remove it as part of an "unused import" cleanup. Removing it
	// silently breaks all default collector exports; that is exactly why
	// there is a regression test (TestGRPCTraces_EndToEnd_Gzip) guarding it.
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

// maxRecvMsgSize is the maximum size of a single gRPC request message this
// server will accept. It mirrors the 10 MB limit enforced on the OTLP/HTTP
// ingest path (see writer/controller/middleware.go:122's
// `uncompressedLen > 10*1024*1024` check) so that a batch accepted over
// OTLP/HTTP is not rejected over OTLP/gRPC purely because grpc-go's own
// default (4 MiB) is smaller. Keep these two limits in sync if either
// changes.
const maxRecvMsgSize = 10 << 20 // 10 MB

// recoveryInterceptor recovers from panics in unary handlers, logs them, and
// returns codes.Internal — mirroring the HTTP ErrorHandler so a panicking
// handler cannot crash the receiver.
func recoveryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("OTLP gRPC handler panic:", r)
			err = status.Errorf(codes.Internal, "panic: %v", r)
			resp = nil
		}
	}()
	return handler(ctx, req)
}

// NewServer builds a gRPC server with all three OTLP signal handlers
// (traces, logs, profiles) registered.
func NewServer() *grpc.Server {
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(recoveryInterceptor),
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
	)
	registerServices(s)
	return s
}
