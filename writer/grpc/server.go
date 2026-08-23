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

// streamRecoveryInterceptor is recoveryInterceptor's counterpart for
// streaming RPCs: a panic anywhere in the stream handler becomes
// codes.Internal instead of killing the receiver.
func streamRecoveryInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("OTLP gRPC handler panic:", r)
			err = status.Errorf(codes.Internal, "panic: %v", r)
		}
	}()
	return handler(srv, ss)
}

// Options carries the gRPC server's runtime configuration. It is an explicit
// struct rather than positional arguments so future additions (e.g. TLS)
// don't churn NewServer's and Mux's signatures again.
type Options struct {
	// BasicAuthUser and BasicAuthPass gate the auth interceptor (see auth.go).
	// Auth is enabled only when BOTH are non-empty, mirroring the gate on
	// BasicAuthMiddleware at cmd/gigapipe/main.go's start().
	BasicAuthUser string
	BasicAuthPass string
}

// NewServer builds a gRPC server with all three OTLP signal handlers
// (traces, logs, profiles) registered.
//
// Interceptor chain, outermost first:
//
//	loggingInterceptor, recoveryInterceptor, authInterceptor (auth optional)
//
// Order rationale:
//   - logging outermost, so a request rejected by auth still appears in the
//     access log;
//   - recovery inside logging but outside auth, so a panic anywhere below it
//     — auth included — becomes codes.Internal instead of killing the
//     process, and is still logged by the outer logging interceptor;
//   - auth innermost, so no business handler ever runs for an unauthenticated
//     request.
//
// The stream chain mirrors the unary one interceptor for interceptor, in the
// same order. Unary interceptors never fire for streaming RPCs, so without
// the mirror any streaming method a service registers would skip logging,
// recovery, and — critically — auth. Keep the two chains in lockstep.
func NewServer(opts Options) *grpc.Server {
	unary := []grpc.UnaryServerInterceptor{loggingInterceptor, recoveryInterceptor}
	stream := []grpc.StreamServerInterceptor{streamLoggingInterceptor, streamRecoveryInterceptor}
	if opts.BasicAuthUser != "" && opts.BasicAuthPass != "" {
		unary = append(unary, newAuthInterceptor(opts.BasicAuthUser, opts.BasicAuthPass))
		stream = append(stream, newAuthStreamInterceptor(opts.BasicAuthUser, opts.BasicAuthPass))
	}
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
	)
	registerServices(s)
	return s
}
