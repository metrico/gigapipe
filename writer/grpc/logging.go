package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/writer/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// loggingInterceptor emits one access-log line per unary RPC, mirroring the
// shape of the HTTP access log configured at cmd/gigapipe/main.go's start()
// ("[{{.status}}] {{.method}} {{.url}} - LAT:{{.latency}}"), e.g.:
//
//	[OK] gRPC /opentelemetry.proto.collector.trace.v1.TraceService/Export - LAT:1.2ms
//
// It intentionally does not reuse LoggingMiddleware's text/template
// machinery (reader/utils/middleware/logging.go): that machinery is
// HTTP-shaped (url, referer, user_agent, response length) and lives in the
// reader package, which this package does not depend on.
func loggingInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	d := time.Since(start)
	logger.Info(formatAccessLog(info.FullMethod, status.Code(err), d))
	return resp, err
}

// streamLoggingInterceptor is loggingInterceptor's counterpart for streaming
// RPCs: one access-log line per stream, emitted when the stream ends, with
// the latency spanning the stream's whole lifetime.
func streamLoggingInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss)
	logger.Info(formatAccessLog(info.FullMethod, status.Code(err), time.Since(start)))
	return err
}

// formatAccessLog builds the access-log line for one gRPC call. It is a pure
// function, factored out of loggingInterceptor so it can be unit-tested
// without capturing logger output.
func formatAccessLog(fullMethod string, code codes.Code, d time.Duration) string {
	return fmt.Sprintf("[%s] gRPC %s - LAT:%s", code, fullMethod, d)
}
