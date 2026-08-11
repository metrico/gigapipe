package grpc

import (
	"context"

	"github.com/metrico/qryn/v5/writer/utils/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(recoveryInterceptor))
	registerServices(s)
	return s
}
