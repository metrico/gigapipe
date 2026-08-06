package grpc

import (
	"net"

	"github.com/metrico/qryn/v5/writer/utils/logger"
	"google.golang.org/grpc"
)

// NewServer builds a gRPC server with all three OTLP signal handlers
// (traces, logs, profiles) registered.
func NewServer() *grpc.Server {
	s := grpc.NewServer()
	registerServices(s)
	return s
}

// Start listens on addr and serves in a goroutine, returning the server for
// shutdown. An empty addr disables the receiver and returns (nil, nil). A
// listen error is returned to the caller.
func Start(addr string) (*grpc.Server, error) {
	if addr == "" {
		return nil, nil
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := NewServer()
	go func() {
		logger.Info("OTLP gRPC receiver listening on", addr)
		if err := s.Serve(lis); err != nil {
			logger.Error("OTLP gRPC serve error:", err)
		}
	}()
	return s, nil
}
