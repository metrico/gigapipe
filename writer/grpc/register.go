package grpc

import "google.golang.org/grpc"

// registerServices wires all three OTLP signal handlers onto the gRPC server.
func registerServices(s *grpc.Server) {
	registerTrace(s)
	registerLogs(s)
	registerProfiles(s)
}
