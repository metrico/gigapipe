package grpc

import (
	"context"

	"github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// logsServer implements the OTLP LogsService gRPC handler, converting the
// already-decoded ExportLogsServiceRequest into the shared parse/ingest path.
type logsServer struct {
	collogspb.UnimplementedLogsServiceServer
}

// Export resolves the tenant's log insert services from the request metadata
// and ingests the pre-decoded log records through the transport-agnostic core.
func (s *logsServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	dsn, ctx := dsnCtx(ctx)
	svcs, err := controller.ResolveLogServices(dsn)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ld := &logsv1.LogsData{ResourceLogs: req.ResourceLogs}
	// Body is nil: the *FromData parsers are built with withPreParsedBody,
	// so the payload is already supplied and no reader is ever read.
	parser := controller.Bind(controller.Parser(unmarshal.OTLPLogsFromData(ld)), nil)
	if err := controller.IngestParsed(ctx, parser, svcs); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// registerLogs wires the logs handler onto a gRPC server. Task 9 calls this
// from registerServices; NewServer stays untouched here.
func registerLogs(g *grpc.Server) {
	collogspb.RegisterLogsServiceServer(g, &logsServer{})
}
