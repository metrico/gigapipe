package grpc

import (
	"context"

	"github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// traceServer implements the OTLP TraceService gRPC handler, converting the
// already-decoded ExportTraceServiceRequest into the shared parse/ingest path.
type traceServer struct {
	coltracepb.UnimplementedTraceServiceServer
}

// Export resolves the tenant's trace insert services from the request metadata
// and ingests the pre-decoded spans through the transport-agnostic core.
func (s *traceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	dsn, ctx := dsnCtx(ctx)
	svcs, err := controller.ResolveTraceServices(dsn)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	td := &tracev1.TracesData{ResourceSpans: req.ResourceSpans}
	// Body is nil: the *FromData parsers are built with withPreParsedBody,
	// so the payload is already supplied and no reader is ever read.
	parser := controller.Bind(controller.Parser(unmarshal.OTLPTracesFromData(td)), nil)
	if err := controller.IngestParsed(ctx, parser, svcs); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// registerTrace wires the trace handler onto a gRPC server. Task 9 calls this
// from registerServices; NewServer stays untouched here.
func registerTrace(g *grpc.Server) {
	coltracepb.RegisterTraceServiceServer(g, &traceServer{})
}
