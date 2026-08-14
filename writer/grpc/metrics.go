package grpc

import (
	"context"

	"github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// metricsServer implements the OTLP MetricsService gRPC handler, converting
// the already-decoded ExportMetricsServiceRequest into the shared
// parse/ingest path.
type metricsServer struct {
	colmetricspb.UnimplementedMetricsServiceServer
}

// Export resolves the tenant's metric insert services from the request
// metadata and ingests the pre-decoded metrics through the transport-agnostic
// core. Data points dropped by ingest policy (delta temporality, invalid
// points) are reported via partial_success rather than failing the request.
func (s *metricsServer) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	dsn, ctx := dsnCtx(ctx)
	svcs, err := controller.ResolveLogServices(dsn)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	md := &metricsv1.MetricsData{ResourceMetrics: req.ResourceMetrics}
	stats := &unmarshal.OTLPMetricsStats{}
	parser := controller.Parser(unmarshal.OTLPMetricsFromData(md, stats))
	if err := controller.IngestParsed(ctx, parser, svcs); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected := stats.RejectedDataPoints(); rejected > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage:       stats.ErrorMessage(),
		}
	}
	return resp, nil
}

// registerMetrics wires the metrics handler onto a gRPC server.
func registerMetrics(g *grpc.Server) {
	colmetricspb.RegisterMetricsServiceServer(g, &metricsServer{})
}
