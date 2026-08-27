package grpc

import (
	"context"

	"github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
	pprofileotlp "go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// profilesServer implements the OTLP ProfilesService gRPC handler, converting the
// already-decoded ExportRequest into the shared parse/ingest path.
type profilesServer struct {
	pprofileotlp.UnimplementedGRPCServer
}

// Export resolves the tenant's profile insert services from the request metadata
// and ingests the pre-decoded profiles through the transport-agnostic core.
func (s *profilesServer) Export(ctx context.Context, req pprofileotlp.ExportRequest) (pprofileotlp.ExportResponse, error) {
	dsn, ctx := dsnCtx(ctx)
	svcs, err := controller.ResolveProfileServices(dsn)
	if err != nil {
		return pprofileotlp.NewExportResponse(), status.Error(codes.InvalidArgument, err.Error())
	}
	// Body is nil: the *FromData parsers are built with withPreParsedBody,
	// so the payload is already supplied and no reader is ever read.
	parser := controller.Bind(controller.Parser(unmarshal.OTLPProfilesFromProfiles(req.Profiles())), nil)
	if err := controller.IngestParsed(ctx, parser, svcs); err != nil {
		return pprofileotlp.NewExportResponse(), status.Error(codes.Internal, err.Error())
	}
	return pprofileotlp.NewExportResponse(), nil
}

// registerProfiles wires the profiles handler onto a gRPC server. It is called
// from registerServices, which NewServer invokes.
func registerProfiles(g *grpc.Server) {
	pprofileotlp.RegisterGRPCServer(g, &profilesServer{})
}
