package grpc

import (
	"context"
	"testing"

	"github.com/metrico/qryn/v5/writer/controller"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestLogsExport_NoTenantErrors asserts that with no service registry
// configured, Export returns an InvalidArgument status rather than panicking.
func TestLogsExport_NoTenantErrors(t *testing.T) {
	saved := controller.Registry
	controller.Registry = nil
	defer func() { controller.Registry = saved }()

	srv := &logsServer{}
	_, err := srv.Export(context.Background(), &collogspb.ExportLogsServiceRequest{})
	if err == nil {
		t.Fatal("expected error without registry")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}
