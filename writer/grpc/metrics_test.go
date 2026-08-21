package grpc

import (
	"context"
	"testing"

	"github.com/metrico/qryn/v5/writer/controller"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestMetricsExport_NoTenantErrors asserts that with no service registry
// configured, Export returns an InvalidArgument status rather than panicking.
func TestMetricsExport_NoTenantErrors(t *testing.T) {
	saved := controller.Registry
	controller.Registry = nil
	defer func() { controller.Registry = saved }()

	srv := &metricsServer{}
	_, err := srv.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err == nil {
		t.Fatal("expected error without registry")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}
