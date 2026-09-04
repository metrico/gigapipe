package grpc

import (
	"context"
	"testing"

	"github.com/metrico/qryn/v5/writer/controller"
	pprofileotlp "go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestProfilesExport_NoTenantErrors verifies that with no tenant registry the
// profiles handler surfaces an InvalidArgument gRPC error rather than panicking.
func TestProfilesExport_NoTenantErrors(t *testing.T) {
	saved := controller.Registry
	controller.Registry = nil
	defer func() { controller.Registry = saved }()

	srv := &profilesServer{}
	_, err := srv.Export(context.Background(), pprofileotlp.NewExportRequest())
	if err == nil {
		t.Fatal("expected error without registry")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}
