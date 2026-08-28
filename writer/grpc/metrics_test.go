package grpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/promise"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
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

// failingSvc is an insert service whose every push fails with a transient
// backend error, standing in for a ClickHouse outage.
type failingSvc struct{ err error }

func (f *failingSvc) Request(helpers.SizeGetter, int) *promise.Promise[uint32] {
	return promise.Fulfilled[uint32](f.err, 0)
}
func (f *failingSvc) Run()                     {}
func (f *failingSvc) Stop()                    {}
func (f *failingSvc) Ping() (time.Time, error) { return time.Time{}, nil }
func (f *failingSvc) GetState(int) int         { return service.INSERT_STATE_IDLE }
func (f *failingSvc) GetNodeName() string      { return "n" }
func (f *failingSvc) Init()                    {}
func (f *failingSvc) PlanFlush()               {}

// failingRegistry hands out the same failing insert service for every signal.
type failingRegistry struct{ svc *failingSvc }

func (r *failingRegistry) GetTimeSeriesService(string) (service.IInsertServiceV2, error) {
	return r.svc, nil
}
func (r *failingRegistry) GetSamplesService(string) (service.IInsertServiceV2, error) {
	return r.svc, nil
}
func (r *failingRegistry) GetMetricsService(string) (service.IInsertServiceV2, error) {
	return r.svc, nil
}
func (r *failingRegistry) GetSpansService(string) (service.IInsertServiceV2, error) {
	return r.svc, nil
}
func (r *failingRegistry) GetSpansSeriesService(string) (service.IInsertServiceV2, error) {
	return r.svc, nil
}
func (r *failingRegistry) GetProfileInsertService(string) (service.IInsertServiceV2, error) {
	return r.svc, nil
}
func (r *failingRegistry) GetPatternInsertService(string) (service.IInsertServiceV2, error) {
	return r.svc, nil
}
func (r *failingRegistry) Run()  {}
func (r *failingRegistry) Stop() {}

func sampleMetricsRequest() *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{{
			Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
				Key:   "service.name",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
			}}},
			ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{{
				Name: "g",
				Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{
					DataPoints: []*metricsv1.NumberDataPoint{{
						TimeUnixNano: 1_700_000_000_000_000_000,
						Value:        &metricsv1.NumberDataPoint_AsInt{AsInt: 1},
					}},
				}},
			}}}},
		}},
	}
}

// TestMetricsExport_TransientFailureRetryableAndOpaque asserts two properties
// of the status returned for an ingest-path failure. First, the code must be
// retryable per the OTLP spec: codes.Internal is in the client's
// MUST-NOT-retry set, so returning it would turn a recoverable ClickHouse
// outage into permanent data loss at the collector. Second, the message must
// not echo the backend error, which carries the ClickHouse DSN — node name,
// connection string, and any credentials embedded in it.
func TestMetricsExport_TransientFailureRetryableAndOpaque(t *testing.T) {
	installFPCache(t, "n")
	installConfig(t)
	old := controller.Registry
	controller.Registry = &failingRegistry{svc: &failingSvc{
		err: errors.New("n-clickhouse://user:secret@ch-1.internal:9000/gigapipe?secure=false: dial: connection refused"),
	}}
	t.Cleanup(func() { controller.Registry = old })

	_, err := (&metricsServer{}).Export(context.Background(), sampleMetricsRequest())
	if err == nil {
		t.Fatal("expected an error when every insert fails")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("transient ingest failure returned code %s, want %s", st.Code(), codes.Unavailable)
	}
	if strings.Contains(st.Message(), "clickhouse://") || strings.Contains(st.Message(), "secret") {
		t.Errorf("gRPC status leaks backend connection details to the client: %q", st.Message())
	}
}
