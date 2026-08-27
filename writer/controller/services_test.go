package controller

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/service/registry"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/promise"
)

func TestResolveServices_RequireRegistry(t *testing.T) {
	// With a nil Registry each resolver must return an error, not panic.
	old := Registry
	Registry = nil
	defer func() { Registry = old }()
	for _, fn := range []func(string) (InsertServices, error){
		ResolveTraceServices, ResolveLogServices, ResolveProfileServices,
	} {
		if _, err := fn("dsn"); err == nil {
			t.Fatal("expected error when Registry is nil")
		}
	}
}

// namedSvc is a no-op insert service with a fixed node name, for asserting
// which node a resolver picks.
type namedSvc struct{ node string }

func (s *namedSvc) Request(helpers.SizeGetter, int) *promise.Promise[uint32] {
	return promise.Fulfilled[uint32](nil, 0)
}
func (s *namedSvc) Run()                     {}
func (s *namedSvc) Stop()                    {}
func (s *namedSvc) Ping() (time.Time, error) { return time.Time{}, nil }
func (s *namedSvc) GetState(int) int         { return 0 }
func (s *namedSvc) GetNodeName() string      { return s.node }
func (s *namedSvc) Init()                    {}
func (s *namedSvc) PlanFlush()               {}

// TestResolveLogServicesNodeFollowsTimeSeries pins Node to the time-series
// service's node. Node namespaces the fingerprint cache (FPCache.DB(Node) in
// IngestParsed), and that cache gates exactly one thing: whether a series'
// time_series row is appended — rows that are pushed to Ts. With an empty
// DSN the static registry picks an independent random service per getter
// call, so samples and time series can resolve to different nodes within one
// request; keying the cache to the samples node would then mark fingerprints
// seen on a node that never received the time_series row, silently losing it
// there until the cache TTL resets.
func TestResolveLogServicesNodeFollowsTimeSeries(t *testing.T) {
	old := Registry
	Registry = registry.NewStaticServiceRegistry(registry.StaticServiceRegistryOpts{
		SamplesSvcs: map[string]service.IInsertServiceV2{
			"spl-node": &namedSvc{node: "spl-node"},
		},
		TimeSeriesSvcs: map[string]service.IInsertServiceV2{
			"ts-node": &namedSvc{node: "ts-node"},
		},
	})
	t.Cleanup(func() { Registry = old })

	svcs, err := ResolveLogServices("")
	if err != nil {
		t.Fatal(err)
	}
	if svcs.Node != "ts-node" {
		t.Errorf("Node=%q, want %q: the fingerprint cache must be namespaced by the node receiving time_series rows", svcs.Node, "ts-node")
	}
}
