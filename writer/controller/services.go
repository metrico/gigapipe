package controller

import (
	"fmt"

	"github.com/metrico/qryn/v5/writer/service"
)

// InsertServices bundles per-tenant insert services. Only the fields a given
// signal needs are populated; the rest stay nil (IngestParsed/doPush are
// nil-safe). SIGNAL ISOLATION: each signal resolves ONLY its own services —
// logs never resolve spans services, traces never resolve samples, etc.
type InsertServices struct {
	Ts        service.IInsertServiceV2
	Spl       service.IInsertServiceV2
	SpanAttrs service.IInsertServiceV2
	Spans     service.IInsertServiceV2
	Profile   service.IInsertServiceV2

	MetricStaging   service.IInsertServiceV2
	MetricSeries    service.IInsertServiceV2
	MetricMetadata  service.IInsertServiceV2
	MetricExemplars service.IInsertServiceV2

	Node string
}

// ResolveTraceServices resolves only the trace insert services for a tenant.
func ResolveTraceServices(dsn string) (InsertServices, error) {
	var s InsertServices
	if Registry == nil {
		return s, fmt.Errorf("service registry not initialized")
	}
	var err error
	if s.SpanAttrs, err = Registry.GetSpansSeriesService(dsn); err != nil {
		return s, err
	}
	if s.Spans, err = Registry.GetSpansService(dsn); err != nil {
		return s, err
	}
	s.Node = s.Spans.GetNodeName()
	return s, nil
}

// ResolveLogServices resolves only the log insert services for a tenant.
func ResolveLogServices(dsn string) (InsertServices, error) {
	var s InsertServices
	if Registry == nil {
		return s, fmt.Errorf("service registry not initialized")
	}
	var err error
	if s.Spl, err = Registry.GetSamplesService(dsn); err != nil {
		return s, err
	}
	if s.Ts, err = Registry.GetTimeSeriesService(dsn); err != nil {
		return s, err
	}
	// Node keys the fingerprint cache, which gates time_series writes — rows
	// that go to Ts, so the cache must be namespaced by Ts's node.
	s.Node = s.Ts.GetNodeName()
	return s, nil
}

// ResolveMetricServices resolves the four metric insert services for a
// tenant, all on the staging service's node, which also keys the metric caches.
func ResolveMetricServices(dsn string) (InsertServices, error) {
	var s InsertServices
	if Registry == nil {
		return s, fmt.Errorf("service registry not initialized")
	}
	var err error
	if s.MetricStaging, err = Registry.GetMetricStagingService(dsn); err != nil {
		return s, err
	}
	s.Node = s.MetricStaging.GetNodeName()
	if s.MetricSeries, err = Registry.GetMetricSeriesService(s.Node); err != nil {
		return s, err
	}
	if s.MetricMetadata, err = Registry.GetMetricMetadataService(s.Node); err != nil {
		return s, err
	}
	if s.MetricExemplars, err = Registry.GetMetricExemplarsService(s.Node); err != nil {
		return s, err
	}
	return s, nil
}

// ResolveLogAndMetricServices resolves the log and the metric insert services
// for a source that carries both, all on the staging service's node.
func ResolveLogAndMetricServices(dsn string) (InsertServices, error) {
	s, err := ResolveMetricServices(dsn)
	if err != nil {
		return s, err
	}
	if s.Spl, err = Registry.GetSamplesService(s.Node); err != nil {
		return s, err
	}
	if s.Ts, err = Registry.GetTimeSeriesService(s.Node); err != nil {
		return s, err
	}
	return s, nil
}

// ResolveProfileServices resolves only the profile insert service for a tenant.
func ResolveProfileServices(dsn string) (InsertServices, error) {
	var s InsertServices
	if Registry == nil {
		return s, fmt.Errorf("service registry not initialized")
	}
	var err error
	if s.Profile, err = Registry.GetProfileInsertService(dsn); err != nil {
		return s, err
	}
	s.Node = s.Profile.GetNodeName()
	return s, nil
}
