package controller

import (
	"fmt"

	"github.com/metrico/qryn/v5/writer/service"
)

// InsertServices bundles the per-tenant insert services the OTLP write path
// needs. It is the single source of these lookups for both the HTTP
// middleware and the gRPC receiver.
type InsertServices struct {
	Ts        service.IInsertServiceV2
	Spl       service.IInsertServiceV2
	SpanAttrs service.IInsertServiceV2
	Spans     service.IInsertServiceV2
	Profile   service.IInsertServiceV2
	Node      string
}

// ResolveInsertServices looks up all insert services for a tenant DSN.
func ResolveInsertServices(dsn string) (InsertServices, error) {
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
	if s.Profile, err = Registry.GetProfileInsertService(dsn); err != nil {
		return s, err
	}
	if s.SpanAttrs, err = Registry.GetSpansSeriesService(dsn); err != nil {
		return s, err
	}
	if s.Spans, err = Registry.GetSpansService(dsn); err != nil {
		return s, err
	}
	s.Node = s.Spans.GetNodeName()
	return s, nil
}
