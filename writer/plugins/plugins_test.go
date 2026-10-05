package plugins_test

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/plugins"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/service/insert"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/promise"
)

// pluginSvc is an insert service a plugin supplies.
type pluginSvc struct{ opts model.InsertServiceOpts }

func (*pluginSvc) Request(helpers.SizeGetter, int) *promise.Promise[uint32] { return nil }
func (*pluginSvc) Run()                                                     {}
func (*pluginSvc) Stop()                                                    {}
func (*pluginSvc) Ping() (time.Time, error)                                 { return time.Time{}, nil }
func (*pluginSvc) GetState(int) int                                         { return 0 }
func (*pluginSvc) GetNodeName() string                                      { return "" }
func (*pluginSvc) Init()                                                    {}
func (*pluginSvc) PlanFlush()                                               {}

func TestRegisteredMetricInsertServicePluginsReplaceTheDefaults(t *testing.T) {
	factory := &insert.DevInsertServiceFactory{}
	type newSvc = func(model.InsertServiceOpts) service.IInsertServiceV2
	for _, tc := range []struct {
		name     string
		register func(newSvc)
		build    []newSvc
	}{
		{"staging", func(f newSvc) { plugins.RegisterMetricStagingInsertServicePlugin(f) },
			[]newSvc{insert.NewMetricStagingInsertService, factory.NewMetricStagingInsertService}},
		{"series", func(f newSvc) { plugins.RegisterMetricSeriesInsertServicePlugin(f) },
			[]newSvc{insert.NewMetricSeriesInsertService, factory.NewMetricSeriesInsertService}},
		{"metadata", func(f newSvc) { plugins.RegisterMetricMetadataInsertServicePlugin(f) },
			[]newSvc{insert.NewMetricMetadataInsertService, factory.NewMetricMetadataInsertService}},
		{"exemplars", func(f newSvc) { plugins.RegisterMetricExemplarsInsertServicePlugin(f) },
			[]newSvc{insert.NewMetricExemplarsInsertService, factory.NewMetricExemplarsInsertService}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.register(func(opts model.InsertServiceOpts) service.IInsertServiceV2 {
				return &pluginSvc{opts: opts}
			})
			opts := model.InsertServiceOpts{Node: &model.DataDatabasesMap{}, ParallelNum: 3}
			for _, build := range tc.build {
				svc, ok := build(opts).(*pluginSvc)
				if !ok {
					t.Fatalf("got %T, want the plugin's service", build(opts))
				}
				if svc.opts.ParallelNum != 3 || svc.opts.Node != opts.Node {
					t.Fatalf("the plugin got opts %+v, want %+v", svc.opts, opts)
				}
			}
		})
	}
}
