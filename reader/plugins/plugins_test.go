package plugins_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
)

// metricLabelsPlugin answers every metric label endpoint with fixed results.
type metricLabelsPlugin struct{ sd *model.ServiceData }

func (p *metricLabelsPlugin) SetServiceData(sd *model.ServiceData) { p.sd = sd }

func (p *metricLabelsPlugin) LabelNames(context.Context, model.MetricIndexQuery) ([]string, bool, error) {
	return []string{"plugin_name"}, true, nil
}

func (p *metricLabelsPlugin) LabelValues(_ context.Context, name string, _ model.MetricIndexQuery) ([]string, bool, error) {
	return []string{"plugin_" + name}, false, nil
}

func (p *metricLabelsPlugin) Series(context.Context, model.MetricIndexQuery) ([]map[string]string, bool, error) {
	return []map[string]string{{"__name__": "plugin_series"}}, false, nil
}

func (p *metricLabelsPlugin) Metadata(_ context.Context, metric string, _ int) (map[string][]model.MetricMetadata, error) {
	return map[string][]model.MetricMetadata{metric: {{Type: "counter"}}}, nil
}

func (p *metricLabelsPlugin) Exemplars(context.Context, model.MetricIndexQuery) ([]model.ExemplarSeries, error) {
	return []model.ExemplarSeries{{SeriesLabels: map[string]string{"__name__": "plugin_exemplar"}}}, nil
}

func TestRegisteredMetricLabelsServicePluginAnswersTheLabelEndpoints(t *testing.T) {
	p := &metricLabelsPlugin{}
	plugins.RegisterMetricLabelsServicePlugin(p)
	sd := &model.ServiceData{Session: fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		return fakeclickhouse.Result{}, errors.New("the default service ran " + query)
	})}
	svc := service.NewMetricLabelsService(sd)
	if p.sd != sd {
		t.Fatal("the plugin did not get the service data")
	}
	ctx, q := context.Background(), model.MetricIndexQuery{}
	check := func(endpoint string, got, want any) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", endpoint, got, want)
		}
	}
	names, truncated, err := svc.LabelNames(ctx, q)
	check("labels", []any{names, truncated, err}, []any{[]string{"plugin_name"}, true, nil})
	values, truncated, err := svc.LabelValues(ctx, "job", q)
	check("label values", []any{values, truncated, err}, []any{[]string{"plugin_job"}, false, nil})
	series, truncated, err := svc.Series(ctx, q)
	check("series", []any{series, truncated, err},
		[]any{[]map[string]string{{"__name__": "plugin_series"}}, false, nil})
	meta, err := svc.Metadata(ctx, "up", 1)
	check("metadata", []any{meta, err}, []any{map[string][]model.MetricMetadata{"up": {{Type: "counter"}}}, nil})
	ex, err := svc.Exemplars(ctx, q)
	check("exemplars", []any{ex, err},
		[]any{[]model.ExemplarSeries{{SeriesLabels: map[string]string{"__name__": "plugin_exemplar"}}}, nil})
}
