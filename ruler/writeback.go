package ruler

import (
	"context"
	"fmt"

	writerController "github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// recordedVector names an evaluated vector's samples for write-back: each
// keeps its labels under the rule's labels and the record name, its value,
// and is stamped at ts. Two samples sharing a label set are an error.
func recordedVector(record string, ruleLabels map[string]string, v promql.Vector, ts int64) (promql.Vector, error) {
	out := make(promql.Vector, 0, len(v))
	seen := make(map[uint64]struct{}, len(v))
	b := labels.NewBuilder(labels.EmptyLabels())
	for _, sample := range v {
		b.Reset(sample.Metric)
		for k, val := range ruleLabels {
			b.Set(k, val)
		}
		b.Set(labels.MetricName, record)
		lbls := b.Labels()
		h := lbls.Hash()
		if _, dup := seen[h]; dup {
			return nil, fmt.Errorf("vector contains metrics with the same labelset after applying rule labels: %s", lbls)
		}
		seen[h] = struct{}{}
		out = append(out, promql.Sample{Metric: lbls, T: ts, F: sample.F})
	}
	return out, nil
}

// inProcessWriter writes recorded samples through the writer's metric entry
// point in-process, without HTTP, snappy or auth.
type inProcessWriter struct{}

// NewInProcessWriter returns the default RecordingRuleWriter. The writer
// module must be initialized first (its registry and caches must be ready).
func NewInProcessWriter() RecordingRuleWriter {
	return inProcessWriter{}
}

func (inProcessWriter) Write(v promql.Vector) error {
	if len(v) == 0 {
		return nil
	}
	series := make([]unmarshal.MetricSeries, 0, len(v))
	for _, sample := range v {
		lbls := make([][]string, 0, sample.Metric.Len())
		sample.Metric.Range(func(l labels.Label) {
			lbls = append(lbls, []string{l.Name, l.Value})
		})
		series = append(series, unmarshal.MetricSeries{
			Labels:       lbls,
			TimestampsMs: []int64{sample.T},
			Values:       []float64{sample.F},
		})
	}
	return writerController.PushMetricSeries(context.Background(), series)
}
