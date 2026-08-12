package ruler

import (
	"context"
	"maps"

	writerController "github.com/metrico/qryn/v5/writer/controller"
	writerutils "github.com/metrico/qryn/v5/writer/utils"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// vectorToWriteRequest turns an evaluated recording-rule vector into a
// Prometheus remote-write request. Each sample becomes a single-point series
// whose labels are the source sample's labels with the rule's static labels
// added on top and __name__ set to the record name. On a name collision the
// rule label takes precedence over the sample label, and the record name takes
// precedence over both. Fingerprinting happens downstream in the writer's
// metrics parser and is order-independent, so labels are not sorted here.
func vectorToWriteRequest(record string, ruleLabels map[string]string, v promql.Vector) *prompb.WriteRequest {
	wr := &prompb.WriteRequest{}
	for _, sample := range v {
		merged := make(map[string]string)
		sample.Metric.Range(func(l labels.Label) {
			merged[l.Name] = l.Value
		})
		maps.Copy(merged, ruleLabels)
		merged["__name__"] = record

		lbls := make([]*prompb.Label, 0, len(merged))
		for k, val := range merged {
			lbls = append(lbls, &prompb.Label{Name: k, Value: val})
		}
		wr.Timeseries = append(wr.Timeseries, &prompb.TimeSeries{
			Labels:  lbls,
			Samples: []*prompb.Sample{{Value: sample.F, Timestamp: sample.T}},
		})
	}
	return wr
}

// inProcessWriter writes recording-rule results back through the writer's
// static insert registry and metrics parser, without HTTP, snappy, auth, or a
// remote-write loopback. See docs/adr/0001.
type inProcessWriter struct{}

// NewInProcessWriter returns the default RecordingRuleWriter, which pushes
// results straight into the writer's metrics pipeline. The writer module must
// be initialized first (its registry and fingerprint cache must be ready).
func NewInProcessWriter() RecordingRuleWriter {
	return inProcessWriter{}
}

func (inProcessWriter) Write(oid, record string, ruleLabels map[string]string, v promql.Vector) error {
	if len(v) == 0 {
		return nil
	}
	// Tag the write-back with the owning tenant so recorded series land under the
	// right oid. The writer's unmarshal layer reads ContextKeyOid; the value is
	// "" (ignored) when federation is off.
	ctx := context.WithValue(context.Background(), writerutils.ContextKeyOid, oid)
	return writerController.PushPromWriteRequest(ctx, vectorToWriteRequest(record, ruleLabels, v))
}
