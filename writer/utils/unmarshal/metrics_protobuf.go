package unmarshal

import (
	"strings"

	"github.com/metrico/qryn/v5/writer/metric"
	"github.com/metrico/qryn/v5/writer/utils/metadata"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"google.golang.org/protobuf/proto"
)

// RejectRemoteWriteNativeHistogram is the counted reason for a dropped
// remote-write native histogram sample.
const RejectRemoteWriteNativeHistogram = "remote_write_native_histogram"

type promMetricsProtoDec struct {
	ctx              *ParserCtx
	onMetricSamples  onMetricSamplesHandler
	onMetricMetadata onMetricMetadataHandler
}

func protoLabels(lbls []*prompb.Label) [][]string {
	res := make([][]string, 0, len(lbls))
	for _, l := range lbls {
		res = append(res, []string{l.GetName(), l.GetValue()})
	}
	return res
}

func (l *promMetricsProtoDec) Decode() error {
	req := l.ctx.bodyObject.(*prompb.WriteRequest)
	for _, ts := range req.GetTimeseries() {
		if n := len(ts.GetHistograms()); n > 0 {
			metric.IngestRejected.WithLabelValues(RejectRemoteWriteNativeHistogram).Add(float64(n))
		}
		if len(ts.GetSamples()) == 0 {
			continue
		}
		labels := sanitizeLabels(protoLabels(ts.GetLabels()))
		tsMs := make([]int64, 0, len(ts.GetSamples()))
		values := make([]float64, 0, len(ts.GetSamples()))
		for _, spl := range ts.GetSamples() {
			tsMs = append(tsMs, spl.GetTimestamp())
			values = append(values, spl.GetValue())
		}
		var exemplars []metricExemplar
		for _, ex := range ts.GetExemplars() {
			exemplars = append(exemplars, metricExemplar{
				labels: protoLabels(ex.GetLabels()),
				tsMs:   ex.GetTimestamp(),
				value:  ex.GetValue(),
			})
		}
		if err := l.onMetricSamples(labels, tsMs, values, exemplars); err != nil {
			return err
		}
	}
	for _, m := range req.GetMetadata() {
		err := l.onMetricMetadata(m.GetMetricFamilyName(), metadata.Entry{
			Type: strings.ToLower(m.GetType().String()),
			Help: m.GetHelp(),
			Unit: m.GetUnit(),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *promMetricsProtoDec) SetOnMetricSamples(h onMetricSamplesHandler) {
	l.onMetricSamples = h
}

func (l *promMetricsProtoDec) SetOnMetricMetadata(h onMetricMetadataHandler) {
	l.onMetricMetadata = h
}

var UnmarshallMetricsWriteProtoV2 = Build(
	withBufferedBody,
	withParsedBody(func() proto.Message { return &prompb.WriteRequest{} }),
	withMetricsParser(func(ctx *ParserCtx) iMetricsParser { return &promMetricsProtoDec{ctx: ctx} }))
