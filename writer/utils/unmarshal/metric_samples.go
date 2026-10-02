package unmarshal

import (
	"fmt"
	"math"
	"time"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/metadata"
	"github.com/metrico/qryn/v5/writer/utils/metriccache"
)

type metricExemplar struct {
	labels [][]string
	tsMs   int64
	value  float64
}

type onMetricSamplesHandler func(labels [][]string, timestampsMs []int64, values []float64,
	exemplars []metricExemplar) error

type onMetricMetadataHandler func(name string, m metadata.Entry)

// iMetricSink receives a parser's metric samples. A logs parser that
// implements it feeds onMetricSamples alongside onEntries.
type iMetricSink interface {
	SetOnMetricSamples(h onMetricSamplesHandler)
	SetOnMetricMetadata(h onMetricMetadataHandler)
}

// metricSink holds a metric parser's entry-point handlers.
type metricSink struct {
	onMetricSamples  onMetricSamplesHandler
	onMetricMetadata onMetricMetadataHandler
}

func (s *metricSink) SetOnMetricSamples(h onMetricSamplesHandler) {
	s.onMetricSamples = h
}

func (s *metricSink) SetOnMetricMetadata(h onMetricMetadataHandler) {
	s.onMetricMetadata = h
}

type iMetricsParser interface {
	Decode() error
	iMetricSink
}

type metricSampleKey struct {
	fp   uint64
	tsMs int64
}

// metricBatch accumulates one request's rows for the four metric insert
// services; flush hands them to the parser's response channel.
type metricBatch struct {
	node      *metriccache.Node
	res       chan *model.ParserResponse
	samples   *model.MetricSamplesData
	index     map[metricSampleKey]int
	series    *model.MetricSeriesData
	metadata  *model.MetricMetadataData
	exemplars *model.MetricExemplarsData
}

func newMetricBatch(node *metriccache.Node, res chan *model.ParserResponse) *metricBatch {
	b := &metricBatch{node: node, res: res}
	b.reset()
	return b
}

func (b *metricBatch) reset() {
	b.samples = &model.MetricSamplesData{}
	b.index = make(map[metricSampleKey]int)
	b.series = &model.MetricSeriesData{}
	b.metadata = &model.MetricMetadataData{}
	b.exemplars = &model.MetricExemplarsData{}
}

// flush fills each staging row's predecessor columns in row order and sends
// the batch.
func (b *metricBatch) flush() {
	s := b.samples
	s.MPrevTimestampMs = make([]int64, len(s.MFingerprint))
	s.MPrevValue = make([]float64, len(s.MFingerprint))
	s.MAggregate = make([]uint8, len(s.MFingerprint))
	for i, fp := range s.MFingerprint {
		prev := b.node.Predecessors.Next(fp, s.MTimestampMs[i], s.MValue[i])
		s.MPrevTimestampMs[i], s.MPrevValue[i], s.MAggregate[i] = prev.TimestampMs, prev.Value, prev.Aggregate
	}
	resp := &model.ParserResponse{}
	if len(s.MFingerprint) > 0 {
		resp.MetricSamplesRequest = s
	}
	if len(b.series.MFingerprint) > 0 {
		resp.MetricSeriesRequest = b.series
	}
	if len(b.metadata.MName) > 0 {
		resp.MetricMetadataRequest = b.metadata
	}
	if len(b.exemplars.MFingerprint) > 0 {
		resp.MetricExemplarsRequest = b.exemplars
	}
	if resp.MetricSamplesRequest != nil || resp.MetricSeriesRequest != nil ||
		resp.MetricMetadataRequest != nil || resp.MetricExemplarsRequest != nil {
		b.res <- resp
	}
	b.reset()
}

func (b *metricBatch) addMetadata(name string, m metadata.Entry) {
	if name == "" || !b.node.Metadata.Changed(name, m) {
		return
	}
	d := b.metadata
	d.MName = append(d.MName, name)
	d.MType = append(d.MType, m.Type)
	d.MHelp = append(d.MHelp, m.Help)
	d.MUnit = append(d.MUnit, m.Unit)
	d.MUpdatedAtMs = append(d.MUpdatedAtMs, time.Now().UnixMilli())
	d.Size += 8 + len(name) + len(m.Type) + len(m.Help) + len(m.Unit)
}

func labelValue(labels [][]string, name string) string {
	for _, l := range labels {
		if l[0] == name {
			return l[1]
		}
	}
	return ""
}

func (p *parserDoer) onMetricMetadata(name string, m metadata.Entry) {
	p.metrics.addMetadata(name, m)
}

// onMetricSamples is the metric entry point: it strips __ttl_days__ and the
// __metric_*__ labels, adds service_name, fingerprints the label set and
// batches the series' staging, series, metadata and exemplar rows. A later
// sample of the same (series, ms) in the request replaces the earlier one.
// A series row, when the fingerprint cache asks for one, spans these samples.
func (p *parserDoer) onMetricSamples(labels [][]string, timestampsMs []int64, values []float64,
	exemplars []metricExemplar,
) error {
	if len(timestampsMs) != len(values) {
		return fmt.Errorf("metric samples: %d timestamps, %d values", len(timestampsMs), len(values))
	}
	b := p.metrics
	meta := metadata.ExtractMetadataFromLabels(labels)
	filtered, _ := stripSpecialLabels(labels, 0, true)
	p.discoverServiceName(&filtered)
	fp := fingerprintLabels(filtered)
	name := labelValue(filtered, "__name__")

	if !meta.IsZero() {
		b.addMetadata(name, meta)
	}

	minTs, maxTs := int64(math.MaxInt64), int64(math.MinInt64)
	for _, ts := range timestampsMs {
		minTs, maxTs = min(minTs, ts), max(maxTs, ts)
	}
	if len(timestampsMs) > 0 && b.node.Fingerprints.Emit(fp, minTs, maxTs) {
		lblMap := make(map[string]string, len(filtered))
		size := 24 + len(name)
		for _, l := range filtered {
			lblMap[l[0]] = l[1]
			size += len(l[0]) + len(l[1])
		}
		d := b.series
		d.MName = append(d.MName, name)
		d.MFingerprint = append(d.MFingerprint, fp)
		d.MLabels = append(d.MLabels, lblMap)
		d.MFirstSeenMs = append(d.MFirstSeenMs, minTs)
		d.MLastSeenMs = append(d.MLastSeenMs, maxTs)
		d.Size += size
	}

	s := b.samples
	for i, ts := range timestampsMs {
		key := metricSampleKey{fp, ts}
		if idx, ok := b.index[key]; ok {
			s.MValue[idx] = values[i]
			continue
		}
		b.index[key] = len(s.MFingerprint)
		s.MFingerprint = append(s.MFingerprint, fp)
		s.MTimestampMs = append(s.MTimestampMs, ts)
		s.MValue = append(s.MValue, values[i])
		s.Size += 41
	}

	e := b.exemplars
	for _, ex := range exemplars {
		traceID := labelValue(ex.labels, "trace_id")
		if traceID == "" {
			traceID = labelValue(ex.labels, "traceID")
		}
		encoded := encodeLabels(ex.labels)
		e.MFingerprint = append(e.MFingerprint, fp)
		e.MTimestampMs = append(e.MTimestampMs, ex.tsMs)
		e.MValue = append(e.MValue, ex.value)
		e.MTraceID = append(e.MTraceID, traceID)
		e.MLabels = append(e.MLabels, encoded)
		e.Size += 24 + len(traceID) + len(encoded)
	}

	return nil
}

// initMetrics points sink at the metric entry point over the request's
// metric caches.
func (p *parserDoer) initMetrics(sink iMetricSink) error {
	node := metriccache.FromContext(p.ctx.ctx)
	if node == nil {
		return fmt.Errorf("metric caches are not set")
	}
	p.metrics = newMetricBatch(node, p.res)
	sink.SetOnMetricSamples(p.onMetricSamples)
	sink.SetOnMetricMetadata(p.onMetricMetadata)
	return nil
}

func (p *parserDoer) doParseMetrics() {
	parser := p.MetricsParser
	if err := p.initMetrics(parser); err != nil {
		p.fail(err)
		return
	}

	go func() {
		defer p.tamePanic()
		err := parser.Decode()
		if err != nil {
			p.res <- &model.ParserResponse{Error: err}
			close(p.res)
			return
		}
		p.metrics.flush()
		close(p.res)
	}()
}

func withMetricsParser(fn func(ctx *ParserCtx) iMetricsParser) buildOption {
	return func(builder *parserBuilder) *parserBuilder {
		builder.MetricsParser = fn
		return builder
	}
}

// MetricSeries is one label set's samples, ready for the metric entry point.
type MetricSeries struct {
	Labels       [][]string
	TimestampsMs []int64
	Values       []float64
}

type metricSeriesDec struct {
	series []MetricSeries
	metricSink
}

func (d *metricSeriesDec) Decode() error {
	for _, s := range d.series {
		if err := d.onMetricSamples(sanitizeLabels(s.Labels), s.TimestampsMs, s.Values, nil); err != nil {
			return err
		}
	}
	return nil
}

// MetricSeriesParser feeds series straight into the metric entry point.
func MetricSeriesParser(series []MetricSeries) ParsingFunction {
	return Build(withMetricsParser(func(*ParserCtx) iMetricsParser {
		return &metricSeriesDec{series: series}
	}))
}
