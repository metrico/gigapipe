package unmarshal

import (
	"time"

	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/utils/proto/prompb"
	"google.golang.org/protobuf/proto"
)

type promMetricsProtoDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler
}

func (l *promMetricsProtoDec) Decode() error {
	points := 0
	sanitizeLabelsNs := int64(0)
	labelsLen := 0
	_labelsLen := 0
	const flushLimit = 1000
	req := l.ctx.bodyObject.(*prompb.WriteRequest)
	oLblsBuf := make([][]string, 16)
	for _, ts := range req.GetTimeseries() {
		oLblsBuf = oLblsBuf[:0]
		for _, lbl := range ts.GetLabels() {
			oLblsBuf = append(oLblsBuf, []string{lbl.GetName(), lbl.GetValue()})
			labelsLen += len(lbl.GetName()) + len(lbl.GetValue())
			_labelsLen += len(lbl.GetName()) + len(lbl.GetValue())
		}
		startSanitizeLabels := time.Now().UnixNano()
		oLblsBuf = sanitizeLabels(oLblsBuf)
		sanitizeLabelsNs += time.Now().UnixNano() - startSanitizeLabels

		tsns := make([]int64, 0, len(ts.GetSamples()))
		value := make([]float64, 0, len(ts.GetSamples()))
		msg := make([]string, 0, len(ts.GetSamples()))

		for _, spl := range ts.GetSamples() {
			tsns = append(tsns, spl.Timestamp*1e6)
			value = append(value, spl.Value)
			msg = append(msg, "")
			points++
			if points >= flushLimit {
				err := l.onEntries(oLblsBuf, tsns, msg, value,
					fastFillArray[uint8](len(ts.GetSamples()), model.SAMPLE_TYPE_METRIC))
				if err != nil {
					return err
				}
				// Reset the count and buffers after flushing
				points = 0
				tsns = tsns[:0]
				value = value[:0]
				msg = msg[:0]
			}
		}

		// Flush remaining samples if sample count is less than maxSamples
		if len(tsns) > 0 {
			err := l.onEntries(oLblsBuf, tsns, msg, value,
				fastFillArray[uint8](len(tsns), model.SAMPLE_TYPE_METRIC))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *promMetricsProtoDec) SetOnEntries(h onEntriesHandler) {
	l.onEntries = h
}

var UnmarshallMetricsWriteProtoV2 = Build(
	withBufferedBody,
	withParsedBody(func() proto.Message { return &prompb.WriteRequest{} }),
	withLogsParser(func(ctx *ParserCtx) iLogsParser { return &promMetricsProtoDec{ctx: ctx} }))
