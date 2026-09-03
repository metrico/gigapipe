package unmarshal

import (
	"slices"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/proto/logproto"
	"google.golang.org/protobuf/proto"
)

type logsProtoDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler
}

func (l *logsProtoDec) Decode() error {
	obj := l.ctx.bodyObject.(*logproto.PushRequest)
	var err error
	labels := make([][]string, 0, 10)
	for _, stream := range obj.GetStreams() {
		labels = labels[:0]
		labels, err = parseLabelsLokiFormat([]byte(stream.GetLabels()), labels)
		if err != nil {
			return err
		}
		labels = sanitizeLabels(labels)
		tsns := make([]int64, len(stream.GetEntries()))
		msgs := make([]string, len(stream.GetEntries()))

		for i, e := range stream.GetEntries() {
			tsns[i] = e.Timestamp.GetSeconds()*1000000000 + int64(e.Timestamp.GetNanos())
			msgs[i] = e.GetLine()
		}
		err = l.onEntries(labels, tsns, msgs, make([]float64, len(stream.GetEntries())),
			slices.Repeat([]uint8{model.SAMPLE_TYPE_LOG}, len(stream.GetEntries())))
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *logsProtoDec) SetOnEntries(h onEntriesHandler) {
	l.onEntries = h
}

var UnmarshalProtoV2 = Build(
	withBufferedBody,
	withParsedBody(func() proto.Message { return &logproto.PushRequest{} }),
	withLogsParser(func(ctx *ParserCtx) iLogsParser {
		return &logsProtoDec{ctx: ctx}
	}))
