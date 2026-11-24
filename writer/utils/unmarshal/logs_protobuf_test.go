package unmarshal

import (
	"testing"

	"github.com/metrico/qryn/v5/writer/utils/proto/logproto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// lokiPushBytes builds a push request with one stream: labels and one entry per
// line, each entry also carrying field 3 (structured metadata of newer clients).
func lokiPushBytes(labels string, lines ...string) []byte {
	stream := protowire.AppendTag(nil, 1, protowire.BytesType)
	stream = protowire.AppendBytes(stream, []byte(labels))
	for i, line := range lines {
		ts := protowire.AppendTag(nil, 1, protowire.VarintType)
		ts = protowire.AppendVarint(ts, uint64(1700000000+i))
		entry := protowire.AppendTag(nil, 1, protowire.BytesType)
		entry = protowire.AppendBytes(entry, ts)
		entry = protowire.AppendTag(entry, 2, protowire.BytesType)
		entry = protowire.AppendBytes(entry, []byte(line))
		entry = protowire.AppendTag(entry, 3, protowire.BytesType)
		entry = protowire.AppendBytes(entry, []byte("\x0a\x01k\x12\x01v"))
		stream = protowire.AppendTag(stream, 2, protowire.BytesType)
		stream = protowire.AppendBytes(stream, entry)
	}
	req := protowire.AppendTag(nil, 1, protowire.BytesType)
	return protowire.AppendBytes(req, stream)
}

func decodeLokiPush(t *testing.T, b []byte) (labels [][]string, msgs []string) {
	t.Helper()
	req := &logproto.PushRequest{}
	if err := proto.Unmarshal(b, req); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	dec := &logsProtoDec{ctx: &ParserCtx{bodyObject: req}}
	dec.SetOnEntries(func(l [][]string, _ []int64, m []string, _ []float64, _ []uint8) error {
		labels = append(labels, l...)
		msgs = append(msgs, m...)
		return nil
	})
	if err := dec.Decode(); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return labels, msgs
}

// TestLokiPushInvalidUTF8Line: a line cut in the middle of a two-byte character
// must not fail the request; the stray byte becomes U+FFFD and valid lines are
// kept as they are.
func TestLokiPushInvalidUTF8Line(t *testing.T) {
	labels, msgs := decodeLokiPush(t, lokiPushBytes(`{job="a"}`, "cut \xd0", "вторая строка"))
	if len(msgs) != 2 || msgs[0] != "cut �" || msgs[1] != "вторая строка" {
		t.Errorf("lines %q", msgs)
	}
	if len(labels) != 1 || labels[0][0] != "job" || labels[0][1] != "a" {
		t.Errorf("labels %q", labels)
	}
}

// TestLokiPushInvalidUTF8LabelRejected: labels identify the stream, so invalid
// UTF-8 there still fails the request instead of being rewritten.
func TestLokiPushInvalidUTF8LabelRejected(t *testing.T) {
	if err := proto.Unmarshal(lokiPushBytes("{job=\"a\xff\"}", "line"), &logproto.PushRequest{}); err == nil {
		t.Error("a label with invalid UTF-8 was accepted")
	}
}

// TestLokiPushTruncated: a cut message is an error, not a partial request.
func TestLokiPushTruncated(t *testing.T) {
	b := lokiPushBytes(`{job="a"}`, "line")
	if err := proto.Unmarshal(b[:len(b)-2], &logproto.PushRequest{}); err == nil {
		t.Error("no error on a truncated request")
	}
}
