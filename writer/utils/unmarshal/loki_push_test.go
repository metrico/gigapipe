package unmarshal

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/writer/metric"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/errors"
	"github.com/metrico/qryn/v5/writer/utils/proto/logproto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"
)

type lokiPushResult struct {
	err     error
	rows    int
	types   []uint8
	message []string
}

func pushLoki(t *testing.T, parse ParsingFunction, body []byte) lokiPushResult {
	t.Helper()
	withCityHashFingerprints(t)
	var res lokiPushResult
	for resp := range parse(context.Background(), bytes.NewReader(body), newTestFPCache(t)) {
		if resp.Error != nil {
			res.err = resp.Error
			continue
		}
		if d, ok := resp.SamplesRequest.(*model.TimeSamplesData); ok {
			res.rows += len(d.MType)
			res.types = append(res.types, d.MType...)
			res.message = append(res.message, d.MMessage...)
		}
	}
	return res
}

func lokiPushValueRejections() float64 {
	return testutil.ToFloat64(metric.IngestRejected.WithLabelValues(RejectLokiPushValue))
}

func TestLokiJSONPushLineIsTypedLog(t *testing.T) {
	res := pushLoki(t, DecodePushRequestStringV2, []byte(`{"streams":[
		{"stream":{"job":"a"},"values":[["1700000000000000000","one"],["1700000000000000001","two",{"trace_id":"x"}]]},
		{"labels":"{job=\"b\"}","entries":[{"ts":"1700000000000000000","line":"three"}]}]}`))
	if res.err != nil {
		t.Fatal(res.err)
	}
	if strings.Join(res.message, ",") != "one,two,three" {
		t.Fatalf("lines: got %q", res.message)
	}
	for _, tp := range res.types {
		if tp != model.SAMPLE_TYPE_LOG {
			t.Fatalf("types: got %v, want every row typed %d", res.types, model.SAMPLE_TYPE_LOG)
		}
	}
}

func TestLokiJSONPushRejectsValues(t *testing.T) {
	cases := []struct {
		name, body, wantMsg, wantEntry string
	}{
		{"entry with value",
			`{"streams":[{"labels":"{job=\"a\"}","entries":[{"ts":"1","line":"ok"},{"ts":"2","value":1.5}]}]}`,
			`{job="a"}`, "entry 1"},
		{"entry with line and value",
			`{"streams":[{"entries":[{"ts":"1","line":"x","value":2}],"stream":{"job":"b"}}]}`,
			`{job="b"}`, "entry 0"},
		{"values row with a number",
			`{"streams":[{"stream":{"job":"c"},"values":[["1","ok"],["2","x",3]]}]}`,
			`{job="c"}`, "entry 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// The first stream is valid: a rejected request writes nothing.
			body := strings.Replace(c.body, `{"streams":[`,
				`{"streams":[{"stream":{"job":"first"},"values":[["1","kept?"]]},`, 1)
			before := lokiPushValueRejections()
			res := pushLoki(t, DecodePushRequestStringV2, []byte(body))
			if res.err == nil {
				t.Fatal("want an error")
			}
			e, ok := res.err.(errors.IQrynError)
			if !ok || e.GetCode() != 400 {
				t.Fatalf("error: got %#v, want a 400", res.err)
			}
			if !strings.Contains(e.Error(), c.wantMsg) || !strings.Contains(e.Error(), c.wantEntry) {
				t.Fatalf("message %q must name the stream %s and %s", e.Error(), c.wantMsg, c.wantEntry)
			}
			if res.rows != 0 {
				t.Fatalf("rows written: got %d, want 0", res.rows)
			}
			if got := lokiPushValueRejections() - before; got != 1 {
				t.Fatalf("loki_push_value count: got %v, want 1", got)
			}
		})
	}
}

func TestLokiProtobufPushIsTypedLog(t *testing.T) {
	body, err := proto.Marshal(&logproto.PushRequest{Streams: []*logproto.StreamAdapter{{
		Labels: `{job="a"}`,
		Entries: []*logproto.EntryAdapter{
			{Timestamp: &logproto.Timestamp{Seconds: 1700000000}, Line: "one"},
			{Timestamp: &logproto.Timestamp{Seconds: 1700000001}, Line: "two"},
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	res := pushLoki(t, UnmarshalProtoV2, body)
	if res.err != nil {
		t.Fatal(res.err)
	}
	if strings.Join(res.message, ",") != "one,two" || res.types[0] != model.SAMPLE_TYPE_LOG || res.types[1] != model.SAMPLE_TYPE_LOG {
		t.Fatalf("rows: got %q typed %v", res.message, res.types)
	}
}
