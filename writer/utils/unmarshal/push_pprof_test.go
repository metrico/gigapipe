package unmarshal

import (
	"bytes"
	"context"
	"testing"
	"time"

	pprof_proto "github.com/google/pprof/profile"
	"github.com/metrico/qryn/v5/writer/model"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	testTimeNanos     = int64(1_700_000_000_000_000_000)
	testDurationNanos = int64(10_000_000_000)
)

// testProfile builds a valid CPU pprof, the shape an SDK samples and uploads.
// gzipped mirrors what the SDKs actually put on the wire; the uncompressed
// form is accepted too, and one test covers it.
func testProfile(t *testing.T, timeNanos, durationNanos int64, gzipped bool) []byte {
	t.Helper()
	fn := &pprof_proto.Function{
		ID: 1, Name: "main.work", SystemName: "main.work", Filename: "main.go",
	}
	loc := &pprof_proto.Location{
		ID:   1,
		Line: []pprof_proto.Line{{Function: fn, Line: 10}},
	}
	p := &pprof_proto.Profile{
		SampleType: []*pprof_proto.ValueType{
			{Type: "samples", Unit: "count"},
			{Type: "cpu", Unit: "nanoseconds"},
		},
		PeriodType:    &pprof_proto.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Period:        10_000_000,
		TimeNanos:     timeNanos,
		DurationNanos: durationNanos,
		Function:      []*pprof_proto.Function{fn},
		Location:      []*pprof_proto.Location{loc},
		Sample: []*pprof_proto.Sample{
			{Location: []*pprof_proto.Location{loc}, Value: []int64{3, 30_000_000}},
		},
	}
	var buf bytes.Buffer
	var err error
	if gzipped {
		err = p.Write(&buf)
	} else {
		err = p.WriteUncompressed(&buf)
	}
	if err != nil {
		t.Fatalf("writing test pprof: %v", err)
	}
	return buf.Bytes()
}

type testSeries struct {
	labels  [][2]string
	samples [][]byte
}

// pushV1Body encodes a push.v1 PushRequest the way an SDK's generated client
// does. Encoding it here by hand rather than reusing the decoder's own structs
// is deliberate: a test that shared an encoder with the code under test would
// pass just as happily if both agreed on the wrong field numbers.
func pushV1Body(series ...testSeries) []byte {
	var body []byte
	for _, s := range series {
		var encoded []byte
		for _, label := range s.labels {
			var pair []byte
			pair = protowire.AppendTag(pair, 1, protowire.BytesType)
			pair = protowire.AppendString(pair, label[0])
			pair = protowire.AppendTag(pair, 2, protowire.BytesType)
			pair = protowire.AppendString(pair, label[1])
			encoded = protowire.AppendTag(encoded, 1, protowire.BytesType)
			encoded = protowire.AppendBytes(encoded, pair)
		}
		for _, raw := range s.samples {
			var sample []byte
			sample = protowire.AppendTag(sample, 1, protowire.BytesType)
			sample = protowire.AppendBytes(sample, raw)
			sample = protowire.AppendTag(sample, 2, protowire.BytesType)
			sample = protowire.AppendString(sample, "sample-id")
			encoded = protowire.AppendTag(encoded, 2, protowire.BytesType)
			encoded = protowire.AppendBytes(encoded, sample)
		}
		body = protowire.AppendTag(body, 1, protowire.BytesType)
		body = protowire.AppendBytes(body, encoded)
	}
	return body
}

type emitted struct {
	timestampNs uint64
	durationNs  uint64
	profileType string
	name        string
	tags        []model.StrStr
	agg         []model.ValuesAgg
	payloadLen  int
}

func decodePushV1(t *testing.T, body []byte) ([]emitted, error) {
	t.Helper()
	dec := &pushV1ProfProtoDec{pProfProtoDec{
		ctx: &ParserCtx{bodyReader: bytes.NewReader(body), ctx: context.Background()},
	}}
	var got []emitted
	dec.SetOnProfile(func(timestampNs uint64, profileType, name string,
		_ []model.StrStr, _, _ string, tags []model.StrStr, durationNs uint64,
		_ string, payload []byte, agg []model.ValuesAgg,
		_ []model.TreeRootStructure, _ []model.Function) error {
		got = append(got, emitted{
			timestampNs: timestampNs,
			durationNs:  durationNs,
			profileType: profileType,
			name:        name,
			tags:        tags,
			agg:         agg,
			payloadLen:  len(payload),
		})
		return nil
	})
	return got, dec.Decode()
}

func decodeOnePushV1(t *testing.T, body []byte) emitted {
	t.Helper()
	got, err := decodePushV1(t, body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("emitted %d profiles, want 1", len(got))
	}
	return got[0]
}

func TestPushV1IngestsAProfile(t *testing.T) {
	got := decodeOnePushV1(t, pushV1Body(testSeries{
		labels: [][2]string{
			{"service_name", "voice-agent"},
			{"region", "eu-west"},
			{"__session_id__", "9f3a"},
			{"env", "prod"},
		},
		samples: [][]byte{testProfile(t, testTimeNanos, testDurationNanos, true)},
	}))

	if got.name != "voice-agent" {
		t.Errorf("name = %q, want %q", got.name, "voice-agent")
	}
	// Derived from the pprof's PeriodType, exactly as the /ingest path does it.
	if got.profileType != "process_cpu" {
		t.Errorf("profile type = %q, want %q", got.profileType, "process_cpu")
	}
	// A push carries no from/until, so both of these can only have come out of
	// the pprof payload.
	if got.timestampNs != uint64(testTimeNanos) {
		t.Errorf("timestamp = %d, want %d", got.timestampNs, testTimeNanos)
	}
	if got.durationNs != uint64(testDurationNanos) {
		t.Errorf("duration = %d, want %d", got.durationNs, testDurationNanos)
	}
	// Sorted by name, the name labels consumed, the internal label dropped.
	want := []model.StrStr{
		{Str1: "env", Str2: "prod"},
		{Str1: "region", Str2: "eu-west"},
	}
	if len(got.tags) != len(want) {
		t.Fatalf("tags = %v, want %v", got.tags, want)
	}
	for i := range want {
		if got.tags[i] != want[i] {
			t.Errorf("tags[%d] = %v, want %v", i, got.tags[i], want[i])
		}
	}
	if got.payloadLen == 0 {
		t.Error("stored payload is empty")
	}
	// Two sample types in, two aggregates out, carrying the sampled values.
	if len(got.agg) != 2 {
		t.Fatalf("aggregates = %v, want 2", got.agg)
	}
	if got.agg[0].ValueInt64 != 3 || got.agg[1].ValueInt64 != 30_000_000 {
		t.Errorf("aggregate sums = %d/%d, want 3/30000000",
			got.agg[0].ValueInt64, got.agg[1].ValueInt64)
	}
}

func TestPushV1IngestsEverySampleOfEverySeries(t *testing.T) {
	// A v2 push is a batch. Decoding only the first series or the first sample
	// would drop profiles without failing anything else in this file.
	profile := testProfile(t, testTimeNanos, testDurationNanos, true)
	got, err := decodePushV1(t, pushV1Body(
		testSeries{
			labels:  [][2]string{{"service_name", "agent-a"}},
			samples: [][]byte{profile, profile},
		},
		testSeries{
			labels:  [][2]string{{"service_name", "agent-b"}},
			samples: [][]byte{profile, profile, profile},
		},
	))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("emitted %d profiles, want 5", len(got))
	}
	names := map[string]int{}
	for _, e := range got {
		names[e.name]++
	}
	if names["agent-a"] != 2 || names["agent-b"] != 3 {
		t.Errorf("per-series counts = %v, want agent-a:2 agent-b:3", names)
	}
}

func TestPushV1FallsBackToTheNameLabel(t *testing.T) {
	got := decodeOnePushV1(t, pushV1Body(testSeries{
		labels:  [][2]string{{"__name__", "legacy-agent"}},
		samples: [][]byte{testProfile(t, testTimeNanos, testDurationNanos, true)},
	}))
	if got.name != "legacy-agent" {
		t.Errorf("name = %q, want %q", got.name, "legacy-agent")
	}
	if len(got.tags) != 0 {
		t.Errorf("tags = %v, want none: __name__ is the name, not a tag", got.tags)
	}
}

func TestPushV1PrefersServiceNameOverTheNameLabel(t *testing.T) {
	got := decodeOnePushV1(t, pushV1Body(testSeries{
		labels: [][2]string{
			{"__name__", "older-spelling"},
			{"service_name", "voice-agent"},
		},
		samples: [][]byte{testProfile(t, testTimeNanos, testDurationNanos, true)},
	}))
	if got.name != "voice-agent" {
		t.Errorf("name = %q, want %q", got.name, "voice-agent")
	}
}

func TestPushV1RejectsASeriesWithNoName(t *testing.T) {
	_, err := decodePushV1(t, pushV1Body(testSeries{
		labels:  [][2]string{{"region", "eu-west"}},
		samples: [][]byte{testProfile(t, testTimeNanos, testDurationNanos, true)},
	}))
	if err == nil {
		t.Fatal("decoded a nameless series; want an error rather than profiles stored under an invented name")
	}
}

func TestPushV1AcceptsAnUncompressedProfile(t *testing.T) {
	// The pprof parser sniffs gzip rather than assuming it, so a client that
	// skips compression still ingests.
	got := decodeOnePushV1(t, pushV1Body(testSeries{
		labels:  [][2]string{{"service_name", "voice-agent"}},
		samples: [][]byte{testProfile(t, testTimeNanos, testDurationNanos, false)},
	}))
	if got.profileType != "process_cpu" || got.payloadLen == 0 {
		t.Errorf("type = %q, payload = %d bytes", got.profileType, got.payloadLen)
	}
}

func TestPushV1TimestampFallsBackToNowAndDurationDoesNot(t *testing.T) {
	before := uint64(time.Now().UnixNano())
	got := decodeOnePushV1(t, pushV1Body(testSeries{
		labels:  [][2]string{{"service_name", "voice-agent"}},
		samples: [][]byte{testProfile(t, 0, 0, true)},
	}))
	after := uint64(time.Now().UnixNano())

	if got.timestampNs < before || got.timestampNs > after {
		t.Errorf("timestamp = %d, want it inside [%d, %d]", got.timestampNs, before, after)
	}
	// Anything non-zero here would be an invented interval stored as if it had
	// been measured.
	if got.durationNs != 0 {
		t.Errorf("duration = %d, want 0 when the pprof omits it", got.durationNs)
	}
}

func TestPushV1SkipsFieldsItDoesNotKnow(t *testing.T) {
	// Forward compatibility: a newer SDK adding a field must not break ingest.
	body := pushV1Body(testSeries{
		labels:  [][2]string{{"service_name", "voice-agent"}},
		samples: [][]byte{testProfile(t, testTimeNanos, testDurationNanos, true)},
	})
	body = protowire.AppendTag(body, 9, protowire.VarintType)
	body = protowire.AppendVarint(body, 1234)
	body = protowire.AppendTag(body, 10, protowire.BytesType)
	body = protowire.AppendString(body, "a field from a later release")

	got := decodeOnePushV1(t, body)
	if got.name != "voice-agent" {
		t.Errorf("name = %q, want %q", got.name, "voice-agent")
	}
}

func TestPushV1RejectsMalformedBodies(t *testing.T) {
	// The handler is reachable by anyone who can reach the port, so malformed
	// input has to come back as an error and never as a panic.
	for name, body := range map[string][]byte{
		"truncated length prefix": {0x0a, 0xff},
		"length beyond the body":  {0x0a, 0x7f, 0x01},
		"random bytes":            []byte("not a protobuf at all"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodePushV1(t, body); err == nil {
				t.Error("decoded malformed input without an error")
			}
		})
	}
}

func TestPushV1RejectsAnUnparseableProfile(t *testing.T) {
	_, err := decodePushV1(t, pushV1Body(testSeries{
		labels:  [][2]string{{"service_name", "voice-agent"}},
		samples: [][]byte{[]byte("this is not a pprof")},
	}))
	if err == nil {
		t.Fatal("accepted a non-pprof payload")
	}
}
