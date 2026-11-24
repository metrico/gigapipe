package unmarshal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"mime/multipart"
	"os"
	"strings"
	"testing"

	pprof_proto "github.com/google/pprof/profile"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils"
)

// Characterization tests for the two legacy /ingest paths: the multipart upload
// in go_pprof.go and the binary/octet-stream variant in binary_pprof.go.
//
// Neither had any test. They were written to pin the behaviour of those paths
// across a refactor that moved their shared conversion block into emitProfile,
// and they are deliberately self-contained -- no helper from another _test.go
// file -- so that the same file can be dropped onto an older checkout and run
// there for comparison. Set GIGAPIPE_DUMP_EMISSIONS=1 to print every field of
// every emission, which is what makes two checkouts diffable.

const (
	legacyFrom  = "1700000000"
	legacyUntil = "1700000010"
	// ns() scales both to nanoseconds, so the window is 10s wide.
	legacyWantTimestampNs = uint64(1_700_000_000_000_000_000)
	legacyWantDurationNs  = uint64(10_000_000_000)
	legacyName            = "voice-agent{region=eu-west,env=prod}"
)

// legacyTestProfile builds a two-frame CPU pprof. Two frames rather than one so
// the emitted trie has a parent and a child, which is where an ordering or
// aggregation change would show up.
func legacyTestProfile(t *testing.T, gzipped bool) []byte {
	t.Helper()
	outer := &pprof_proto.Function{
		ID: 1, Name: "main.serve", SystemName: "main.serve", Filename: "main.go",
	}
	inner := &pprof_proto.Function{
		ID: 2, Name: "main.resample", SystemName: "main.resample", Filename: "audio.go",
	}
	outerLoc := &pprof_proto.Location{
		ID: 1, Line: []pprof_proto.Line{{Function: outer, Line: 10}},
	}
	innerLoc := &pprof_proto.Location{
		ID: 2, Line: []pprof_proto.Line{{Function: inner, Line: 42}},
	}
	p := &pprof_proto.Profile{
		SampleType: []*pprof_proto.ValueType{
			{Type: "samples", Unit: "count"},
			{Type: "cpu", Unit: "nanoseconds"},
		},
		PeriodType:    &pprof_proto.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Period:        10_000_000,
		TimeNanos:     int64(legacyWantTimestampNs),
		DurationNanos: int64(legacyWantDurationNs),
		Function:      []*pprof_proto.Function{outer, inner},
		Location:      []*pprof_proto.Location{outerLoc, innerLoc},
		Sample: []*pprof_proto.Sample{
			{
				Location: []*pprof_proto.Location{innerLoc, outerLoc},
				Value:    []int64{3, 30_000_000},
			},
			{
				Location: []*pprof_proto.Location{outerLoc},
				Value:    []int64{1, 10_000_000},
			},
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

// legacyMultipartBody wraps a profile the way the Pyroscope uploader does.
func legacyMultipartBody(t *testing.T, profile []byte) []byte {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("profile", "profile.pprof")
	if err != nil {
		t.Fatalf("building multipart body: %v", err)
	}
	if _, err := part.Write(profile); err != nil {
		t.Fatalf("writing multipart part: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing multipart body: %v", err)
	}
	return body.Bytes()
}

// canonicalEmission renders every argument the profile handler received. The
// payload is hashed rather than printed: it is the stored artefact, and the
// hash is what proves it byte-identical without 50KB of noise.
func canonicalEmission(timestampNs uint64, profileType, name string,
	sampleTypesUnits []model.StrStr, periodType, periodUnit string,
	tags []model.StrStr, durationNs uint64, payloadType string, payload []byte,
	agg []model.ValuesAgg, tree []model.TreeRootStructure,
	functions []model.Function) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ts=%d dur=%d type=%q name=%q periodType=%q periodUnit=%q payloadType=%q payloadLen=%d payloadSHA256=%x\n",
		timestampNs, durationNs, profileType, name, periodType, periodUnit,
		payloadType, len(payload), sha256.Sum256(payload))
	for _, s := range sampleTypesUnits {
		fmt.Fprintf(&b, "  sampleTypeUnit %q=%q\n", s.Str1, s.Str2)
	}
	for _, t := range tags {
		fmt.Fprintf(&b, "  tag %q=%q\n", t.Str1, t.Str2)
	}
	for _, a := range agg {
		fmt.Fprintf(&b, "  agg %q sum=%d count=%d\n", a.ValueStr, a.ValueInt64, a.ValueInt32)
	}
	for _, f := range functions {
		fmt.Fprintf(&b, "  func id=%d name=%q\n", f.ValueInt64, f.ValueStr)
	}
	for _, n := range tree {
		fmt.Fprintf(&b, "  node parent=%d func=%d id=%d\n", n.Field1, n.Field2, n.Field3)
		for _, v := range n.ValueArrTuple {
			fmt.Fprintf(&b, "    value %q self=%d total=%d\n",
				v.ValueStr, v.FirstValueInt64, v.SecondValueInt64)
		}
	}
	return b.String()
}

// runLegacyDecoder drives one legacy decoder and returns the canonical dump of
// everything it emitted.
func runLegacyDecoder(t *testing.T, label string, body []byte, binary bool) string {
	t.Helper()
	ctx := &ParserCtx{
		bodyReader: bytes.NewReader(body),
		ctx:        context.Background(),
		ctxMap: map[utils.ContextKey]string{
			utils.ContextKeyFrom:  legacyFrom,
			utils.ContextKeyUntil: legacyUntil,
			utils.ContextKeyName:  legacyName,
		},
	}

	var dumps []string
	handler := func(timestampNs uint64, profileType, name string,
		stu []model.StrStr, periodType, periodUnit string, tags []model.StrStr,
		durationNs uint64, payloadType string, payload []byte,
		agg []model.ValuesAgg, tree []model.TreeRootStructure,
		functions []model.Function) error {
		dumps = append(dumps, canonicalEmission(timestampNs, profileType, name,
			stu, periodType, periodUnit, tags, durationNs, payloadType, payload,
			agg, tree, functions))
		return nil
	}

	var dec iProfilesParser
	if binary {
		dec = &binaryStreamPProfProtoDec{pProfProtoDec{ctx: ctx}}
	} else {
		dec = &pProfProtoDec{ctx: ctx}
	}
	dec.SetOnProfile(handler)
	if err := dec.Decode(); err != nil {
		t.Fatalf("%s: decode: %v", label, err)
	}
	if len(dumps) != 1 {
		t.Fatalf("%s: emitted %d profiles, want 1", label, len(dumps))
	}

	dump := dumps[0]
	if os.Getenv("GIGAPIPE_DUMP_EMISSIONS") != "" {
		fmt.Printf("=== EMISSION %s BEGIN ===\n%s=== EMISSION %s END ===\n",
			label, dump, label)
	}
	return dump
}

// assertLegacyEmission pins the fields that the /ingest contract fixes: the
// window comes from the from/until parameters, the name and tags are split out
// of the name parameter, and the profile type is derived from the pprof.
func assertLegacyEmission(t *testing.T, label, dump string) {
	t.Helper()
	for _, want := range []string{
		fmt.Sprintf("ts=%d", legacyWantTimestampNs),
		fmt.Sprintf("dur=%d", legacyWantDurationNs),
		`type="process_cpu"`,
		`name="voice-agent"`,
		`periodType="cpu"`,
		`periodUnit="nanoseconds"`,
		`sampleTypeUnit "samples"="count"`,
		`sampleTypeUnit "cpu"="nanoseconds"`,
		`tag "region"="eu-west"`,
		`tag "env"="prod"`,
		`agg "samples:count" sum=4 count=2`,
		`agg "cpu:nanoseconds" sum=40000000 count=2`,
		// Function ids are content hashes, not the pprof's own ids.
		`name="main.serve"`,
		`name="main.resample"`,
		// The trie: the leaf holds its own samples, the root totals both.
		`value "samples:count" self=3 total=3`,
		`value "samples:count" self=1 total=4`,
		`value "cpu:nanoseconds" self=10000000 total=40000000`,
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("%s: emission is missing %s\n--- full emission ---\n%s", label, want, dump)
		}
	}
}

func TestLegacyMultipartIngestEmission(t *testing.T) {
	dump := runLegacyDecoder(t, "multipart",
		legacyMultipartBody(t, legacyTestProfile(t, true)), false)
	assertLegacyEmission(t, "multipart", dump)
}

func TestLegacyBinaryStreamIngestEmission(t *testing.T) {
	dump := runLegacyDecoder(t, "binary", legacyTestProfile(t, true), true)
	assertLegacyEmission(t, "binary", dump)
}

// TestLegacyPathsAgreeWithEachOther is the cheap half of the regression proof:
// the two paths differ only in how they unwrap the body, so the rows they
// derive from the same profile must be identical. A change that altered the
// shared conversion for one of them would break this even if the per-path
// assertions above were both updated to match.
func TestLegacyPathsAgreeWithEachOther(t *testing.T) {
	profile := legacyTestProfile(t, true)
	multipart := runLegacyDecoder(t, "multipart", legacyMultipartBody(t, profile), false)
	binary := runLegacyDecoder(t, "binary", profile, true)
	if multipart != binary {
		t.Errorf("the two legacy paths disagree\n--- multipart ---\n%s\n--- binary ---\n%s",
			multipart, binary)
	}
}

func TestLegacyBinaryStreamAcceptsAnUncompressedProfile(t *testing.T) {
	dump := runLegacyDecoder(t, "binary-uncompressed", legacyTestProfile(t, false), true)
	assertLegacyEmission(t, "binary-uncompressed", dump)
}
