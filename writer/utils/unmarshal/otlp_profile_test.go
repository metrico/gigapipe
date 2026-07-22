package unmarshal

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-faster/city"
	"github.com/metrico/qryn/v4/writer/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
)

func TestOTLPProfilesDecEmitsProfile(t *testing.T) {
	// Build a request with one symbolized profile.
	profs := pprofile.NewProfiles()
	dict := profs.Dictionary()
	dict.StringTable().Append("", "cpu", "nanoseconds", "main")
	f0 := dict.FunctionTable().AppendEmpty()
	f0.SetNameStrindex(3)
	l0 := dict.LocationTable().AppendEmpty()
	l0.Lines().AppendEmpty().SetFunctionIndex(0)
	stk := dict.StackTable().AppendEmpty()
	stk.LocationIndices().Append(0)
	rp := profs.ResourceProfiles().AppendEmpty()
	rp.Resource().Attributes().PutStr("service.name", "svc")
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SetTime(pcommon.Timestamp(1_700_000_000_000_000_000))
	p.SetDurationNano(1_000_000_000)
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	p.PeriodType().SetTypeStrindex(1)
	p.PeriodType().SetUnitStrindex(2)
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.Values().Append(42)

	body, err := pprofileotlp.NewExportRequestFromProfiles(profs).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}

	dec := &otlpProfilesDec{ctx: &ParserCtx{bodyReader: bytes.NewReader(body), ctx: context.Background()}}
	var gotType, gotSvc, gotPayloadType string
	var gotTs, gotDur uint64
	var gotAgg []model.ValuesAgg
	var gotPayloadLen int
	dec.SetOnProfile(func(timestampNs uint64, Type, serviceName string,
		stu []model.StrStr, pt, pu string, tags []model.StrStr, durationNs uint64,
		payloadType string, payload []byte, agg []model.ValuesAgg,
		tree []model.TreeRootStructure, fns []model.Function) error {
		gotType, gotSvc, gotPayloadType = Type, serviceName, payloadType
		gotTs, gotDur = timestampNs, durationNs
		gotAgg = agg
		gotPayloadLen = len(payload)
		return nil
	})

	if err := dec.Decode(); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotType != "cpu" || gotSvc != "svc" || gotPayloadType != otlpProfilePayloadType {
		t.Fatalf("meta: type=%q svc=%q ptype=%q", gotType, gotSvc, gotPayloadType)
	}
	if gotTs != 1_700_000_000_000_000_000 || gotDur != 1_000_000_000 {
		t.Fatalf("ts/dur: %d/%d", gotTs, gotDur)
	}
	if len(gotAgg) != 1 || gotAgg[0].ValueInt64 != 42 {
		t.Fatalf("agg: %+v", gotAgg)
	}
	if gotPayloadLen == 0 {
		t.Fatalf("empty payload")
	}
}

func TestOTLPProfilesMultiProfilePerRowFlush(t *testing.T) {
	// Two profiles that differ: service "a" value 10, service "b" value 99.
	profs := pprofile.NewProfiles()
	dict := profs.Dictionary()
	dict.StringTable().Append("", "cpu", "nanoseconds", "main")
	f0 := dict.FunctionTable().AppendEmpty()
	f0.SetNameStrindex(3)
	l0 := dict.LocationTable().AppendEmpty()
	l0.Lines().AppendEmpty().SetFunctionIndex(0)
	stk := dict.StackTable().AppendEmpty()
	stk.LocationIndices().Append(0)

	addProfile := func(svc string, val int64) {
		rp := profs.ResourceProfiles().AppendEmpty()
		rp.Resource().Attributes().PutStr("service.name", svc)
		sp := rp.ScopeProfiles().AppendEmpty()
		p := sp.Profiles().AppendEmpty()
		p.SetTime(pcommon.Timestamp(1_700_000_000_000_000_000))
		p.SetDurationNano(1_000_000_000)
		p.SampleType().SetTypeStrindex(1)
		p.SampleType().SetUnitStrindex(2)
		p.PeriodType().SetTypeStrindex(1)
		p.PeriodType().SetUnitStrindex(2)
		s := p.Samples().AppendEmpty()
		s.SetStackIndex(0)
		s.Values().Append(val)
	}
	addProfile("a", 10)
	addProfile("b", 99)

	body, err := pprofileotlp.NewExportRequestFromProfiles(profs).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}

	// Drive the REAL parserDoer profile-parse path (onProfile + doParseProfile),
	// mirroring how the controller consumes the res channel.
	doer := &parserDoer{
		ProfileParser: &otlpProfilesDec{ctx: &ParserCtx{
			bodyReader: bytes.NewReader(body),
			ctx:        context.Background(),
		}},
		ctx: &ParserCtx{ctx: context.Background()},
	}
	res := doer.Do()

	var profileResponses []*model.ProfileData
	for response := range res {
		if response.Error != nil {
			t.Fatalf("parser error: %v", response.Error)
		}
		if response.ProfileRequest == nil {
			continue
		}
		pd, ok := response.ProfileRequest.(*model.ProfileData)
		if !ok {
			t.Fatalf("ProfileRequest is %T, want *model.ProfileData", response.ProfileRequest)
		}
		profileResponses = append(profileResponses, pd)
	}

	if len(profileResponses) != 2 {
		t.Fatalf("want exactly 2 profile responses, got %d", len(profileResponses))
	}

	svcToAgg := map[string]int64{}
	for i, pd := range profileResponses {
		if len(pd.TimestampNs) != 1 {
			t.Fatalf("response %d: want single-row TimestampNs (len 1), got %d", i, len(pd.TimestampNs))
		}
		if len(pd.ValuesAgg) == 0 {
			t.Fatalf("response %d: empty ValuesAgg", i)
		}
		if len(pd.ServiceName) != 1 {
			t.Fatalf("response %d: want single ServiceName, got %v", i, pd.ServiceName)
		}
		svcToAgg[pd.ServiceName[0]] = pd.ValuesAgg[0].ValueInt64
	}

	// Arrays are per-profile, not collapsed to the last profile.
	if svcToAgg["a"] != 10 {
		t.Fatalf("service a ValuesAgg = %d, want 10 (arrays collapsed?)", svcToAgg["a"])
	}
	if svcToAgg["b"] != 99 {
		t.Fatalf("service b ValuesAgg = %d, want 99 (arrays collapsed?)", svcToAgg["b"])
	}
}

func TestPprofileDepDecodes(t *testing.T) {
	// Build a request with one resource/scope/profile, marshal, unmarshal.
	src := pprofile.NewProfiles()
	rp := src.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	sp.Profiles().AppendEmpty()

	reqOut := pprofileotlp.NewExportRequestFromProfiles(src)
	b, err := reqOut.MarshalProto()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	reqIn := pprofileotlp.NewExportRequest()
	if err := reqIn.UnmarshalProto(b); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := reqIn.Profiles().ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().Len()
	if got != 1 {
		t.Fatalf("want 1 profile, got %d", got)
	}
}

func TestExtractOTLPMeta(t *testing.T) {
	profs := pprofile.NewProfiles()
	dict := profs.Dictionary()
	st := dict.StringTable()
	st.Append("", "cpu", "nanoseconds", "service.name", "checkout", "region", "eu")
	// indices: 0="",1="cpu",2="nanoseconds",3="service.name",4="checkout",5="region",6="eu"

	rp := profs.ResourceProfiles().AppendEmpty()
	rp.Resource().Attributes().PutStr("service.name", "checkout")
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SetTime(pcommon.Timestamp(1_700_000_000_000_000_000))
	p.SetDurationNano(5_000_000_000)
	p.SampleType().SetTypeStrindex(1)  // "cpu"
	p.SampleType().SetUnitStrindex(2)  // "nanoseconds"
	p.PeriodType().SetTypeStrindex(1)  // "cpu"
	p.PeriodType().SetUnitStrindex(2)  // "nanoseconds"

	m := extractOTLPMeta(rp.Resource(), sp.Scope(), p, dict)

	if m.ServiceName != "checkout" {
		t.Fatalf("service: %q", m.ServiceName)
	}
	if m.Type != "cpu" {
		t.Fatalf("type: %q", m.Type)
	}
	if len(m.SampleTypesUnits) != 1 || m.SampleTypesUnits[0].Str1 != "cpu" || m.SampleTypesUnits[0].Str2 != "nanoseconds" {
		t.Fatalf("sampleTypesUnits: %+v", m.SampleTypesUnits)
	}
	if m.PeriodType != "cpu" || m.PeriodUnit != "nanoseconds" {
		t.Fatalf("period: %q/%q", m.PeriodType, m.PeriodUnit)
	}
	if m.TimestampNs != 1_700_000_000_000_000_000 || m.DurationNs != 5_000_000_000 {
		t.Fatalf("ts/dur: %d/%d", m.TimestampNs, m.DurationNs)
	}
}

func TestExtractOTLPMetaDefaultsService(t *testing.T) {
	profs := pprofile.NewProfiles()
	dict := profs.Dictionary()
	dict.StringTable().Append("", "samples", "count")
	rp := profs.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)

	m := extractOTLPMeta(rp.Resource(), sp.Scope(), p, dict)
	if m.ServiceName != "unknown_service" {
		t.Fatalf("want unknown_service, got %q", m.ServiceName)
	}
}

func TestBuildOTLPTreeSymbolized(t *testing.T) {
	profs := pprofile.NewProfiles()
	dict := profs.Dictionary()
	st := dict.StringTable()
	st.Append("", "cpu", "nanoseconds", "main", "work") // 3=main, 4=work

	// function table: fn0->main(name idx3), fn1->work(name idx4)
	f0 := dict.FunctionTable().AppendEmpty()
	f0.SetNameStrindex(3)
	f1 := dict.FunctionTable().AppendEmpty()
	f1.SetNameStrindex(4)

	// location table: loc0->main, loc1->work
	l0 := dict.LocationTable().AppendEmpty()
	l0.Lines().AppendEmpty().SetFunctionIndex(0)
	l1 := dict.LocationTable().AppendEmpty()
	l1.Lines().AppendEmpty().SetFunctionIndex(1)

	// stack: leaf-first [work(loc1), main(loc0)]
	stk := dict.StackTable().AppendEmpty()
	stk.LocationIndices().Append(1, 0)

	rp := profs.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.Values().Append(10, 5) // summed = 15

	functions, tree, valuesAgg := buildOTLPTree(p, dict)

	// functions contains main and work
	names := map[string]bool{}
	for _, f := range functions {
		names[f.ValueStr] = true
	}
	if !names["main"] || !names["work"] {
		t.Fatalf("functions missing: %+v", functions)
	}

	// valuesAgg: one entry, sum 15, count 1
	if len(valuesAgg) != 1 || valuesAgg[0].ValueInt64 != 15 || valuesAgg[0].ValueInt32 != 1 {
		t.Fatalf("valuesAgg: %+v", valuesAgg)
	}

	// two tree nodes (main root, work leaf). root total=15 self=0; leaf total=15 self=15.
	var rootSelf, leafSelf, rootTotal, leafTotal int64 = -1, -1, -1, -1
	mainId := city.CH64([]byte("main"))
	workId := city.CH64([]byte("work"))
	for _, n := range tree {
		if len(n.ValueArrTuple) != 1 {
			t.Fatalf("node value len: %+v", n)
		}
		switch n.Field2 { // funcId
		case mainId:
			rootSelf, rootTotal = n.ValueArrTuple[0].FirstValueInt64, n.ValueArrTuple[0].SecondValueInt64
		case workId:
			leafSelf, leafTotal = n.ValueArrTuple[0].FirstValueInt64, n.ValueArrTuple[0].SecondValueInt64
		}
	}
	if rootTotal != 15 || rootSelf != 0 {
		t.Fatalf("root total/self = %d/%d, want 15/0", rootTotal, rootSelf)
	}
	if leafTotal != 15 || leafSelf != 15 {
		t.Fatalf("leaf total/self = %d/%d, want 15/15", leafTotal, leafSelf)
	}
}

func TestSliceOTLPProfileRoundTrips(t *testing.T) {
	profs := pprofile.NewProfiles()
	dict := profs.Dictionary()
	dict.StringTable().Append("", "cpu", "nanoseconds", "main")
	f0 := dict.FunctionTable().AppendEmpty()
	f0.SetNameStrindex(3)
	l0 := dict.LocationTable().AppendEmpty()
	l0.Lines().AppendEmpty().SetFunctionIndex(0)
	stk := dict.StackTable().AppendEmpty()
	stk.LocationIndices().Append(0)

	rp := profs.ResourceProfiles().AppendEmpty()
	rp.Resource().Attributes().PutStr("service.name", "svc")
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.Values().Append(3)

	b, err := sliceOTLPProfile(p, dict)
	if err != nil {
		t.Fatalf("slice: %v", err)
	}

	req := pprofileotlp.NewExportRequest()
	if err := req.UnmarshalProto(b); err != nil {
		t.Fatalf("unmarshal slice: %v", err)
	}
	rps := req.Profiles().ResourceProfiles()
	if rps.Len() != 1 {
		t.Fatalf("resource profiles: %d", rps.Len())
	}
	gotP := rps.At(0).ScopeProfiles().At(0).Profiles().At(0)
	if gotP.Samples().Len() != 1 || gotP.Samples().At(0).Values().At(0) != 3 {
		t.Fatalf("sample not preserved")
	}
	// dictionary carried across so frame name resolves
	name := strAt(req.Profiles().Dictionary().StringTable(),
		req.Profiles().Dictionary().FunctionTable().At(0).NameStrindex())
	if name != "main" {
		t.Fatalf("dict not preserved, name=%q", name)
	}
}

func TestBuildOTLPTreeUnsymbolizedFallback(t *testing.T) {
	profs := pprofile.NewProfiles()
	dict := profs.Dictionary()
	dict.StringTable().Append("", "cpu", "nanoseconds")

	// location with no lines, address only, no mapping build id
	l0 := dict.LocationTable().AppendEmpty()
	l0.SetAddress(0x1234)
	stk := dict.StackTable().AppendEmpty()
	stk.LocationIndices().Append(0)

	rp := profs.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.Values().Append(7)

	functions, _, _ := buildOTLPTree(p, dict)
	found := false
	for _, f := range functions {
		if f.ValueStr == "+0x1234" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected +0x1234 fallback, got %+v", functions)
	}
}
