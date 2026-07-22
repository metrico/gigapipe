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

// stackFuncNames resolves, per sample, the function names along its stack via
// stack -> locations -> first line -> function -> name, through the given dict.
func stackFuncNames(dict pprofile.ProfilesDictionary, p pprofile.Profile) [][]string {
	st := dict.StringTable()
	var res [][]string
	for si := 0; si < p.Samples().Len(); si++ {
		s := p.Samples().At(si)
		var names []string
		idx := s.StackIndex()
		if idx >= 0 && int(idx) < dict.StackTable().Len() {
			li := dict.StackTable().At(int(idx)).LocationIndices()
			for k := 0; k < li.Len(); k++ {
				loc := dict.LocationTable().At(int(li.At(k)))
				if loc.Lines().Len() > 0 {
					fi := loc.Lines().At(0).FunctionIndex()
					names = append(names, strAt(st, dict.FunctionTable().At(int(fi)).NameStrindex()))
				}
			}
		}
		res = append(res, names)
	}
	return res
}

// validatePrunedIndices asserts every table index in the decoded request is
// in-range for its (pruned) table — catches any dropped/mis-remapped reference.
func validatePrunedIndices(t *testing.T, dict pprofile.ProfilesDictionary, p pprofile.Profile) {
	t.Helper()
	nStr, nFunc, nLoc, nMap := dict.StringTable().Len(), dict.FunctionTable().Len(), dict.LocationTable().Len(), dict.MappingTable().Len()
	nStack, nAttr, nLink := dict.StackTable().Len(), dict.AttributeTable().Len(), dict.LinkTable().Len()
	inStr := func(i int32) bool { return i >= 0 && int(i) < nStr }
	for i := 0; i < dict.FunctionTable().Len(); i++ {
		f := dict.FunctionTable().At(i)
		if !inStr(f.NameStrindex()) || !inStr(f.SystemNameStrindex()) || !inStr(f.FilenameStrindex()) {
			t.Fatalf("function %d strindex out of range (nStr=%d)", i, nStr)
		}
	}
	for i := 0; i < dict.MappingTable().Len(); i++ {
		m := dict.MappingTable().At(i)
		if !inStr(m.FilenameStrindex()) {
			t.Fatalf("mapping %d filename strindex out of range", i)
		}
		for k := 0; k < m.AttributeIndices().Len(); k++ {
			if a := m.AttributeIndices().At(k); a < 0 || int(a) >= nAttr {
				t.Fatalf("mapping %d attr idx %d out of range (nAttr=%d)", i, a, nAttr)
			}
		}
	}
	for i := 0; i < dict.LocationTable().Len(); i++ {
		l := dict.LocationTable().At(i)
		if mi := l.MappingIndex(); mi >= 0 && int(mi) >= nMap {
			t.Fatalf("location %d mapping idx out of range (nMap=%d)", i, nMap)
		}
		for k := 0; k < l.Lines().Len(); k++ {
			if fi := l.Lines().At(k).FunctionIndex(); fi < 0 || int(fi) >= nFunc {
				t.Fatalf("location %d line func idx out of range (nFunc=%d)", i, nFunc)
			}
		}
		for k := 0; k < l.AttributeIndices().Len(); k++ {
			if a := l.AttributeIndices().At(k); a < 0 || int(a) >= nAttr {
				t.Fatalf("location %d attr idx out of range", i)
			}
		}
	}
	for i := 0; i < dict.StackTable().Len(); i++ {
		li := dict.StackTable().At(i).LocationIndices()
		for k := 0; k < li.Len(); k++ {
			if l := li.At(k); l < 0 || int(l) >= nLoc {
				t.Fatalf("stack %d loc idx out of range (nLoc=%d)", i, nLoc)
			}
		}
	}
	for i := 0; i < dict.AttributeTable().Len(); i++ {
		a := dict.AttributeTable().At(i)
		if !inStr(a.KeyStrindex()) || !inStr(a.UnitStrindex()) {
			t.Fatalf("attribute %d strindex out of range", i)
		}
	}
	if !inStr(p.SampleType().TypeStrindex()) || !inStr(p.SampleType().UnitStrindex()) ||
		!inStr(p.PeriodType().TypeStrindex()) || !inStr(p.PeriodType().UnitStrindex()) {
		t.Fatalf("profile sample/period strindex out of range")
	}
	for k := 0; k < p.AttributeIndices().Len(); k++ {
		if a := p.AttributeIndices().At(k); a < 0 || int(a) >= nAttr {
			t.Fatalf("profile attr idx out of range")
		}
	}
	for si := 0; si < p.Samples().Len(); si++ {
		s := p.Samples().At(si)
		if idx := s.StackIndex(); idx < 0 || int(idx) >= nStack {
			t.Fatalf("sample %d stack idx out of range (nStack=%d)", si, nStack)
		}
		if idx := s.LinkIndex(); idx >= 0 && int(idx) >= nLink {
			t.Fatalf("sample %d link idx out of range (nLink=%d)", si, nLink)
		}
		for k := 0; k < s.AttributeIndices().Len(); k++ {
			if a := s.AttributeIndices().At(k); a < 0 || int(a) >= nAttr {
				t.Fatalf("sample %d attr idx out of range", si)
			}
		}
	}
}

func TestSliceOTLPProfilePrunesAndRoundTrips(t *testing.T) {
	src := pprofile.NewProfiles()
	dict := src.Dictionary()
	// strings: 0="" 1=cpu 2=nanoseconds 3=main 4=app.go 5=worker 6=lib.so
	// 7=thread 8=count 9=DECOY_STRING 10=decoyFunc
	dict.StringTable().Append("", "cpu", "nanoseconds", "main", "app.go", "worker",
		"lib.so", "thread", "count", "DECOY_STRING", "decoyFunc")

	// functions: f0=main/app.go, f1=worker, f2=decoy (unreferenced)
	f0 := dict.FunctionTable().AppendEmpty()
	f0.SetNameStrindex(3)
	f0.SetFilenameStrindex(4)
	f1 := dict.FunctionTable().AppendEmpty()
	f1.SetNameStrindex(5)
	fDecoy := dict.FunctionTable().AppendEmpty()
	fDecoy.SetNameStrindex(10)

	// attributes: 0=loc,1=mapping,2=profile,3=sample,4=decoy
	mkAttr := func(key, unit int32, val string) {
		a := dict.AttributeTable().AppendEmpty()
		a.SetKeyStrindex(key)
		a.SetUnitStrindex(unit)
		a.Value().SetStr(val)
	}
	mkAttr(7, 8, "loc-attr")     // 0
	mkAttr(7, 8, "map-attr")     // 1
	mkAttr(7, 8, "prof-attr")    // 2
	mkAttr(7, 8, "sample-attr")  // 3
	mkAttr(9, 9, "decoy-attr")   // 4 (unreferenced)

	// mappings: m0=lib.so w/ attr1, m1=decoy
	m0 := dict.MappingTable().AppendEmpty()
	m0.SetFilenameStrindex(6)
	m0.AttributeIndices().Append(1)
	mDecoy := dict.MappingTable().AppendEmpty()
	mDecoy.SetFilenameStrindex(9)

	// locations: loc0=main(mapping0, attr0), loc1=worker, loc2=decoy
	loc0 := dict.LocationTable().AppendEmpty()
	loc0.SetMappingIndex(0)
	loc0.Lines().AppendEmpty().SetFunctionIndex(0)
	loc0.AttributeIndices().Append(0)
	loc1 := dict.LocationTable().AppendEmpty()
	loc1.Lines().AppendEmpty().SetFunctionIndex(1)
	locDecoy := dict.LocationTable().AppendEmpty()
	locDecoy.Lines().AppendEmpty().SetFunctionIndex(2)

	// links: link0 referenced, link1 decoy
	l0 := dict.LinkTable().AppendEmpty()
	l0.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	l0.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	dict.LinkTable().AppendEmpty() // decoy

	// stack: leaf worker(loc1) -> main(loc0)
	stk := dict.StackTable().AppendEmpty()
	stk.LocationIndices().Append(1, 0)
	dict.StackTable().AppendEmpty() // decoy stack

	rp := src.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	p.PeriodType().SetTypeStrindex(1)
	p.PeriodType().SetUnitStrindex(2)
	p.AttributeIndices().Append(2) // profile attr
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.AttributeIndices().Append(3) // sample attr
	s.SetLinkIndex(0)
	s.Values().Append(42)

	pruned, err := sliceOTLPProfile(p, dict)
	if err != nil {
		t.Fatalf("slice: %v", err)
	}

	// whole-dict slice (old behavior) for size comparison
	whole := pprofile.NewProfiles()
	dict.CopyTo(whole.Dictionary())
	wrp := whole.ResourceProfiles().AppendEmpty()
	wsp := wrp.ScopeProfiles().AppendEmpty()
	p.CopyTo(wsp.Profiles().AppendEmpty())
	wholeBytes, err := pprofileotlp.NewExportRequestFromProfiles(whole).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned) >= len(wholeBytes) {
		t.Fatalf("pruned (%d) not smaller than whole (%d)", len(pruned), len(wholeBytes))
	}

	req := pprofileotlp.NewExportRequest()
	if err := req.UnmarshalProto(pruned); err != nil {
		t.Fatalf("unmarshal pruned: %v", err)
	}
	pd := req.Profiles().Dictionary()
	pp := req.Profiles().ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0)

	// structural validity
	validatePrunedIndices(t, pd, pp)

	// decoy absence
	for i := 0; i < pd.StringTable().Len(); i++ {
		if str := pd.StringTable().At(i); str == "DECOY_STRING" || str == "decoyFunc" {
			t.Fatalf("decoy string %q leaked into pruned dict", str)
		}
	}
	if pd.FunctionTable().Len() != 2 {
		t.Fatalf("pruned FunctionTable len=%d, want 2 (decoy dropped)", pd.FunctionTable().Len())
	}
	if pd.MappingTable().Len() != 1 {
		t.Fatalf("pruned MappingTable len=%d, want 1 (decoy dropped)", pd.MappingTable().Len())
	}
	if pd.LinkTable().Len() != 1 {
		t.Fatalf("pruned LinkTable len=%d, want 1", pd.LinkTable().Len())
	}
	if pd.AttributeTable().Len() != 4 {
		t.Fatalf("pruned AttributeTable len=%d, want 4", pd.AttributeTable().Len())
	}

	// functional equivalence: stack function names identical source vs pruned
	want := stackFuncNames(dict, p)
	got := stackFuncNames(pd, pp)
	if len(got) != len(want) || len(got[0]) != len(want[0]) {
		t.Fatalf("stack shape differs: want %+v got %+v", want, got)
	}
	for i := range want[0] {
		if got[0][i] != want[0][i] {
			t.Fatalf("frame %d name: want %q got %q", i, want[0][i], got[0][i])
		}
	}
	if got[0][0] != "worker" || got[0][1] != "main" {
		t.Fatalf("resolved names wrong: %+v", got[0])
	}

	// sample value preserved
	if pp.Samples().At(0).Values().At(0) != 42 {
		t.Fatalf("sample value not preserved")
	}

	// mapping filename still resolves through pruned dict
	mi := pd.LocationTable().At(int(pd.StackTable().At(int(pp.Samples().At(0).StackIndex())).LocationIndices().At(1))).MappingIndex()
	if strAt(pd.StringTable(), pd.MappingTable().At(int(mi)).FilenameStrindex()) != "lib.so" {
		t.Fatalf("mapping filename not preserved")
	}

	// a referenced attribute key resolves
	if strAt(pd.StringTable(), pd.AttributeTable().At(int(pp.AttributeIndices().At(0))).KeyStrindex()) != "thread" {
		t.Fatalf("profile attribute key not preserved")
	}
}
