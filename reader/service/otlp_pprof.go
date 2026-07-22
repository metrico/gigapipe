package service

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/prof"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
)

// rStrAt safely resolves a string-table index into its string value.
func rStrAt(st pcommon.StringSlice, idx int32) string {
	if idx < 0 || int(idx) >= st.Len() {
		return ""
	}
	return st.At(int(idx))
}

// otlpToPProf converts a stored OTLP profiles payload (one profile) into a
// Google-pprof Profile for the existing merge path.
func otlpToPProf(payload []byte) (*prof.Profile, error) {
	req := pprofileotlp.NewExportRequest()
	if err := req.UnmarshalProto(payload); err != nil {
		return nil, fmt.Errorf("otlpToPProf: unmarshal: %w", err)
	}
	profs := req.Profiles()
	dict := profs.Dictionary()
	st := dict.StringTable()

	rps := profs.ResourceProfiles()
	if rps.Len() == 0 || rps.At(0).ScopeProfiles().Len() == 0 ||
		rps.At(0).ScopeProfiles().At(0).Profiles().Len() == 0 {
		return nil, fmt.Errorf("otlpToPProf: empty profile")
	}
	p := rps.At(0).ScopeProfiles().At(0).Profiles().At(0)

	out := &prof.Profile{}

	// string table: index 0 must be "" for pprof
	strIdx := map[string]int64{"": 0}
	out.StringTable = []string{""}
	intern := func(s string) int64 {
		if id, ok := strIdx[s]; ok {
			return id
		}
		id := int64(len(out.StringTable))
		out.StringTable = append(out.StringTable, s)
		strIdx[s] = id
		return id
	}

	sType := rStrAt(st, p.SampleType().TypeStrindex())
	sUnit := rStrAt(st, p.SampleType().UnitStrindex())
	out.SampleType = []*prof.ValueType{{Type: intern(sType), Unit: intern(sUnit)}}
	out.PeriodType = &prof.ValueType{
		Type: intern(rStrAt(st, p.PeriodType().TypeStrindex())),
		Unit: intern(rStrAt(st, p.PeriodType().UnitStrindex())),
	}

	// build function + location tables lazily, keyed by OTLP index
	fnOut := map[int32]*prof.Function{}
	locOut := map[int32]*prof.Location{}
	var nextFn, nextLoc uint64 = 1, 1

	getFn := func(otlpFnIdx int32) *prof.Function {
		if f, ok := fnOut[otlpFnIdx]; ok {
			return f
		}
		name := ""
		if otlpFnIdx >= 0 && int(otlpFnIdx) < dict.FunctionTable().Len() {
			name = rStrAt(st, dict.FunctionTable().At(int(otlpFnIdx)).NameStrindex())
		}
		f := &prof.Function{Id: nextFn, Name: intern(name)}
		nextFn++
		fnOut[otlpFnIdx] = f
		out.Function = append(out.Function, f)
		return f
	}
	getLoc := func(otlpLocIdx int32) *prof.Location {
		if l, ok := locOut[otlpLocIdx]; ok {
			return l
		}
		l := &prof.Location{Id: nextLoc}
		nextLoc++
		if otlpLocIdx >= 0 && int(otlpLocIdx) < dict.LocationTable().Len() {
			otlpLoc := dict.LocationTable().At(int(otlpLocIdx))
			l.Address = otlpLoc.Address()
			for li := 0; li < otlpLoc.Lines().Len(); li++ {
				fnIdx := otlpLoc.Lines().At(li).FunctionIndex()
				l.Line = append(l.Line, &prof.Line{
					FunctionId: getFn(fnIdx).Id,
					Line:       otlpLoc.Lines().At(li).Line(),
				})
			}
		}
		locOut[otlpLocIdx] = l
		out.Location = append(out.Location, l)
		return l
	}

	stacks := dict.StackTable()
	for si := 0; si < p.Samples().Len(); si++ {
		s := p.Samples().At(si)
		var v int64
		for k := 0; k < s.Values().Len(); k++ {
			v += s.Values().At(k)
		}
		outSample := &prof.Sample{Value: []int64{v}}
		if idx := s.StackIndex(); idx >= 0 && int(idx) < stacks.Len() {
			li := stacks.At(int(idx)).LocationIndices()
			for i := 0; i < li.Len(); i++ {
				outSample.LocationId = append(outSample.LocationId, getLoc(li.At(i)).Id)
			}
		}
		out.Sample = append(out.Sample, outSample)
	}

	return out, nil
}
