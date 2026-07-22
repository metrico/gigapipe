package service

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
)

func TestOtlpToPProf(t *testing.T) {
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
	sp := rp.ScopeProfiles().AppendEmpty()
	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.Values().Append(9)

	payload, err := pprofileotlp.NewExportRequestFromProfiles(profs).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}

	out, err := otlpToPProf(payload)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if len(out.SampleType) != 1 {
		t.Fatalf("sample types: %d", len(out.SampleType))
	}
	if len(out.Sample) != 1 || out.Sample[0].Value[0] != 9 {
		t.Fatalf("sample value not preserved: %+v", out.Sample)
	}
	// "main" present in the pprof string table
	found := false
	for _, str := range out.StringTable {
		if str == "main" {
			found = true
		}
	}
	if !found {
		t.Fatalf("function name not carried into pprof string table")
	}
}
