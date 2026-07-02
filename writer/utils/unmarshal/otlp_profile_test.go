package unmarshal

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
)

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
