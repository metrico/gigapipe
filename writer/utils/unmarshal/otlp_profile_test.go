package unmarshal

import (
	"testing"

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
