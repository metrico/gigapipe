package unmarshal

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
)

func newProfilesGRPCCache() numbercache.ICache[uint64] {
	return numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{})
}

// TestOTLPProfilesFromProfiles_NoPanicOnEmpty verifies the pre-decoded gRPC path
// runs the shared decode loop over an empty pprofile.Profiles without panicking
// or surfacing an error.
func TestOTLPProfilesFromProfiles_NoPanicOnEmpty(t *testing.T) {
	profs := pprofile.NewProfiles()
	cache := newProfilesGRPCCache()
	defer cache.Stop()

	ch := OTLPProfilesFromProfiles(profs)(context.Background(), nil, cache)
	for resp := range ch {
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
	}
}

// TestOTLPProfilesFromProfiles_EmitsProfile proves a real, populated
// pprofile.Profiles flows through the pre-decoded gRPC path
// (OTLPProfilesFromProfiles) into an emitted *model.ProfileData carrying the
// fixture's content. The fixture is identical to the HTTP path's
// TestOTLPProfilesDecEmitsProfile so the two paths are proven equivalent.
func TestOTLPProfilesFromProfiles_EmitsProfile(t *testing.T) {
	// Same one-symbolized-profile fixture as the HTTP TestOTLPProfilesDecEmitsProfile.
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

	cache := newProfilesGRPCCache()
	defer cache.Stop()

	ch := OTLPProfilesFromProfiles(profs)(context.Background(), nil, cache)

	var got []*model.ProfileData
	for resp := range ch {
		if resp.Error != nil {
			t.Fatalf("unexpected error: %v", resp.Error)
		}
		if resp.ProfileRequest == nil {
			continue
		}
		pd, ok := resp.ProfileRequest.(*model.ProfileData)
		if !ok {
			t.Fatalf("ProfileRequest is %T, want *model.ProfileData", resp.ProfileRequest)
		}
		got = append(got, pd)
	}

	if len(got) != 1 {
		t.Fatalf("want exactly 1 profile response, got %d", len(got))
	}
	pd := got[0]

	if len(pd.ServiceName) != 1 || pd.ServiceName[0] != "svc" {
		t.Fatalf("ServiceName = %v, want [svc]", pd.ServiceName)
	}
	if len(pd.Ptype) != 1 || pd.Ptype[0] != "cpu" {
		t.Fatalf("Ptype = %v, want [cpu]", pd.Ptype)
	}
	if len(pd.TimestampNs) != 1 || pd.TimestampNs[0] != 1_700_000_000_000_000_000 {
		t.Fatalf("TimestampNs = %v, want [1700000000000000000]", pd.TimestampNs)
	}
	if len(pd.DurationNs) != 1 || pd.DurationNs[0] != 1_000_000_000 {
		t.Fatalf("DurationNs = %v, want [1000000000]", pd.DurationNs)
	}
	if len(pd.PayloadType) != 1 || pd.PayloadType[0] != otlpProfilePayloadType {
		t.Fatalf("PayloadType = %v, want [%s]", pd.PayloadType, otlpProfilePayloadType)
	}
	if len(pd.Payload) != 1 || len(pd.Payload[0]) == 0 {
		t.Fatalf("Payload not carried through: %v", pd.Payload)
	}
	if len(pd.ValuesAgg) != 1 || pd.ValuesAgg[0].ValueInt64 != 42 {
		t.Fatalf("ValuesAgg = %+v, want single entry ValueInt64=42", pd.ValuesAgg)
	}
}
