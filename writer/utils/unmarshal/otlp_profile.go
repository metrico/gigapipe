package unmarshal

import (
	"fmt"

	"github.com/metrico/qryn/v4/writer/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
)

const otlpProfilePayloadType = "otel_v1development"

type otlpProfileMeta struct {
	TimestampNs      uint64
	DurationNs       uint64
	Type             string
	ServiceName      string
	SampleTypesUnits []model.StrStr
	PeriodType       string
	PeriodUnit       string
	Tags             []model.StrStr
}

// strAt safely resolves a string-table index; returns "" for out-of-range.
func strAt(st pcommon.StringSlice, idx int32) string {
	if idx < 0 || int(idx) >= st.Len() {
		return ""
	}
	return st.At(int(idx))
}

func extractOTLPMeta(res pcommon.Resource, scope pcommon.InstrumentationScope,
	p pprofile.Profile, dict pprofile.ProfilesDictionary) otlpProfileMeta {
	st := dict.StringTable()

	sampleType := strAt(st, p.SampleType().TypeStrindex())
	sampleUnit := strAt(st, p.SampleType().UnitStrindex())

	m := otlpProfileMeta{
		TimestampNs:      uint64(p.Time()),
		DurationNs:       p.DurationNano(),
		Type:             sampleType,
		SampleTypesUnits: []model.StrStr{{Str1: sampleType, Str2: sampleUnit}},
		PeriodType:       strAt(st, p.PeriodType().TypeStrindex()),
		PeriodUnit:       strAt(st, p.PeriodType().UnitStrindex()),
		ServiceName:      "unknown_service",
	}

	appendAttrs := func(attrs pcommon.Map) {
		attrs.Range(func(k string, v pcommon.Value) bool {
			if k == "service.name" {
				m.ServiceName = v.AsString()
				return true
			}
			m.Tags = append(m.Tags, model.StrStr{Str1: k, Str2: v.AsString()})
			return true
		})
	}
	appendAttrs(res.Attributes())
	appendAttrs(scope.Attributes())

	return m
}

var _ = fmt.Sprint // keep fmt import for later steps
