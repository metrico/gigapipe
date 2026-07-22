package unmarshal

import (
	"fmt"
	"io"
	"sort"

	"github.com/go-faster/city"
	"github.com/metrico/qryn/v4/writer/model"
	sharedotlp "github.com/metrico/qryn/v4/shared/otlp"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
)

// otlpProfilePayloadType is the storage payload_type for OTLP profile rows;
// sourced from shared/otlp so writer and reader share one definition.
const otlpProfilePayloadType = sharedotlp.ProfilePayloadType

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

// sliceOTLPProfile builds a standalone ExportRequest with one profile + the
// source dictionary, so the stored payload decodes independently at read time.
func sliceOTLPProfile(p pprofile.Profile, dict pprofile.ProfilesDictionary) ([]byte, error) {
	out := pprofile.NewProfiles()
	dict.CopyTo(out.Dictionary())
	rp := out.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	p.CopyTo(sp.Profiles().AppendEmpty())

	return pprofileotlp.NewExportRequestFromProfiles(out).MarshalProto()
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

// frameName resolves a location's leaf function name, or a build-id+addr fallback.
// The resolved pdata pprofile.Mapping type exposes no build-id string accessor
// (only FilenameStrindex), so the fallback uses an empty build id.
func frameName(loc pprofile.Location, dict pprofile.ProfilesDictionary) string {
	st := dict.StringTable()
	if loc.Lines().Len() > 0 {
		fnIdx := loc.Lines().At(0).FunctionIndex()
		if fnIdx >= 0 && int(fnIdx) < dict.FunctionTable().Len() {
			name := strAt(st, dict.FunctionTable().At(int(fnIdx)).NameStrindex())
			if name != "" {
				return name
			}
		}
	}
	buildID := ""
	return fmt.Sprintf("%s+0x%x", buildID, loc.Address())
}

func buildOTLPTree(p pprofile.Profile, dict pprofile.ProfilesDictionary) (
	[]model.Function, []model.TreeRootStructure, []model.ValuesAgg) {

	st := dict.StringTable()
	sampleType := strAt(st, p.SampleType().TypeStrindex())
	sampleUnit := strAt(st, p.SampleType().UnitStrindex())
	valueName := fmt.Sprintf("%s:%s", sampleType, sampleUnit)

	locs := dict.LocationTable()
	stacks := dict.StackTable()

	funcs := map[uint64]string{}
	tree := map[uint64]*profTrieNode{}
	var totalSum int64
	var sampleCount int32

	for si := 0; si < p.Samples().Len(); si++ {
		sample := p.Samples().At(si)

		// aggregate this sample's values by sum
		var v int64
		for k := 0; k < sample.Values().Len(); k++ {
			v += sample.Values().At(k)
		}
		totalSum += v
		sampleCount++

		stackIdx := sample.StackIndex()
		if stackIdx < 0 || int(stackIdx) >= stacks.Len() {
			continue
		}
		li := stacks.At(int(stackIdx)).LocationIndices()

		parentId := uint64(0)
		for i := li.Len() - 1; i >= 0; i-- {
			locIdx := li.At(i)
			name := "n/a"
			if locIdx >= 0 && int(locIdx) < locs.Len() {
				name = frameName(locs.At(int(locIdx)), dict)
			}
			fnId := city.CH64([]byte(name))
			funcs[fnId] = name
			nodeId := getNodeId(parentId, fnId, li.Len()-i)
			node := tree[nodeId]
			if node == nil {
				node = &profTrieNode{
					parentId: parentId,
					funcId:   fnId,
					nodeId:   nodeId,
					values:   []profTrieValue{{name: valueName}},
				}
				tree[nodeId] = node
			}
			node.values[0].total += v
			if i == 0 {
				node.values[0].self += v
			}
			parentId = nodeId
		}
	}

	// functions, ordered descending by id to match postProcessProf
	fnIdx := make([]uint64, 0, len(funcs))
	for id := range funcs {
		fnIdx = append(fnIdx, id)
	}
	sort.Slice(fnIdx, func(i, j int) bool { return fnIdx[i] > fnIdx[j] })
	functions := make([]model.Function, 0, len(fnIdx))
	for _, id := range fnIdx {
		functions = append(functions, model.Function{ValueInt64: id, ValueStr: funcs[id]})
	}

	// tree, ordered descending by node id to match postProcessProf
	tIdx := make([]uint64, 0, len(tree))
	for id := range tree {
		tIdx = append(tIdx, id)
	}
	sort.Slice(tIdx, func(i, j int) bool { return tIdx[i] > tIdx[j] })
	treeRes := make([]model.TreeRootStructure, 0, len(tIdx))
	for _, id := range tIdx {
		n := tree[id]
		vals := make([]model.ValuesArrTuple, len(n.values))
		for k, v := range n.values {
			vals[k] = model.ValuesArrTuple{ValueStr: v.name, FirstValueInt64: v.self, SecondValueInt64: v.total}
		}
		treeRes = append(treeRes, model.TreeRootStructure{
			Field1: n.parentId, Field2: n.funcId, Field3: n.nodeId, ValueArrTuple: vals,
		})
	}

	valuesAgg := []model.ValuesAgg{{ValueStr: valueName, ValueInt64: totalSum, ValueInt32: sampleCount}}
	return functions, treeRes, valuesAgg
}

// otlpProfilesDec decodes an OTLP profiles ExportRequest and emits one
// onProfiles call per profile in the request.
type otlpProfilesDec struct {
	ctx        *ParserCtx
	onProfiles onProfileHandler
}

func (d *otlpProfilesDec) SetOnProfile(h onProfileHandler) { d.onProfiles = h }

func (d *otlpProfilesDec) Decode() error {
	data, err := io.ReadAll(d.ctx.bodyReader)
	if err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
	req := pprofileotlp.NewExportRequest()
	if err := req.UnmarshalProto(data); err != nil {
		return fmt.Errorf("failed to unmarshal OTLP profiles: %w", err)
	}
	profs := req.Profiles()
	dict := profs.Dictionary()

	rps := profs.ResourceProfiles()
	for i := 0; i < rps.Len(); i++ {
		rp := rps.At(i)
		sps := rp.ScopeProfiles()
		for j := 0; j < sps.Len(); j++ {
			sp := sps.At(j)
			ps := sp.Profiles()
			for k := 0; k < ps.Len(); k++ {
				p := ps.At(k)

				meta := extractOTLPMeta(rp.Resource(), sp.Scope(), p, dict)
				functions, tree, valuesAgg := buildOTLPTree(p, dict)
				payload, err := sliceOTLPProfile(p, dict)
				if err != nil {
					return fmt.Errorf("failed to slice OTLP profile: %w", err)
				}

				if err := d.onProfiles(
					meta.TimestampNs, meta.Type, meta.ServiceName,
					meta.SampleTypesUnits, meta.PeriodType, meta.PeriodUnit,
					meta.Tags, meta.DurationNs, otlpProfilePayloadType, payload,
					valuesAgg, tree, functions,
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

var UnmarshalOTLPProfilesProtoV2 = Build(
	withProfileParser(func(ctx *ParserCtx) iProfilesParser {
		return &otlpProfilesDec{ctx: ctx}
	}))
