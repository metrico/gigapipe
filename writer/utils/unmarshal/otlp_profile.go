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

// sortedInt32Keys returns the keys of a set sorted ascending (deterministic remap).
func sortedInt32Keys(m map[int32]struct{}) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// buildRemap maps each old index (in ascending order) to its new compact index.
func buildRemap(keys []int32) map[int32]int32 {
	r := make(map[int32]int32, len(keys))
	for newI, oldI := range keys {
		r[oldI] = int32(newI)
	}
	return r
}

// sliceOTLPProfile builds a standalone ExportRequest carrying exactly one profile
// plus ONLY the dictionary entries that profile transitively references, compacted
// and index-remapped. The stored payload decodes independently at read time without
// dragging along the whole shared dictionary (which otherwise dominates payload size
// and write/read CPU on batched exports).
func sliceOTLPProfile(p pprofile.Profile, dict pprofile.ProfilesDictionary) ([]byte, error) {
	srcStr := dict.StringTable()
	srcFunc := dict.FunctionTable()
	srcLoc := dict.LocationTable()
	srcMap := dict.MappingTable()
	srcStack := dict.StackTable()
	srcAttr := dict.AttributeTable()
	srcLink := dict.LinkTable()

	refStr := map[int32]struct{}{}
	refFunc := map[int32]struct{}{}
	refLoc := map[int32]struct{}{}
	refMap := map[int32]struct{}{}
	refStack := map[int32]struct{}{}
	refAttr := map[int32]struct{}{}
	refLink := map[int32]struct{}{}

	addStr := func(i int32) {
		if i >= 0 && int(i) < srcStr.Len() {
			refStr[i] = struct{}{}
		}
	}
	addAttrIdxs := func(s pcommon.Int32Slice) {
		for k := 0; k < s.Len(); k++ {
			if i := s.At(k); i >= 0 && int(i) < srcAttr.Len() {
				refAttr[i] = struct{}{}
			}
		}
	}

	// 1. Seed from the profile itself (sample/period types, profile + sample
	//    attributes, referenced stacks and links). Order 1..6 below is a single
	//    ordered pass: the graph is acyclic toward strings (leaves), and every
	//    contributor to a set runs before that set is expanded.
	addStr(p.SampleType().TypeStrindex())
	addStr(p.SampleType().UnitStrindex())
	addStr(p.PeriodType().TypeStrindex())
	addStr(p.PeriodType().UnitStrindex())
	addAttrIdxs(p.AttributeIndices())
	for si := 0; si < p.Samples().Len(); si++ {
		s := p.Samples().At(si)
		if idx := s.StackIndex(); idx >= 0 && int(idx) < srcStack.Len() {
			refStack[idx] = struct{}{}
		}
		addAttrIdxs(s.AttributeIndices())
		if idx := s.LinkIndex(); idx >= 0 && int(idx) < srcLink.Len() {
			refLink[idx] = struct{}{}
		}
	}
	// 2. Stacks -> locations.
	for idx := range refStack {
		li := srcStack.At(int(idx)).LocationIndices()
		for k := 0; k < li.Len(); k++ {
			if l := li.At(k); l >= 0 && int(l) < srcLoc.Len() {
				refLoc[l] = struct{}{}
			}
		}
	}
	// 3. Locations -> mappings, functions, attributes.
	for idx := range refLoc {
		loc := srcLoc.At(int(idx))
		if m := loc.MappingIndex(); m >= 0 && int(m) < srcMap.Len() {
			refMap[m] = struct{}{}
		}
		lines := loc.Lines()
		for k := 0; k < lines.Len(); k++ {
			if f := lines.At(k).FunctionIndex(); f >= 0 && int(f) < srcFunc.Len() {
				refFunc[f] = struct{}{}
			}
		}
		addAttrIdxs(loc.AttributeIndices())
	}
	// 4. Mappings -> strings, attributes.
	for idx := range refMap {
		m := srcMap.At(int(idx))
		addStr(m.FilenameStrindex())
		addAttrIdxs(m.AttributeIndices())
	}
	// 5. Functions -> strings.
	for idx := range refFunc {
		f := srcFunc.At(int(idx))
		addStr(f.NameStrindex())
		addStr(f.SystemNameStrindex())
		addStr(f.FilenameStrindex())
	}
	// 6. Attributes -> strings.
	for idx := range refAttr {
		a := srcAttr.At(int(idx))
		addStr(a.KeyStrindex())
		addStr(a.UnitStrindex())
	}

	strKeys := sortedInt32Keys(refStr)
	funcKeys := sortedInt32Keys(refFunc)
	locKeys := sortedInt32Keys(refLoc)
	mapKeys := sortedInt32Keys(refMap)
	stackKeys := sortedInt32Keys(refStack)
	attrKeys := sortedInt32Keys(refAttr)
	linkKeys := sortedInt32Keys(refLink)

	remapStr := buildRemap(strKeys)
	remapFunc := buildRemap(funcKeys)
	remapLoc := buildRemap(locKeys)
	remapMap := buildRemap(mapKeys)
	remapStack := buildRemap(stackKeys)
	remapAttr := buildRemap(attrKeys)
	remapLink := buildRemap(linkKeys)

	// remapIdx returns the new index for old, preserving unset/negative or any
	// index outside the referenced set (e.g. -1) as-is.
	remapIdx := func(old int32, remap map[int32]int32) int32 {
		if nv, ok := remap[old]; ok {
			return nv
		}
		return old
	}
	remapIdxSlice := func(s pcommon.Int32Slice, remap map[int32]int32) []int32 {
		out := make([]int32, 0, s.Len())
		for k := 0; k < s.Len(); k++ {
			if nv, ok := remap[s.At(k)]; ok {
				out = append(out, nv)
			}
		}
		return out
	}

	out := pprofile.NewProfiles()
	dst := out.Dictionary()

	dstStr := dst.StringTable()
	for _, old := range strKeys {
		dstStr.Append(srcStr.At(int(old)))
	}
	for _, old := range funcKeys {
		f := srcFunc.At(int(old))
		nf := dst.FunctionTable().AppendEmpty()
		nf.SetNameStrindex(remapIdx(f.NameStrindex(), remapStr))
		nf.SetSystemNameStrindex(remapIdx(f.SystemNameStrindex(), remapStr))
		nf.SetFilenameStrindex(remapIdx(f.FilenameStrindex(), remapStr))
		nf.SetStartLine(f.StartLine())
	}
	for _, old := range mapKeys {
		m := srcMap.At(int(old))
		nm := dst.MappingTable().AppendEmpty()
		m.CopyTo(nm)
		nm.SetFilenameStrindex(remapIdx(m.FilenameStrindex(), remapStr))
		nm.AttributeIndices().FromRaw(remapIdxSlice(m.AttributeIndices(), remapAttr))
	}
	for _, old := range locKeys {
		l := srcLoc.At(int(old))
		nl := dst.LocationTable().AppendEmpty()
		l.CopyTo(nl)
		nl.SetMappingIndex(remapIdx(l.MappingIndex(), remapMap))
		lines := nl.Lines()
		for k := 0; k < lines.Len(); k++ {
			ln := lines.At(k)
			ln.SetFunctionIndex(remapIdx(ln.FunctionIndex(), remapFunc))
		}
		nl.AttributeIndices().FromRaw(remapIdxSlice(l.AttributeIndices(), remapAttr))
	}
	for _, old := range stackKeys {
		s := srcStack.At(int(old))
		ns := dst.StackTable().AppendEmpty()
		ns.LocationIndices().FromRaw(remapIdxSlice(s.LocationIndices(), remapLoc))
	}
	for _, old := range attrKeys {
		a := srcAttr.At(int(old))
		na := dst.AttributeTable().AppendEmpty()
		a.CopyTo(na)
		na.SetKeyStrindex(remapIdx(a.KeyStrindex(), remapStr))
		na.SetUnitStrindex(remapIdx(a.UnitStrindex(), remapStr))
	}
	for _, old := range linkKeys {
		srcLink.At(int(old)).CopyTo(dst.LinkTable().AppendEmpty())
	}

	rp := out.ResourceProfiles().AppendEmpty()
	sp := rp.ScopeProfiles().AppendEmpty()
	np := sp.Profiles().AppendEmpty()
	p.CopyTo(np)
	np.SampleType().SetTypeStrindex(remapIdx(np.SampleType().TypeStrindex(), remapStr))
	np.SampleType().SetUnitStrindex(remapIdx(np.SampleType().UnitStrindex(), remapStr))
	np.PeriodType().SetTypeStrindex(remapIdx(np.PeriodType().TypeStrindex(), remapStr))
	np.PeriodType().SetUnitStrindex(remapIdx(np.PeriodType().UnitStrindex(), remapStr))
	np.AttributeIndices().FromRaw(remapIdxSlice(np.AttributeIndices(), remapAttr))
	for si := 0; si < np.Samples().Len(); si++ {
		s := np.Samples().At(si)
		s.SetStackIndex(remapIdx(s.StackIndex(), remapStack))
		s.AttributeIndices().FromRaw(remapIdxSlice(s.AttributeIndices(), remapAttr))
		s.SetLinkIndex(remapIdx(s.LinkIndex(), remapLink))
	}

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
