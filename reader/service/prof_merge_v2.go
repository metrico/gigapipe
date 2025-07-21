package service

import (
	"github.com/metrico/qryn/reader/prof"
	"github.com/metrico/qryn/writer/utils/heputils/cityhash102"
	"google.golang.org/protobuf/proto"
)

type ProfileMergeV2 struct {
	prof          *prof.Profile
	stringTable   *RewriteTableV2[string]
	functionTable *RewriteTableV2[*prof.Function]
	mappingTable  *RewriteTableV2[*prof.Mapping]
	locationTable *RewriteTableV2[*prof.Location]
	sampleTable   *RewriteTableV2[*prof.Sample]
}

func NewProfileMergeV2() *ProfileMergeV2 {
	return &ProfileMergeV2{
		prof: nil,
		stringTable: NewRewriteTableV2(func(s string, i int64) string {
			return s
		}, hashString),
		functionTable: NewRewriteTableV2(func(function *prof.Function, i int64) *prof.Function {
			res := clone(function)
			res.Id = uint64(i)
			return res
		}, GetFunctionKey),
		mappingTable: NewRewriteTableV2(
			func(mapping *prof.Mapping, i int64) *prof.Mapping {
				res := clone(mapping)
				res.Id = uint64(i)
				return res
			}, GetMappingKey),
		locationTable: NewRewriteTableV2(func(location *prof.Location, i int64) *prof.Location {
			res := clone(location)
			res.Line = cloneArr(location.Line)
			res.Id = uint64(i)
			return res
		}, GetLocationKey),
		sampleTable: NewRewriteTableV2(func(sample *prof.Sample, i int64) *prof.Sample {
			res := clone(sample)
			res.Value = make([]int64, len(sample.Value))
			res.LocationId = append([]uint64{}, sample.LocationId...)
			res.Label = cloneArr(sample.Label)
			return res
		}, GetSampleKey),
	}
}

func (pm *ProfileMergeV2) Merge(p *prof.Profile) error {
	if len(p.Sample) == 0 || len(p.StringTable) < 2 {
		return nil
	}

	sanitizeProfile(p)

	strIdx := make([]int64, len(p.StringTable))
	for i := range p.StringTable {
		_strIdx, _ := pm.stringTable.Get(p.StringTable[i])
		strIdx[i] = int64(_strIdx) - 1
	}

	p.PeriodType.Type = strIdx[p.PeriodType.Type]
	p.PeriodType.Unit = strIdx[p.PeriodType.Unit]
	for _, s := range p.SampleType {
		s.Unit = strIdx[s.Unit]
		s.Type = strIdx[s.Type]
	}

	if pm.prof == nil {
		pm.init(p)
	}

	err := combineHeaders(pm.prof, p)
	if err != nil {
		return err
	}

	fnIdx := make(map[uint64]uint64, len(p.Function))
	for _, f := range p.Function {
		f.Name = strIdx[f.Name]
		f.Filename = strIdx[f.Filename]
		f.SystemName = strIdx[f.SystemName]
		fnIdx[f.Id], _ = pm.functionTable.Get(f)
	}

	mappingIdx := make(map[uint64]uint64, len(p.Mapping))
	for _, m := range p.Mapping {
		m.BuildId = strIdx[m.BuildId]
		m.Filename = strIdx[m.Filename]
		mappingIdx[m.Id], _ = pm.mappingTable.Get(m)
	}

	locationIdx := make(map[uint64]uint64, len(p.Location))
	for _, loc := range p.Location {
		for _, l := range loc.Line {
			l.FunctionId = fnIdx[l.FunctionId]
		}
		loc.MappingId = mappingIdx[loc.MappingId]
		locationIdx[loc.Id], _ = pm.locationTable.Get(loc)
	}

	for _, s := range p.Sample {
		for _, label := range s.Label {
			label.Key = strIdx[label.Key]
			label.Str = strIdx[label.Str]
		}
		for i := range s.LocationId {
			s.LocationId[i] = locationIdx[s.LocationId[i]]
		}
		_, _s := pm.sampleTable.Get(s)
		for i := range _s.Value {
			_s.Value[i] += s.Value[i]
		}
	}
	return nil
}

func (pm *ProfileMergeV2) init(p *prof.Profile) {
	prof := &prof.Profile{
		DropFrames:        p.DropFrames,
		KeepFrames:        p.KeepFrames,
		TimeNanos:         p.TimeNanos,
		PeriodType:        p.PeriodType,
		Period:            p.Period,
		DefaultSampleType: p.DefaultSampleType,
	}

	for _, s := range p.SampleType {
		prof.SampleType = append(prof.SampleType, clone(s))
	}
	pm.prof = prof
}

func (pm *ProfileMergeV2) Profile() *prof.Profile {
	if pm.prof == nil {
		return &prof.Profile{}
	}
	p := proto.Clone(pm.prof).(*prof.Profile)
	p.Sample = append([]*prof.Sample{}, pm.sampleTable.Values()...)
	p.Location = append([]*prof.Location{}, pm.locationTable.Values()...)
	p.Function = append([]*prof.Function{}, pm.functionTable.Values()...)
	p.Mapping = append([]*prof.Mapping{}, pm.mappingTable.Values()...)
	p.StringTable = append([]string{}, pm.stringTable.Values()...)

	for i := range p.Location {
		p.Location[i].Id = uint64(i + 1)
	}
	for i := range p.Function {
		p.Function[i].Id = uint64(i + 1)
	}
	for i := range p.Mapping {
		p.Mapping[i].Id = uint64(i + 1)
	}

	return p
}

func hashString(s string) uint64 {
	return cityhash102.CityHash64([]byte(s), uint32(len(s)))
}

func cloneArr[T any](arr []*T) []*T {
	res := make([]*T, len(arr))
	for i := range arr {
		res[i] = clone(arr[i])
	}
	return res
}

type RewriteTableV2[V any] struct {
	Map    map[uint64]uint64
	values []V
	clone  func(V, int64) V
	index  func(V) uint64
}

func NewRewriteTableV2[V any](clone func(V, int64) V, index func(V) uint64) *RewriteTableV2[V] {
	return &RewriteTableV2[V]{
		Map:    make(map[uint64]uint64),
		values: make([]V, 0),
		clone:  clone,
		index:  index,
	}
}

func (rt *RewriteTableV2[V]) Get(value V) (uint64, V) {
	idx := rt.index(value)
	if _idx, ok := rt.Map[idx]; ok {
		return _idx + 1, rt.values[_idx]
	}
	rt.Map[idx] = uint64(len(rt.values))
	rt.values = append(rt.values, rt.clone(value, int64(len(rt.values))+1))
	return uint64(len(rt.values)), rt.values[len(rt.values)-1]
}

func (rt *RewriteTableV2[V]) Values() []V {
	return rt.values
}
