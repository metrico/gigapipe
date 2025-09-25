package service

import (
	"fmt"
	"sort"
	"unsafe"

	"github.com/metrico/qryn/reader/prof"
	"github.com/metrico/qryn/writer/utils/helputils/cityhash102"
)

func clone[T any](v *T) *T {
	tmp := *v
	return &tmp
}

func sanitizeProfile(p *prof.Profile) {
	if p == nil {
		return
	}

	ms := int64(len(p.StringTable))
	z := int64(-1)
	for i, s := range p.StringTable {
		if s == "" {
			z = int64(i)
			break
		}
	}
	if z == -1 {
		z = ms
		p.StringTable = append(p.StringTable, "")
		ms++
	}

	tmp := p.StringTable[0]
	p.StringTable[0] = p.StringTable[z]
	p.StringTable[z] = tmp

	str := func(i int64) int64 {
		if i == 0 && z > 0 {
			return z
		}
		if i == z || i >= ms || i < 0 {
			return 0
		}
		return i
	}

	p.SampleType = removeInPlace(p.SampleType, func(x *prof.ValueType) bool {
		x.Type = str(x.Type)
		x.Unit = str(x.Unit)
		return false
	})

	if p.PeriodType != nil {
		p.PeriodType.Type = str(p.PeriodType.Type)
		p.PeriodType.Unit = str(p.PeriodType.Unit)
	}

	p.DefaultSampleType = str(p.DefaultSampleType)
	p.DropFrames = str(p.DropFrames)
	p.KeepFrames = str(p.KeepFrames)
	for i := range p.Comment {
		p.Comment[i] = str(p.Comment[i])
	}

	t := make(map[uint64]uint64)
	j := uint64(1)
	p.Mapping = removeInPlace(p.Mapping, func(x *prof.Mapping) bool {
		x.BuildId = str(x.BuildId)
		x.Filename = str(x.Filename)
		t[x.Id] = j
		x.Id = j
		j++
		return false
	})

	var mapping *prof.Mapping
	p.Location = removeInPlace(p.Location, func(x *prof.Location) bool {
		if x.MappingId == 0 {
			if mapping == nil {
				mapping = &prof.Mapping{Id: uint64(len(p.Mapping)) + 1}
				p.Mapping = append(p.Mapping, mapping)
			}
			x.MappingId = mapping.Id
			return false
		}
		x.MappingId = t[x.MappingId]
		return x.MappingId == 0
	})

	t = make(map[uint64]uint64)
	j = 1
	p.Function = removeInPlace(p.Function, func(x *prof.Function) bool {
		x.Name = str(x.Name)
		x.SystemName = str(x.SystemName)
		x.Filename = str(x.Filename)
		t[x.Id] = j
		x.Id = j
		j++
		return false
	})

	p.Location = removeInPlace(p.Location, func(x *prof.Location) bool {
		for i := range x.Line {
			line := x.Line[i]
			line.FunctionId = t[line.FunctionId]
			if line.FunctionId == 0 {
				return true
			}
		}
		return false
	})

	t = make(map[uint64]uint64)
	j = 1
	for i := range p.Location {
		x := p.Location[i]
		t[x.Id] = j
		x.Id = j
		j++
	}

	vs := len(p.SampleType)
	p.Sample = removeInPlace(p.Sample, func(x *prof.Sample) bool {
		if len(x.Value) != vs {
			return true
		}
		for i := range x.LocationId {
			x.LocationId[i] = t[x.LocationId[i]]
			if x.LocationId[i] == 0 {
				return true
			}
		}
		for i := range x.Label {
			l := x.Label[i]
			l.Key = str(l.Key)
			l.Str = str(l.Str)
			l.NumUnit = str(l.NumUnit)
		}
		return false
	})
}

func removeInPlace[T any](slice []T, predicate func(T) bool) []T {
	n := 0
	for i := range slice {
		if !predicate(slice[i]) {
			slice[n] = slice[i]
			n++
		}
	}
	return slice[:n]
}

func combineHeaders(a, b *prof.Profile) error {
	err := compatible(a, b)
	if err != nil {
		return err
	}
	if a.TimeNanos == 0 || b.TimeNanos < a.TimeNanos {
		a.TimeNanos = b.TimeNanos
	}
	a.DurationNanos += b.DurationNanos
	if a.Period == 0 || a.Period < b.Period {
		a.Period = b.Period
	}
	if a.DefaultSampleType == 0 {
		a.DefaultSampleType = b.DefaultSampleType
	}
	return nil
}

// You'll need to implement the compatible function as well
func compatible(a, b *prof.Profile) error {
	if !equalValueType(a.PeriodType, b.PeriodType) {
		return fmt.Errorf("incompatible period types %v and %v", a.PeriodType, b.PeriodType)
	}
	if len(b.SampleType) != len(a.SampleType) {
		return fmt.Errorf("incompatible sample types %v and %v", a.SampleType, b.SampleType)
	}
	for i := 0; i < len(a.SampleType); i++ {
		if !equalValueType(a.SampleType[i], b.SampleType[i]) {
			return fmt.Errorf("incompatible sample types %v and %v", a.SampleType, b.SampleType)
		}
	}
	return nil
}

func equalValueType(vt1, vt2 *prof.ValueType) bool {
	if vt1 == nil || vt2 == nil {
		return false
	}
	return vt1.Type == vt2.Type && vt1.Unit == vt2.Unit
}

func GetFunctionKey(f *prof.Function) uint64 {
	str := fmt.Sprintf("%d:%d:%d:%d", f.StartLine, f.Name, f.SystemName, f.Filename)
	return cityhash102.CityHash64([]byte(str), uint32(len(str)))
}

func GetMappingKey(m *prof.Mapping) uint64 {
	mapSizeRounding := uint64(0x1000)
	size := m.MemoryLimit - m.MemoryStart
	size = size + mapSizeRounding - 1
	size = size - (size % mapSizeRounding)

	var buildIdOrFile int64
	if m.BuildId != 0 {
		buildIdOrFile = m.BuildId
	} else if m.Filename != 0 {
		buildIdOrFile = m.Filename
	}

	str := fmt.Sprintf("%d:%d:%d", size, m.FileOffset, buildIdOrFile)
	return cityhash102.CityHash64([]byte(str), uint32(len(str)))
}

func GetLocationKey(l *prof.Location) uint64 {
	lines := hashLines(l.Line)
	str := fmt.Sprintf("%d:%d:%d", l.Address, lines, l.MappingId)
	return cityhash102.CityHash64([]byte(str), uint32(len(str)))
}

func hashLines(lines []*prof.Line) uint64 {
	x := make([]uint64, len(lines))
	for i, line := range lines {
		x[i] = line.FunctionId | (uint64(line.Line) << 32)
	}

	// Convert []uint64 to []byte
	u8Arr := (*[1 << 30]byte)(unsafe.Pointer(&x[0]))[:len(x)*8]

	return cityhash102.CityHash64(u8Arr, uint32(len(u8Arr)))
}

func GetSampleKey(s *prof.Sample) uint64 {
	locations := hashLocations(s.LocationId)
	labels := hashProfileLabels(s.Label)
	str := fmt.Sprintf("%d:%d", locations, labels)
	return cityhash102.CityHash64([]byte(str), uint32(len(str)))
}

func hashProfileLabels(labels []*prof.Label) uint64 {
	if len(labels) == 0 {
		return 0
	}

	// Create a copy of labels to sort
	_labels := make([]*prof.Label, len(labels))
	copy(_labels, labels)

	// Sort labels
	sort.Slice(_labels, func(i, j int) bool {
		if _labels[i].Key < _labels[j].Key {
			return true
		}
		if _labels[i].Key == _labels[j].Key && _labels[i].Str < _labels[j].Str {
			return true
		}
		return false
	})

	arr := make([]uint64, len(_labels))
	for i, label := range _labels {
		arr[i] = uint64(label.Key) | (uint64(label.Str) << 32)
	}

	// Convert []uint64 to []byte
	u8Arr := unsafe.Slice((*byte)(unsafe.Pointer(&arr[0])), len(arr)*8)

	return cityhash102.CityHash64(u8Arr, uint32(len(u8Arr)))
}

func hashLocations(locations []uint64) uint64 {
	// Convert []uint64 to []byte
	u8Arr := unsafe.Slice((*byte)(unsafe.Pointer(&locations[0])), len(locations)*8)

	return cityhash102.CityHash64(u8Arr, uint32(len(u8Arr)))
}
