package model

import (
	"github.com/ClickHouse/ch-go/proto"
	"time"
)

const (
	SAMPLE_TYPE_LOG    = 1
	SAMPLE_TYPE_METRIC = 2
	SAMPLE_TYPE_UNDEF  = 0
)

// Our replacement for gofaster.ch StrCol
type StrColumn interface {
	Append(v string)
	AppendBytes(v []byte)
	AppendArr(v []string)
}

type ByteColumn interface {
	Append(v []byte)
	AppendArr(v [][]byte)
}

type I8Column interface {
	Append(v int8)
	AppendArr(v []int8)
}

type I64Column interface {
	Append(v int64)
	AppendArr(v []int64)
}

type BColumn interface {
	Append(v bool)
	AppendArr(v []bool)
}

type I64ArrayColumn interface {
	Append(v []int64)
	AppendArr(v []int64)
}

type StrArrayColumn interface {
	Append(v []string)
}

type DateColumn interface {
	Append(v time.Time)
	AppendArr(v []time.Time)
}

type DateColumnV2 interface {
	Append(v time.Time)
	AppendArr(v []time.Time)
}

type UInt64Column interface {
	Append(v uint64)
	AppendArr(v []uint64)
}

type UInt8Column interface {
	Append(v uint8)
	AppendArr(v []uint8)
}

type UArrayInt64Column interface {
	Append(v []uint64)
	AppendArr(v [][]uint64)
}

type UInt32Column interface {
	Append(v uint32)
	AppendArr(v []uint32)
}

type UInt16Column interface {
	Append(v uint16)
	AppendArr(v []uint16)
}

type Float64Column interface {
	Append(v float64)
	AppendArr(v []float64)
}
type TempoSamplesRequest struct {
	TraceId     ByteColumn
	SpanId      ByteColumn
	ParentId    StrColumn
	Name        StrColumn
	TimestampNs I64Column
	DurationNs  I64Column
	ServiceName StrColumn
	PayloadType I8Column
	Payload     StrColumn
}

type TempoTagsRequest struct {
	Date        DateColumn
	Key         StrColumn
	Val         StrColumn
	TraceId     ByteColumn
	SpanId      ByteColumn
	TimestampNS I64Column
	DurationNS  I64Column
}

type TimeSeriesRequest struct {
	Type        UInt8Column
	Date        DateColumn
	Fingerprint UInt64Column
	Labels      StrColumn
	Meta        StrColumn
	TTLDays     UInt16Column
}

type ProfileSamplesRequest struct {
	TimestampNs       UInt64Column
	Ptype             StrColumn
	ServiceName       StrColumn
	SamplesTypesUnits *proto.ColArr[StrStr]
	PeriodType        StrColumn
	PeriodUnit        StrColumn
	Tags              *proto.ColArr[StrStr]
	DurationNs        UInt64Column
	PayloadType       StrColumn
	Payload           StrColumn
	ValuesAgg         *proto.ColArr[ValuesAgg]
	Tree              *proto.ColArr[TreeRootStructure]
	Functions         *proto.ColArr[Function]
}

type ProfileData struct {
	TimestampNs       []uint64
	Ptype             []string
	ServiceName       []string
	SamplesTypesUnits []StrStr
	PeriodType        []string
	PeriodUnit        []string
	Tags              []StrStr
	DurationNs        []uint64
	PayloadType       []string
	Payload           [][]byte
	ValuesAgg         []ValuesAgg
	Tree              []TreeRootStructure
	Function          []Function
	Size              int
}

func (t *ProfileData) GetSize() int64 {
	return int64(t.Size)
}

// ///////////////
type TimeSeriesData struct {
	MDate        []time.Time
	MLabels      []string
	MFingerprint []uint64
	MTTLDays     []uint16
	Size         int
	MType        []uint8
	MMeta        string
}

func (t *TimeSeriesData) GetSize() int64 {
	return int64(t.Size)
}

type TimeSamplesData struct {
	MFingerprint []uint64
	MTimestampNS []int64
	MMessage     []string
	MValue       []float64
	MTTLDays     []uint16
	Size         int
	MType        []uint8
}

func (t *TimeSamplesData) GetSize() int64 {
	return int64(t.Size)
}

type TempoTag struct {
	MTraceId     [][]byte
	MSpanId      [][]byte
	MTimestampNs []int64
	MDurationNs  []int64
	MDate        []time.Time
	MKey         []string
	MVal         []string
	Size         int
}

func (t *TempoTag) GetSize() int64 {
	return int64(t.Size)
}

type TempoSamples struct {
	MTraceId     [][]byte
	MSpanId      [][]byte
	MTimestampNs []int64
	MDurationNs  []int64

	MParentId    []string
	MName        []string
	MServiceName []string
	MPayloadType []int8
	MPayload     [][]byte
	Size         int
}

func (t *TempoSamples) GetSize() int64 {
	return int64(t.Size)
}

type PatternsData struct {
	MTimestamp10m    []uint32
	MFingerprint     []uint64
	MTimestampS      []uint32
	MTokens          [][]string
	MClasses         [][]uint32
	MWriterID        []string
	MOverallCost     []uint32
	MGeneralizedCost []uint32
	MSamplesCount    []uint32
	MPatternId       []uint64
	MIterationId     []uint64
}

func (t *PatternsData) GetSize() int64 {
	if len(t.MTimestamp10m) == 0 {
		return 0
	}
	//writerIdSize := len(t.MWriterID[0]) + 1 TODO
	arraysSize := int64(0)
	for i, tokens := range t.MTokens {
		for _, t := range tokens {
			arraysSize += int64(len(t) + 1)
		}
		arraysSize += int64(len(t.MClasses[i]) * 4)
	}
	return arraysSize + int64(
		len(t.MTimestamp10m)*4+
			len(t.MFingerprint)*8+
			len(t.MPatternId)*8+
			len(t.MIterationId)*4+
			len(t.MTimestampS)*4+
			len(t.MOverallCost)*4+
			len(t.MGeneralizedCost)*4+
			len(t.MSamplesCount)*4)
	//+len(t.MWriterID)*writerIdSize) TODO
}
