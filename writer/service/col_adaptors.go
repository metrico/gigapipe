package service

import (
	"github.com/ClickHouse/ch-go/proto"
	"time"
)

type DateAppender struct {
	D *proto.ColDate
}

func (d *DateAppender) Append(date time.Time) {
	d.D.Append(date)
}

func (d *DateAppender) AppendArr(date []time.Time) {
	for _, _d := range date {
		d.D.Append(_d)
	}
}

type Uint64Adaptor struct {
	*proto.ColUInt64
}

type Uint32Adaptor struct {
	*proto.ColUInt32
}

type UArray64Adaptor struct {
	*proto.ColArr[uint64]
}

func (u UArray64Adaptor) AppendArr(arr [][]uint64) {
	for _, a := range arr {
		u.ColArr.Append(a)
	}
}
func (u Uint32Adaptor) AppendArr(arr []uint32) {
	*u.ColUInt32 = append(*u.ColUInt32, arr...)
}

func (u Uint64Adaptor) AppendArr(arr []uint64) {
	*u.ColUInt64 = append(*u.ColUInt64, arr...)
}

type Uint16Adaptor struct {
	*proto.ColUInt16
}

func (u Uint16Adaptor) AppendArr(arr []uint16) {
	*u.ColUInt16 = append(*u.ColUInt16, arr...)
}

type Int64Adaptor struct {
	*proto.ColInt64
}

type Uint8Adaptor struct {
	*proto.ColUInt8
}

func (u Uint8Adaptor) AppendArr(arr []uint8) {
	*u.ColUInt8 = append(*u.ColUInt8, arr...)
}

func (u Int64Adaptor) AppendArr(arr []int64) {
	*u.ColInt64 = append(*u.ColInt64, arr...)
}

type FixedStrAdaptor struct {
	*proto.ColFixedStr
}

func (u FixedStrAdaptor) AppendArr(arr [][]byte) {
	for _, e := range arr {
		u.ColFixedStr.Append(e)
	}
}

type I8Adaptor struct {
	*proto.ColInt8
}

func (u I8Adaptor) AppendArr(arr []int8) {
	*u.ColInt8 = append(*u.ColInt8, arr...)
}

type F64Adaptor struct {
	*proto.ColFloat64
}

func (u F64Adaptor) AppendArr(arr []float64) {
	*u.ColFloat64 = append(*u.ColFloat64, arr...)
}
