package service

import (
	"sync"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/writer/model"
)

func CreateColPools(size int32) {
	DatePool = newColPool(func() proto.ColDate {
		return make(proto.ColDate, 0, 10000)
	}, size).OnRelease(func(col *PooledColumn[proto.ColDate]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColDate]) int {
		return len(col.Data)
	})
	Int64Pool = newColPool(func() proto.ColInt64 {
		return make(proto.ColInt64, 0, 10000)
	}, size).OnRelease(func(col *PooledColumn[proto.ColInt64]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColInt64]) int {
		return len(col.Data)
	})

	UInt64Pool = newColPool(func() proto.ColUInt64 {
		return make(proto.ColUInt64, 0, 10000)
	}, size).OnRelease(func(col *PooledColumn[proto.ColUInt64]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColUInt64]) int {
		return len(col.Data)
	})

	UInt8Pool = newColPool(func() proto.ColUInt8 {
		return make(proto.ColUInt8, 0, 1024*1024)
	}, size).OnRelease(func(col *PooledColumn[proto.ColUInt8]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColUInt8]) int {
		return col.Data.Rows()
	})

	UInt64ArrayPool = newColPool(func() *proto.ColArr[uint64] {
		return proto.NewArray(&proto.ColUInt64{})
	}, size).
		OnRelease(func(col *PooledColumn[*proto.ColArr[uint64]]) {
			col.Data.Reset()
		}).
		OnGetSize(func(col *PooledColumn[*proto.ColArr[uint64]]) int {
			return col.Data.Rows()
		})

	Uint32ColPool = newColPool(func() proto.ColUInt32 {
		return make(proto.ColUInt32, 0, 10000)
	}, size).OnRelease(func(col *PooledColumn[proto.ColUInt32]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColUInt32]) int {
		return len(col.Data)
	})

	Float64Pool = newColPool(func() proto.ColFloat64 {
		return make(proto.ColFloat64, 0, 10000)
	}, size).OnRelease(func(col *PooledColumn[proto.ColFloat64]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColFloat64]) int {
		return len(col.Data)
	})
	StrPool = newColPool(func() *proto.ColStr {
		return &proto.ColStr{
			Buf: make([]byte, 0, 100000),
			Pos: make([]proto.Position, 0, 10000),
		}
	}, size).OnRelease(func(col *PooledColumn[*proto.ColStr]) {
		col.Data.Buf = col.Data.Buf[:0]
		col.Data.Pos = col.Data.Pos[:0]
	}).OnGetSize(func(col *PooledColumn[*proto.ColStr]) int {
		return col.Data.Rows()
	})
	FixedStringPool = newColPool(func() *proto.ColFixedStr {
		return &proto.ColFixedStr{
			Buf:  make([]byte, 0, 1024*1024),
			Size: 8,
		}
	}, size).OnRelease(func(col *PooledColumn[*proto.ColFixedStr]) {
		col.Data.Buf = col.Data.Buf[:0]
	}).OnGetSize(func(col *PooledColumn[*proto.ColFixedStr]) int {
		return col.Data.Rows()
	})
	Int8ColPool = newColPool(func() proto.ColInt8 {
		return make(proto.ColInt8, 0, 1024*1024)
	}, size).OnRelease(func(col *PooledColumn[proto.ColInt8]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColInt8]) int {
		return col.Data.Rows()
	})

	BoolColPool = newColPool(func() proto.ColBool {
		return make(proto.ColBool, 0, 1024*1024)
	}, size).OnRelease(func(col *PooledColumn[proto.ColBool]) {
		col.Data = col.Data[:0]
	}).OnGetSize(func(col *PooledColumn[proto.ColBool]) int {
		return col.Data.Rows()
	})
	Uint16ColPool = newColPool(func() proto.ColUInt16 {
		return make(proto.ColUInt16, 0, 1024*1024)
	}, size).OnRelease(func(column *PooledColumn[proto.ColUInt16]) {
		column.Data = column.Data[:0]
	}).OnGetSize(func(column *PooledColumn[proto.ColUInt16]) int {
		return column.Data.Rows()
	})

	TupleStrInt64Int32Pool = newColPool(func() *proto.ColArr[model.ValuesAgg] {
		return proto.NewArray(ColTupleStrInt64Int32Adapter{proto.ColTuple{&proto.ColStr{}, &proto.ColInt64{}, &proto.ColInt32{}}})
	},
		size).OnRelease(func(col *PooledColumn[*proto.ColArr[model.ValuesAgg]]) {
		col.Data.Reset()
	}).OnGetSize(func(col *PooledColumn[*proto.ColArr[model.ValuesAgg]]) int {
		return col.Data.Rows()
	})

	TupleUInt64StrPool = newColPool(func() *proto.ColArr[model.Function] {
		return proto.NewArray(ColTupleFunctionAdapter{proto.ColTuple{&proto.ColUInt64{}, &proto.ColStr{}}})
	}, size).OnRelease(func(col *PooledColumn[*proto.ColArr[model.Function]]) {
		col.Data.Reset()
	}).OnGetSize(func(col *PooledColumn[*proto.ColArr[model.Function]]) int {
		return col.Data.Rows()
	})

	TupleUInt64UInt64UInt64ArrPool = newColPool(func() *proto.ColArr[model.TreeRootStructure] {
		return proto.NewArray(ColTupleTreeAdapter{
			proto.ColTuple{
				&proto.ColUInt64{},
				&proto.ColUInt64{},
				&proto.ColUInt64{},
				proto.NewArray(ColTupleTreeValueAdapter{proto.ColTuple{
					&proto.ColStr{},
					&proto.ColInt64{},
					&proto.ColInt64{},
				}}),
			},
		})
	}, size).OnRelease(func(col *PooledColumn[*proto.ColArr[model.TreeRootStructure]]) {
		col.Data.Reset()
	}).OnGetSize(func(col *PooledColumn[*proto.ColArr[model.TreeRootStructure]]) int {
		return col.Data.Rows()
	})

	TupleStrStrPool = newColPool(func() *proto.ColArr[model.StrStr] {
		return proto.NewArray(ColTupleStrStrAdapter{proto.ColTuple{&proto.ColStr{}, &proto.ColStr{}}})
		//
		//return proto.ColArr[proto.ColTuple]{}
	}, size).OnRelease(func(col *PooledColumn[*proto.ColArr[model.StrStr]]) {
		col.Data.Reset()
	}).OnGetSize(func(col *PooledColumn[*proto.ColArr[model.StrStr]]) int {
		return col.Data.Rows()
	})

	UInt32ArrayPool = newColPool(func() *proto.ColArr[uint32] {
		return proto.NewArray(&proto.ColUInt32{})
	}, size).OnRelease(func(col *PooledColumn[*proto.ColArr[uint32]]) {
		col.Data.Reset()
	}).OnGetSize(func(col *PooledColumn[*proto.ColArr[uint32]]) int {
		return col.Data.Rows()
	})
	UInt32Pool = newColPool(func() proto.ColUInt32 {
		return proto.ColUInt32{}
	}, size).OnRelease(func(column *PooledColumn[proto.ColUInt32]) {
		column.Data.Reset()
	}).OnGetSize(func(col *PooledColumn[proto.ColUInt32]) int {
		return col.Data.Rows()
	})
	StrArrayPool = newColPool(func() *proto.ColArr[string] {
		return proto.NewArray(&proto.ColStr{})
	}, size).OnRelease(func(col *PooledColumn[*proto.ColArr[string]]) {
		col.Data.Reset()
	}).OnGetSize(func(col *PooledColumn[*proto.ColArr[string]]) int {
		return col.Data.Rows()
	})
}

var DatePool *colPool[proto.ColDate]
var Int64Pool *colPool[proto.ColInt64]
var UInt64Pool *colPool[proto.ColUInt64]
var UInt8Pool *colPool[proto.ColUInt8]
var UInt64ArrayPool *colPool[*proto.ColArr[uint64]]
var Float64Pool *colPool[proto.ColFloat64]
var StrPool *colPool[*proto.ColStr]
var FixedStringPool *colPool[*proto.ColFixedStr]
var Int8ColPool *colPool[proto.ColInt8]
var BoolColPool *colPool[proto.ColBool]
var Uint16ColPool *colPool[proto.ColUInt16]
var TupleStrStrPool *colPool[*proto.ColArr[model.StrStr]]
var UInt32ArrayPool *colPool[*proto.ColArr[uint32]]
var UInt32Pool *colPool[proto.ColUInt32]
var StrArrayPool *colPool[*proto.ColArr[string]]

var TupleStrInt64Int32Pool *colPool[*proto.ColArr[model.ValuesAgg]]
var TupleUInt64UInt64UInt64ArrPool *colPool[*proto.ColArr[model.TreeRootStructure]]
var TupleUInt64StrPool *colPool[*proto.ColArr[model.Function]]
var Uint32ColPool *colPool[proto.ColUInt32]
var acqMtx sync.Mutex

func StartAcq() {
	acqMtx.Lock()
}

func FinishAcq() {
	acqMtx.Unlock()
}
