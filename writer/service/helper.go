package service

import (
	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v4/writer/model"
)

type ColTupleStrStrAdapter struct {
	proto.ColTuple
}

func (c ColTupleStrStrAdapter) AppendArr(v []model.StrStr) {
	for _, data := range v {
		c.ColTuple[0].(*proto.ColStr).Append(data.Str1)
		c.ColTuple[1].(*proto.ColStr).Append(data.Str2)
	}

}

type ColTupleStrInt64Int32Adapter struct {
	proto.ColTuple
}

func (c ColTupleStrInt64Int32Adapter) AppendArr(v []model.ValuesAgg) {
	for _, data := range v {
		c.ColTuple[0].(*proto.ColStr).Append(data.ValueStr)
		c.ColTuple[1].(*proto.ColInt64).Append(data.ValueInt64)
		c.ColTuple[2].(*proto.ColInt32).Append(data.ValueInt32)
	}
}

type ColTupleFunctionAdapter struct {
	proto.ColTuple
}

func (c ColTupleFunctionAdapter) AppendArr(v []model.Function) {
	for _, data := range v {
		c.ColTuple[0].(*proto.ColUInt64).Append(data.ValueInt64)
		c.ColTuple[1].(*proto.ColStr).Append(data.ValueStr)
	}
}

type ColTupleTreeAdapter struct {
	proto.ColTuple
}

func (c ColTupleTreeAdapter) AppendArr(v []model.TreeRootStructure) {
	for _, data := range v {
		c.ColTuple[0].(*proto.ColUInt64).Append(data.Field1)
		c.ColTuple[1].(*proto.ColUInt64).Append(data.Field2)
		c.ColTuple[2].(*proto.ColUInt64).Append(data.Field3)
		c.ColTuple[3].(*proto.ColArr[model.ValuesArrTuple]).Append(data.ValueArrTuple)
	}
}

type ColTupleTreeValueAdapter struct {
	proto.ColTuple
}

func (c ColTupleTreeValueAdapter) AppendArr(v []model.ValuesArrTuple) {
	for _, data := range v {
		c.ColTuple[0].(*proto.ColStr).Append(data.ValueStr)
		c.ColTuple[1].(*proto.ColInt64).Append(data.FirstValueInt64)
		c.ColTuple[2].(*proto.ColInt64).Append(data.SecondValueInt64)
	}
}

func (c ColTupleTreeValueAdapter) Append(v model.ValuesArrTuple) {
	c.ColTuple[0].(*proto.ColStr).Append(v.ValueStr)
	c.ColTuple[1].(*proto.ColInt64).Append(v.FirstValueInt64)
	c.ColTuple[2].(*proto.ColInt64).Append(v.SecondValueInt64)

}

func (c ColTupleTreeValueAdapter) Row(i int) model.ValuesArrTuple {
	return model.ValuesArrTuple{ValueStr: c.ColTuple[0].(*proto.ColStr).Row(i),
		FirstValueInt64:  c.ColTuple[1].(*proto.ColInt64).Row(i),
		SecondValueInt64: c.ColTuple[1].(*proto.ColInt64).Row(i),
	}
}

func (c ColTupleTreeAdapter) Append(v model.TreeRootStructure) {
	c.ColTuple[0].(*proto.ColUInt64).Append(v.Field1)
	c.ColTuple[1].(*proto.ColUInt64).Append(v.Field2)
	c.ColTuple[2].(*proto.ColUInt64).Append(v.Field3)
	c.ColTuple[3].(*proto.ColArr[model.ValuesArrTuple]).Append(v.ValueArrTuple)
}

func (c ColTupleTreeAdapter) Row(i int) model.TreeRootStructure {

	return model.TreeRootStructure{
		Field1:        c.ColTuple[0].(*proto.ColUInt64).Row(i),
		Field2:        c.ColTuple[1].(*proto.ColUInt64).Row(i),
		Field3:        c.ColTuple[2].(*proto.ColUInt64).Row(i),
		ValueArrTuple: c.ColTuple[3].(*proto.ColArr[model.ValuesArrTuple]).Row(i),
	}

}

func (c ColTupleFunctionAdapter) Append(v model.Function) {
	c.ColTuple[0].(*proto.ColUInt64).Append(v.ValueInt64)
	c.ColTuple[1].(*proto.ColStr).Append(v.ValueStr)

}

func (c ColTupleFunctionAdapter) Row(i int) model.Function {
	return model.Function{ValueInt64: c.ColTuple[0].(*proto.ColUInt64).Row(i),
		ValueStr: c.ColTuple[1].(*proto.ColStr).Row(i)}
}

func (c ColTupleStrInt64Int32Adapter) Append(v model.ValuesAgg) {

	c.ColTuple[0].(*proto.ColStr).Append(v.ValueStr)
	c.ColTuple[1].(*proto.ColInt64).Append(v.ValueInt64)
	c.ColTuple[2].(*proto.ColInt32).Append(v.ValueInt32)

}

func (c ColTupleStrInt64Int32Adapter) Row(i int) model.ValuesAgg {
	return model.ValuesAgg{ValueStr: c.ColTuple[0].(*proto.ColStr).Row(i),
		ValueInt64: c.ColTuple[1].(*proto.ColInt64).Row(i),
		ValueInt32: c.ColTuple[2].(*proto.ColInt32).Row(i)}
}

func (c ColTupleStrStrAdapter) Append(v model.StrStr) {
	c.ColTuple[0].(*proto.ColStr).Append(v.Str1)
	c.ColTuple[1].(*proto.ColStr).Append(v.Str2)
}

func (c ColTupleStrStrAdapter) Row(i int) model.StrStr {

	return model.StrStr{Str1: c.ColTuple[0].(*proto.ColStr).Row(i),
		Str2: c.ColTuple[1].(*proto.ColStr).Row(i)}
}
