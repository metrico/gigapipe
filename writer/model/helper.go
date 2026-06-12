package model

type StrStr struct {
	Str1 string
	Str2 string
}

func (s StrStr) HttpValues() any {
	return []string{s.Str1, s.Str2}
}

type ValuesAgg struct {
	ValueStr   string
	ValueInt64 int64
	ValueInt32 int32
}

func (s ValuesAgg) HttpValues() any {
	return []any{s.ValueStr, s.ValueInt64, s.ValueInt32}
}

type ValuesArrTuple struct {
	ValueStr         string
	FirstValueInt64  int64
	SecondValueInt64 int64
}

func (s ValuesArrTuple) HttpValues() any {
	return []any{s.ValueStr, s.FirstValueInt64, s.SecondValueInt64}
}

type TreeRootStructure struct {
	Field1        uint64
	Field2        uint64
	Field3        uint64
	ValueArrTuple []ValuesArrTuple
}

func (s TreeRootStructure) HttpValues() any {
	res := make([]any, len(s.ValueArrTuple))
	for i, v := range s.ValueArrTuple {
		res[i] = v.HttpValues()
	}
	return []any{s.Field1, s.Field2, s.Field3, res}
}

type Function struct {
	ValueInt64 uint64
	ValueStr   string
}

func (s Function) HttpValues() any {
	return []any{s.ValueInt64, s.ValueStr}
}
