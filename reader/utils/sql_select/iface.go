package sql_select

const (
	STRING_OPT_SKIP_WITH    = 1
	STRING_OPT_INLINE_WITH  = 2
	ORDER_BY_DIRECTION_ASC  = 3
	ORDER_BY_DIRECTION_DESC = 4
	WITH_REF_NO_ALIAS       = 5
)

type SQLObject interface {
	String(ctx *Ctx, options ...int) (string, error)
}

type SQLCondition interface {
	GetFunction() string
	GetEntity() []SQLObject
	String(ctx *Ctx, options ...int) (string, error)
}

type Ctx struct {
	id     int
	Params map[string]SQLObject
	Result map[string]SQLObject
}

func (c *Ctx) Id() int {
	c.id++
	return c.id
}

func DefaultCtx() *Ctx {
	return &Ctx{
		Params: make(map[string]SQLObject),
		Result: make(map[string]SQLObject),
	}
}

type ISelect interface {
	Distinct(distinct bool) ISelect
	GetDistinct() bool
	Select(cols ...SQLObject) ISelect
	GetSelect() []SQLObject
	From(table SQLObject) ISelect
	GetFrom() SQLObject
	AndWhere(clauses ...SQLCondition) ISelect
	OrWhere(clauses ...SQLCondition) ISelect
	GetWhere() SQLCondition
	AndPreWhere(clauses ...SQLCondition) ISelect
	OrPreWhere(clauses ...SQLCondition) ISelect
	GetPreWhere() SQLCondition
	AndHaving(clauses ...SQLCondition) ISelect
	OrHaving(clauses ...SQLCondition) ISelect
	GetHaving() SQLCondition
	SetHaving(having SQLCondition) ISelect
	GroupBy(fields ...SQLObject) ISelect
	GetGroupBy() []SQLObject
	OrderBy(fields ...SQLObject) ISelect
	GetOrderBy() []SQLObject
	Limit(limit SQLObject) ISelect
	GetLimit() SQLObject
	Offset(offset SQLObject) ISelect
	GetOffset() SQLObject
	With(withs ...*With) ISelect
	AddWith(withs ...*With) ISelect
	DropWith(alias ...string) ISelect
	GetWith() []*With
	Join(joins ...*Join) ISelect
	AddJoin(joins ...*Join) ISelect
	GetJoin() []*Join
	String(ctx *Ctx, options ...int) (string, error)
	SetSetting(name string, value string) ISelect
	GetSettings(table SQLObject) map[string]string
	AddWindows(windows ...*WindowFunction) ISelect
	GetWindows() []*WindowFunction
	SetWindows(windows ...*WindowFunction) ISelect
}

type Aliased interface {
	GetExpr() SQLObject
	GetAlias() string
	String(ctx *Ctx, options ...int) (string, error)
}
