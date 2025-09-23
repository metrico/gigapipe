package sql_select

import (
	"fmt"
	"strings"
)

type RawObject struct {
	val string
}

func (r *RawObject) String(ctx *Ctx, options ...int) (string, error) {
	return r.val, nil
}

func NewRawObject(val string) *RawObject {
	return &RawObject{
		val: val,
	}
}

func FmtRawObject(tmpl string, arg ...interface{}) *RawObject {
	return &RawObject{fmt.Sprintf(tmpl, arg...)}
}

type OrderBy struct {
	col       SQLObject
	direction int
}

func (o *OrderBy) String(ctx *Ctx, options ...int) (string, error) {
	order := "desc"
	if o.direction == ORDER_BY_DIRECTION_ASC {
		order = "asc"
	}
	str, err := o.col.String(ctx, options...)
	return fmt.Sprintf("%s %s", str, order), err
}

func NewOrderBy(col SQLObject, direction int) *OrderBy {
	return &OrderBy{
		col:       col,
		direction: direction,
	}
}

type With struct {
	query ISelect
	alias string
}

func (w *With) GetQuery() ISelect {
	return w.query
}

func (w *With) GetAlias() string {
	return w.alias
}

func (w *With) String(ctx *Ctx, options ...int) (string, error) {
	str, err := w.query.String(ctx, options...)
	return fmt.Sprintf("%s as (%s)", w.alias, str), err
}

func NewWith(query ISelect, alias string) *With {
	return &With{
		query: query,
		alias: alias,
	}
}

type WithRef struct {
	ref *With
}

func (w *WithRef) String(ctx *Ctx, options ...int) (string, error) {
	if w.ref.alias == "" {
		return "", fmt.Errorf("alias is empty")
	}
	inline := false
	noAlias := false
	var _opts []int
	for _, opt := range options {
		inline = inline || opt == STRING_OPT_INLINE_WITH
		noAlias = noAlias || opt == WITH_REF_NO_ALIAS
		if opt != WITH_REF_NO_ALIAS {
			_opts = append(_opts, opt)
		}
	}
	res := w.ref.alias
	if inline {
		str, err := w.ref.GetQuery().String(ctx, _opts...)
		if err != nil {
			return "", err
		}
		res = "(" + str + ")"
		if !noAlias {
			res += " as " + w.ref.alias
		}
	}
	return res, nil
}

func NewWithRef(ref *With) *WithRef {
	return &WithRef{ref: ref}
}

type Join struct {
	tp    string
	table SQLObject
	on    SQLCondition
}

func (l *Join) String(ctx *Ctx, options ...int) (string, error) {
	tbl, err := l.table.String(ctx, options...)
	if err != nil {
		return "", err
	}
	on := ""
	if strings.ToLower(l.tp) != "array" {
		_on, err := l.on.String(ctx, options...)
		if err != nil {
			return "", err
		}
		on += "ON " + _on
	}
	return fmt.Sprintf("%s %s", tbl, on), err
}

func (l *Join) GetTable() SQLObject {
	return l.table
}

func (l *Join) GetOn() SQLCondition {
	return l.on
}

func NewJoin(tp string, table SQLObject, on SQLCondition) *Join {
	return &Join{
		tp:    tp,
		table: table,
		on:    on,
	}
}

type CtxParam struct {
	name string
	def  *string
}

func (c *CtxParam) String(ctx *Ctx, options ...int) (string, error) {
	if _, ok := ctx.Params[c.name]; !ok {
		if c.def == nil {
			return "", fmt.Errorf("undefined parameter %s", c.name)
		}
		return *c.def, nil
	}
	return ctx.Params[c.name].String(ctx, options...)
}

func NewCtxParam(name string, def *string) *CtxParam {
	return &CtxParam{
		name: name,
		def:  def,
	}
}

func NewCtxParamOrDef(name string, def string) *CtxParam {
	return &CtxParam{
		name: name,
		def:  &def,
	}
}

type StringVal struct {
	val string
}

func (s *StringVal) String(ctx *Ctx, options ...int) (string, error) {
	find := []string{"\\", "\000", "\n", "\r", "\b", "\t", "\x1a", "'"}
	replace := []string{"\\\\", "\\0", "\\n", "\\r", "\\b", "\\t", "\\x1a", "\\'"}
	res := s.val
	for i, v := range find {
		res = strings.Replace(res, v, replace[i], -1)
	}
	return "'" + res + "'", nil
}

func NewStringVal(s string) SQLObject {
	return &StringVal{
		val: s,
	}
}

type IntVal struct {
	val int64
}

func (i *IntVal) String(ctx *Ctx, options ...int) (string, error) {
	return fmt.Sprintf("%d", i.val), nil
}

func NewIntVal(val int64) *IntVal {
	return &IntVal{
		val: val,
	}
}

type BoolVal struct {
	val bool
}

func (b *BoolVal) String(ctx *Ctx, options ...int) (string, error) {
	if b.val {
		return "true", nil
	}
	return "false", nil
}

func NewBoolVal(b bool) SQLObject {
	return &BoolVal{
		val: b,
	}
}

type FloatVal struct {
	val float64
}

func (f *FloatVal) String(ctx *Ctx, options ...int) (string, error) {
	return fmt.Sprintf("%f", f.val), nil
}

func NewFloatVal(f float64) SQLObject {
	return &FloatVal{
		val: f,
	}
}

type Col struct {
	expr  SQLObject
	alias string
}

func (c *Col) GetExpr() SQLObject {
	return c.expr
}

func (c *Col) GetAlias() string {
	return c.alias
}

func (c *Col) String(ctx *Ctx, options ...int) (string, error) {
	_opts := append(options, WITH_REF_NO_ALIAS)
	expr, err := c.expr.String(ctx, _opts...)
	if c.alias == "" {
		return expr, err
	}
	return fmt.Sprintf("%s as %s", expr, c.alias), err
}

func NewCol(expr SQLObject, alias string) SQLObject {
	return &Col{
		expr:  expr,
		alias: alias,
	}
}

func NewSimpleCol(name string, alias string) SQLObject {
	return &Col{
		expr:  NewRawObject(name),
		alias: alias,
	}
}

type In struct {
	leftSide  SQLObject
	rightSide []SQLObject
}

func (in *In) String(ctx *Ctx, options ...int) (string, error) {
	parts := make([]string, len(in.rightSide))
	for i, e := range in.rightSide {
		str, err := e.String(ctx, options...)
		if err != nil {
			return "", err
		}
		parts[i] = str
	}
	str, err := in.leftSide.String(ctx, options...)
	return fmt.Sprintf("%s IN (%s)", str, strings.Join(parts, ",")), err
}

func (in *In) GetFunction() string {
	return "IN"
}

func (in *In) GetEntity() []SQLObject {
	ent := make([]SQLObject, len(in.rightSide)+1)
	ent[0] = in.leftSide
	for i, r := range in.rightSide {
		ent[i+1] = r
	}
	return ent
}

func NewIn(left SQLObject, right ...SQLObject) *In {
	return &In{
		leftSide:  left,
		rightSide: right,
	}
}

type CustomCol struct {
	stringify func(ctx *Ctx, options ...int) (string, error)
}

func (c *CustomCol) String(ctx *Ctx, options ...int) (string, error) {
	return c.stringify(ctx, options...)
}

func NewCustomCol(fn func(ctx *Ctx, options ...int) (string, error)) SQLObject {
	return &CustomCol{stringify: fn}
}

type WindowPoint struct {
	Unbounded   bool
	Offset      int32
	IsFollowing bool
}

func (w *WindowPoint) String(ctx *Ctx, options ...int) (string, error) {
	if w.Offset == 0 {
		return "CURRENT ROW", nil
	}
	suffix := "PRECEDING"
	if w.IsFollowing {
		suffix = "FOLLOWING"
	}
	offset := "UNBOUBDED"
	if !w.Unbounded {
		offset = fmt.Sprintf("%d", w.Offset)
	}
	return fmt.Sprintf("%s %s", offset, suffix), nil
}

type WindowFunction struct {
	Alias       string
	PartitionBy []SQLObject
	OrderBy     []SQLObject
	Rows        bool
	Start       WindowPoint
	End         WindowPoint
}

func (w *WindowFunction) String(ctx *Ctx, options ...int) (string, error) {
	res := ""
	if w.Alias != "" {
		res += fmt.Sprintf("%s AS ", w.Alias)
	}
	partitionBy, err := w.stringify(w.PartitionBy, "PARTITION BY", ctx, options...)
	orderBy, err := w.stringify(w.OrderBy, "ORDER BY", ctx, options...)

	rows := "ROWS"
	if !w.Rows {
		rows = "RANGE"
	}
	start, err := w.Start.String(ctx, options...)
	if err != nil {
		return "", err
	}
	end, err := w.End.String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s(%s%s %s BETWEEN %s AND %s)", res, partitionBy, orderBy, rows, start, end), nil

}

func (w *WindowFunction) stringify(objs []SQLObject, prefix string, ctx *Ctx, options ...int) (string, error) {
	if len(objs) == 0 {
		return "", nil
	}
	partitions := make([]string, len(objs))
	var err error
	for i, p := range objs {
		partitions[i], err = p.String(ctx, options...)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%s %s ", prefix, strings.Join(partitions, ", ")), nil
}

type WindowFunctionRef struct {
	Fn *WindowFunction
}

func (w *WindowFunctionRef) String(ctx *Ctx, options ...int) (string, error) {
	if w.Fn.Alias == "" {
		return w.Fn.String(ctx, options...)
	}
	return fmt.Sprintf("%s", w.Fn.Alias), nil
}
