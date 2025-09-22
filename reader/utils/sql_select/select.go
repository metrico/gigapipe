package sql

import (
	"fmt"
	"strings"
)

type Select struct {
	distinct bool
	columns  []SQLObject
	from     SQLObject
	where    SQLCondition
	preWhere SQLCondition
	having   SQLCondition
	groupBy  []SQLObject
	orderBy  []SQLObject
	limit    SQLObject
	offset   SQLObject
	withs    []*With
	joins    []*Join
	windows  []*WindowFunction
	settings map[string]string
}

func (s *Select) Distinct(distinct bool) ISelect {
	s.distinct = distinct
	return s
}

func (s *Select) GetDistinct() bool {
	return s.distinct
}

func (s *Select) Select(cols ...SQLObject) ISelect {
	s.columns = cols
	return s
}

func (s *Select) GetSelect() []SQLObject {
	return s.columns
}

func (s *Select) From(table SQLObject) ISelect {
	s.from = table
	return s
}

func (s *Select) SetSetting(name string, value string) ISelect {
	if s.settings == nil {
		s.settings = make(map[string]string)
	}
	s.settings[name] = value
	return s
}

func (s *Select) GetSettings(table SQLObject) map[string]string {
	return s.settings
}

func (s *Select) GetFrom() SQLObject {
	return s.from
}

func (s *Select) AndWhere(clauses ...SQLCondition) ISelect {
	if s.where == nil {
		s.where = And(clauses...)
		return s
	}
	if _, ok := s.where.(*LogicalOp); ok && s.where.GetFunction() == "and" {
		s.where.(*LogicalOp).AppendEntity(clauses...)
		return s
	}
	_clauses := make([]SQLCondition, len(clauses)+1)
	_clauses[0] = s.where
	for i, v := range clauses {
		_clauses[i+1] = v
	}
	s.where = And(_clauses...)
	return s
}

func (s *Select) OrWhere(clauses ...SQLCondition) ISelect {
	if s.where == nil {
		s.where = Or(clauses...)
		return s
	}
	if _, ok := s.where.(*LogicalOp); ok && s.where.GetFunction() == "or" {
		s.where.(*LogicalOp).AppendEntity(clauses...)
		return s
	}
	_clauses := make([]SQLCondition, len(clauses)+1)
	_clauses[0] = s.where
	for i, v := range clauses {
		_clauses[i+1] = v
	}
	s.where = Or(_clauses...)
	return s
}

func (s *Select) GetPreWhere() SQLCondition {
	return s.preWhere
}

func (s *Select) AndPreWhere(clauses ...SQLCondition) ISelect {
	if s.preWhere == nil {
		s.preWhere = And(clauses...)
		return s
	}
	if _, ok := s.preWhere.(*LogicalOp); ok && s.preWhere.GetFunction() == "and" {
		s.preWhere.(*LogicalOp).AppendEntity(clauses...)
		return s
	}
	_clauses := make([]SQLCondition, len(clauses))
	_clauses[0] = s.preWhere
	for i, v := range clauses {
		_clauses[i+1] = v
	}
	return s
}

func (s *Select) OrPreWhere(clauses ...SQLCondition) ISelect {
	if s.preWhere == nil {
		s.preWhere = Or(clauses...)
		return s
	}
	if _, ok := s.preWhere.(*LogicalOp); ok && s.preWhere.GetFunction() == "or" {
		s.preWhere.(*LogicalOp).AppendEntity(clauses...)
		return s
	}
	_clauses := make([]SQLCondition, len(clauses)+1)
	_clauses[0] = s.preWhere
	for i, v := range clauses {
		_clauses[i+1] = v
	}
	return s
}

func (s *Select) GetWhere() SQLCondition {
	return s.where
}

func (s *Select) AndHaving(clauses ...SQLCondition) ISelect {
	if s.having == nil {
		s.having = And(clauses...)
		return s
	}
	if _, ok := s.having.(*LogicalOp); ok && s.having.GetFunction() == "and" {
		s.having.(*LogicalOp).AppendEntity(clauses...)
		return s
	}
	_clauses := make([]SQLCondition, len(clauses)+1)
	_clauses[0] = s.having
	for i, v := range clauses {
		_clauses[i+1] = v
	}
	s.having = And(_clauses...)
	return s
}

func (s *Select) OrHaving(clauses ...SQLCondition) ISelect {
	if s.having == nil {
		s.having = Or(clauses...)
		return s
	}
	if _, ok := s.having.(*LogicalOp); ok && s.where.GetFunction() == "or" {
		s.having.(*LogicalOp).AppendEntity(clauses...)
		return s
	}
	_clauses := make([]SQLCondition, len(clauses)+1)
	_clauses[0] = s.having
	for i, v := range clauses {
		_clauses[i+1] = v
	}
	s.having = Or(_clauses...)
	return s
}

func (s *Select) GetHaving() SQLCondition {
	return s.having
}

func (s *Select) SetHaving(having SQLCondition) ISelect {
	s.having = having
	return s
}

func (s *Select) GroupBy(fields ...SQLObject) ISelect {
	s.groupBy = fields
	return s
}

func (s *Select) GetGroupBy() []SQLObject {
	return s.groupBy
}

func (s *Select) OrderBy(fields ...SQLObject) ISelect {
	s.orderBy = fields
	return s
}

func (s *Select) GetOrderBy() []SQLObject {
	return s.orderBy
}

func (s *Select) Limit(limit SQLObject) ISelect {
	s.limit = limit
	return s
}

func (s *Select) GetLimit() SQLObject {
	return s.limit
}

func (s *Select) Offset(offset SQLObject) ISelect {
	s.offset = offset
	return s
}

func (s *Select) GetOffset() SQLObject {
	return s.offset
}

func (s *Select) With(withs ...*With) ISelect {
	s.withs = []*With{}
	s.AddWith(withs...)
	return s
}

func (s *Select) AddWith(withs ...*With) ISelect {
	if s.withs == nil {
		return s.With(withs...)
	}
	for _, w := range withs {
		exists := false
		for _, with := range s.withs {
			if with.alias == w.alias {
				exists = true
			}
		}
		if exists {
			continue
		}

		if _, ok := w.GetQuery().(ISelect); ok {
			s.AddWith(w.GetQuery().(ISelect).GetWith()...)
		}
		s.withs = append(s.withs, w)
	}
	return s
}

func (s *Select) DropWith(alias ...string) ISelect {
	aliases := map[string]bool{}
	for _, a := range alias {
		aliases[a] = true
	}
	withs := make([]*With, 0, len(s.withs))
	for _, w := range s.withs {
		if aliases[w.alias] {
			continue
		}
		withs = append(withs, w)
	}
	s.withs = withs
	return s
}

func (s *Select) GetWith() []*With {
	res := make([]*With, 0, len(s.withs))
	for _, w := range s.withs {
		res = append(res, w)
	}
	return res
}

func (s *Select) Join(joins ...*Join) ISelect {
	s.joins = joins
	return s
}

func (s *Select) AddJoin(joins ...*Join) ISelect {
	for _, lj := range joins {
		s.joins = append(s.joins, lj)
	}
	return s
}

func (s *Select) GetJoin() []*Join {
	return s.joins
}

func (s *Select) AddWindows(windows ...*WindowFunction) ISelect {
	s.windows = append(s.windows, windows...)
	return s
}

func (s *Select) GetWindows() []*WindowFunction {
	return s.windows
}

func (s *Select) SetWindows(windows ...*WindowFunction) ISelect {
	s.windows = windows
	return s
}

func (s *Select) String(ctx *Ctx, options ...int) (string, error) {
	res := strings.Builder{}
	renderer := selectRenderer{
		ctx:     ctx,
		options: options,
		s:       s,
		res:     &res,
	}

	funcs := []func() error{
		renderer.with,
		renderer.sel,
		renderer.from,
		renderer.prewhere,
		renderer.where,
		renderer.groupBy,
		renderer.having,
		renderer.window,
		renderer.orderBy,
		renderer.limit,
		renderer.offset,
		renderer.settings,
	}

	for _, f := range funcs {
		if err := f(); err != nil {
			return "", err
		}
	}
	return res.String(), nil
}

func NewSelect() ISelect {
	return &Select{}
}

type selectRenderer struct {
	ctx     *Ctx
	options []int
	s       *Select
	res     *strings.Builder
}

func (r *selectRenderer) with() error {
	skipWith := false
	for _, i := range r.options {
		skipWith = skipWith || i == STRING_OPT_SKIP_WITH || i == STRING_OPT_INLINE_WITH
	}

	if skipWith || len(r.s.withs) == 0 {
		return nil
	}
	r.res.WriteString("WITH ")
	i := 0
	_options := append(r.options, STRING_OPT_SKIP_WITH)
	for _, w := range r.s.withs {
		if i != 0 {
			r.res.WriteRune(',')
		}
		str, err := w.String(r.ctx, _options...)
		if err != nil {
			return err
		}
		r.res.WriteString(str)
		i++
	}
	return nil
}

func (r *selectRenderer) sel() error {
	r.res.WriteString(" SELECT ")
	if r.s.distinct {
		r.res.WriteString(" DISTINCT ")
	}
	if r.s.columns == nil || len(r.s.columns) == 0 {
		return fmt.Errorf("no 'SELECT' part")
	}
	for i, col := range r.s.columns {
		if i != 0 {
			r.res.WriteString(", ")
		}
		str, err := col.String(r.ctx, r.options...)
		if err != nil {
			return err
		}
		r.res.WriteString(str)
	}
	return nil
}

func (r *selectRenderer) from() error {
	if r.s.from == nil {
		return nil
	}
	r.res.WriteString(" FROM ")
	str, err := r.s.from.String(r.ctx, r.options...)
	if err != nil {
		return err
	}
	r.res.WriteString(str)
	for _, lj := range r.s.joins {
		r.res.WriteString(fmt.Sprintf(" %s JOIN ", lj.tp))
		str, err = lj.String(r.ctx, r.options...)
		if err != nil {
			return err
		}
		r.res.WriteString(str)
	}
	return nil
}

func (r *selectRenderer) prewhere() error {
	if r.s.preWhere == nil {
		return nil
	}
	r.res.WriteString(" PREWHERE ")
	str, err := r.s.preWhere.String(r.ctx, r.options...)
	if err != nil {
		return err
	}
	r.res.WriteString(str)
	return nil
}

func (r *selectRenderer) where() error {
	if r.s.where == nil {
		return nil
	}
	r.res.WriteString(" WHERE ")
	str, err := r.s.where.String(r.ctx, r.options...)
	if err != nil {
		return err
	}
	r.res.WriteString(str)
	return nil
}

func (r *selectRenderer) groupBy() error {
	if len(r.s.groupBy) == 0 {
		return nil
	}
	r.res.WriteString(" GROUP BY ")
	for i, f := range r.s.groupBy {
		if i != 0 {
			r.res.WriteString(", ")
		}
		str, err := f.String(r.ctx, r.options...)
		if err != nil {
			return err
		}
		r.res.WriteString(str)
	}
	return nil
}

func (r *selectRenderer) having() error {
	if r.s.having == nil {
		return nil
	}
	r.res.WriteString(" HAVING ")
	str, err := r.s.having.String(r.ctx, r.options...)
	if err != nil {
		return err
	}
	r.res.WriteString(str)
	return nil
}

func (r *selectRenderer) window() error {
	if len(r.s.windows) == 0 {
		return nil
	}
	windows := make([]string, len(r.s.windows))
	var err error
	i := 0
	for _, w := range r.s.windows {
		if w.Alias == "" {
			continue
		}
		windows[i], err = w.String(r.ctx, r.options...)
		if err != nil {
			return err
		}
		i++
	}
	if i > 0 {
		r.res.WriteString(fmt.Sprintf(" WINDOW %s", strings.Join(windows[:i], ", ")))
	}
	return nil
}

func (r *selectRenderer) orderBy() error {
	if len(r.s.orderBy) == 0 {
		return nil
	}
	r.res.WriteString(" ORDER BY ")
	for i, f := range r.s.orderBy {
		if i != 0 {
			r.res.WriteString(", ")
		}
		str, err := f.String(r.ctx, r.options...)
		if err != nil {
			return err
		}
		r.res.WriteString(str)
	}
	return nil
}

func (r *selectRenderer) limit() error {
	if r.s.limit == nil {
		return nil
	}
	str, err := r.s.limit.String(r.ctx, r.options...)
	if err != nil {
		return err
	}
	if str != "" {
		r.res.WriteString(" LIMIT ")
		r.res.WriteString(str)
	}
	return nil
}

func (r *selectRenderer) offset() error {
	if r.s.offset == nil {
		return nil
	}
	str, err := r.s.offset.String(r.ctx, r.options...)
	if err != nil {
		return err
	}
	if str != "" {
		r.res.WriteString(" OFFSET ")
		r.res.WriteString(str)
	}
	return nil
}

func (r *selectRenderer) settings() error {
	if r.s.settings == nil {
		return nil
	}
	r.res.WriteString(" SETTINGS ")
	for k, v := range r.s.settings {
		r.res.WriteString(k)
		r.res.WriteString("=")
		r.res.WriteString(v)
		r.res.WriteString(" ")
	}
	return nil
}
