package sql_select

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

		s.AddWith(w.GetQuery().GetWith()...)

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
	res = append(res, s.withs...)
	return res
}

func (s *Select) Join(joins ...*Join) ISelect {
	s.joins = joins
	return s
}

func (s *Select) AddJoin(joins ...*Join) ISelect {
	s.joins = append(s.joins, joins...)
	return s
}

func (s *Select) GetJoin() []*Join {
	return s.joins
}

func (s *Select) String(ctx *Ctx, options ...int) (string, error) {
	res := strings.Builder{}
	skipWith := false
	for _, i := range options {
		skipWith = skipWith || i == STRING_OPT_SKIP_WITH || i == STRING_OPT_INLINE_WITH
	}
	if !skipWith && len(s.withs) > 0 {
		res.WriteString("WITH ")
		i := 0
		_options := append(options, STRING_OPT_SKIP_WITH)
		for _, w := range s.withs {
			if i != 0 {
				res.WriteRune(',')
			}
			str, err := w.String(ctx, _options...)
			if err != nil {
				return "", err
			}
			res.WriteString(str)
			i++
		}
	}
	res.WriteString(" SELECT ")
	if s.distinct {
		res.WriteString(" DISTINCT ")
	}
	if len(s.columns) == 0 {
		return "", fmt.Errorf("no 'SELECT' part")
	}
	for i, col := range s.columns {
		if i != 0 {
			res.WriteString(", ")
		}
		str, err := col.String(ctx, options...)
		if err != nil {
			return "", err
		}
		res.WriteString(str)
	}
	var (
		str string
		err error
	)
	if s.from != nil {
		res.WriteString(" FROM ")
		str, err = s.from.String(ctx, options...)
		if err != nil {
			return "", err
		}
		res.WriteString(str)
		for _, lj := range s.joins {
			res.WriteString(fmt.Sprintf(" %s JOIN ", lj.tp))
			str, err = lj.String(ctx, options...)
			if err != nil {
				return "", err
			}
			res.WriteString(str)
		}
	}
	if s.preWhere != nil {
		res.WriteString(" PREWHERE ")
		str, err = s.preWhere.String(ctx, options...)
		if err != nil {
			return "", err
		}
		res.WriteString(str)
	}
	if s.where != nil {
		res.WriteString(" WHERE ")
		str, err = s.where.String(ctx, options...)
		if err != nil {
			return "", err
		}
		res.WriteString(str)
	}
	if len(s.groupBy) > 0 {
		res.WriteString(" GROUP BY ")
		for i, f := range s.groupBy {
			if i != 0 {
				res.WriteString(", ")
			}
			str, err = f.String(ctx, options...)
			if err != nil {
				return "", err
			}
			res.WriteString(str)
		}
	}
	if s.having != nil {
		res.WriteString(" HAVING ")
		str, err = s.having.String(ctx, options...)
		if err != nil {
			return "", err
		}
		res.WriteString(str)
	}
	if len(s.orderBy) > 0 {
		res.WriteString(" ORDER BY ")
		for i, f := range s.orderBy {
			if i != 0 {
				res.WriteString(", ")
			}
			str, err = f.String(ctx, options...)
			if err != nil {
				return "", err
			}
			res.WriteString(str)
		}
	}
	if s.limit != nil {
		str, err = s.limit.String(ctx, options...)
		if err != nil {
			return "", err
		}
		if str != "" {
			res.WriteString(" LIMIT ")
			res.WriteString(str)
		}
	}
	if s.offset != nil {
		str, err = s.offset.String(ctx, options...)
		if err != nil {
			return "", err
		}
		if str != "" {
			res.WriteString(" OFFSET ")
			res.WriteString(str)
		}
	}
	if s.settings != nil {
		res.WriteString(" SETTINGS ")
		for k, v := range s.settings {
			res.WriteString(k)
			res.WriteString("=")
			res.WriteString(v)
			res.WriteString(" ")
		}
	}
	return res.String(), nil
}

func NewSelect() ISelect {
	return &Select{}
}
