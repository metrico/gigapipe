package sql_select

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func render(t *testing.T, s SQLObject, options ...int) string {
	t.Helper()
	str, err := s.String(DefaultCtx(), options...)
	require.NoError(t, err)
	return str
}

func jumpWith() *With {
	return NewRecursiveWith(
		NewSelect().Select(NewRawObject("span_id"), NewRawObject("parent_id")).From(NewRawObject("edges")),
		"jump",
		func(self *WithRef) ISelect {
			return NewSelect().Select(NewRawObject("span_id"), NewRawObject("parent_id")).From(self).
				AndWhere(Neq(NewRawObject("parent_id"), NewIntVal(0)))
		})
}

func TestSettingsRenderSortedAndCommaSeparated(t *testing.T) {
	s := NewSelect().Select(NewRawObject("1")).
		SetSetting("max_recursive_cte_evaluation_depth", "20").
		SetSetting("enable_analyzer", "1")
	require.Equal(t, " SELECT 1 SETTINGS enable_analyzer=1, max_recursive_cte_evaluation_depth=20", render(t, s))
}

func TestNoSettingsRendersNoClause(t *testing.T) {
	require.Equal(t, " SELECT 1", render(t, NewSelect().Select(NewRawObject("1"))))
}

func TestRecursiveWith(t *testing.T) {
	w := jumpWith()
	require.True(t, w.IsRecursive())
	require.False(t, NewWith(NewSelect().Select(NewRawObject("1")), "x").IsRecursive())

	s := NewSelect().With(w).Select(NewRawObject("count()")).From(NewWithRef(w))
	require.Equal(t,
		"WITH RECURSIVE jump as ( SELECT span_id, parent_id FROM edges UNION ALL "+
			" SELECT span_id, parent_id FROM jump WHERE ((parent_id) != (0))) SELECT count() FROM jump",
		render(t, s))
}

func TestRecursiveKeywordCoversWholeWithClause(t *testing.T) {
	plain := NewWith(NewSelect().Select(NewRawObject("1")), "plain")
	s := NewSelect().With(plain, jumpWith()).Select(NewRawObject("1"))
	require.Regexp(t, `^WITH RECURSIVE plain as \(.*\),jump as \(`, render(t, s))
}

func TestRecursiveWithHoistsNestedWiths(t *testing.T) {
	edges := NewWith(NewSelect().Select(NewRawObject("span_id")).From(NewRawObject("traces")), "edges")
	extra := NewWith(NewSelect().Select(NewRawObject("1")), "extra")
	w := NewRecursiveWith(
		NewSelect().With(edges).Select(NewRawObject("span_id")).From(NewWithRef(edges)),
		"walk",
		func(self *WithRef) ISelect {
			return NewSelect().With(extra).Select(NewRawObject("span_id")).From(self)
		})
	outer := NewWith(NewSelect().With(w).Select(NewRawObject("span_id")).From(NewWithRef(w)), "outer")
	s := NewSelect().With(outer).Select(NewRawObject("1")).From(NewWithRef(outer))

	aliases := []string{}
	for _, with := range s.GetWith() {
		aliases = append(aliases, with.GetAlias())
	}
	require.Equal(t, []string{"edges", "extra", "walk", "outer"}, aliases)
	require.Regexp(t, `^WITH RECURSIVE edges as `, render(t, s))
}

func TestInliningRecursiveWithFails(t *testing.T) {
	w := jumpWith()
	_, err := NewWithRef(w).String(DefaultCtx(), STRING_OPT_INLINE_WITH)
	require.Error(t, err)
}
