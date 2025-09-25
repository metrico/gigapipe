package planner

import sql "github.com/metrico/qryn/reader/utils/sql_select"

func patchField(query sql.ISelect, alias string, newField sql.Aliased) sql.ISelect {
	_select := make([]sql.SQLObject, len(query.GetSelect()))
	for i, f := range query.GetSelect() {
		if f.(sql.Aliased).GetAlias() != alias {
			_select[i] = f
			continue
		}
		_select[i] = newField
	}
	query.Select(_select...)
	return query
}
