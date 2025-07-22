package clickhouse_transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

/*
const attrCondition = require('./attr_condition')
const {bitSet} = require('./shared')
const Sql = require('@cloki/clickhouse-sql')
module.exports = class Builder extends attrCondition {
  build () {
    const self = this
    const superBuild = super.build()
const res = (ctx) => {
const sel = superBuild(ctx)
sel.having_conditions = []
sel.aggregations = [bitSet(self.sqlConditions)]
sel.select_list = [[new Sql.Raw('count()'), 'count']]
sel.order_expressions = []
return sel
}
return res
}
}
*/

type AttrConditionEvaluatorPlanner struct {
	Main   *AttrConditionPlanner
	Prefix string
}

func (a *AttrConditionEvaluatorPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := a.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	main.SetHaving(nil).
		GroupBy(&bitSet{a.Main.sqlConds}, sql.NewRawObject("prefix")).
		OrderBy().
		Select(
			sql.NewCol(sql.NewStringVal(a.Prefix), "prefix"),
			sql.NewSimpleCol("count()", "_count"))

	return main, nil
}
