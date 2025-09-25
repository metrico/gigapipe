package planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"strings"
)

type AggPlanner struct {
	Main   shared.SQLRequestPlanner
	Labels []string
	By     bool
	Fn     string
}

func (s *AggPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	var withFp *sql.With
	for _, w := range main.GetWith() {
		if w.GetAlias() == "fp" {
			withFp = w
			break
		}
	}
	if withFp == nil {
		return nil, fmt.Errorf("could not find fingerprint subquery")
	}

	labels := s.getLabels(withFp, ctx)
	withLabels := sql.NewWith(labels, "labels")

	withMain := sql.NewWith(main, "pre_agg")

	patchVal, err := s.patchVal()
	if err != nil {
		return nil, err
	}

	values := sql.NewSelect().
		Select(
			sql.NewSimpleCol("1", "type"),
			sql.NewSimpleCol("labels.new_fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ms", "timestamp_ms"),
			sql.NewCol(patchVal, "val"),
			sql.NewSimpleCol("''", "labels")).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin(
			"any left",
			sql.NewWithRef(withLabels),
			sql.Eq(sql.NewRawObject("pre_agg.fingerprint"), sql.NewRawObject("labels.old_fingerprint")))).
		GroupBy(sql.NewRawObject("labels.new_fingerprint"), sql.NewRawObject("timestamp_ms")).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("labels.new_fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ms"), sql.ORDER_BY_DIRECTION_ASC))

	labelsReq := sql.NewSelect().
		Select(
			sql.NewSimpleCol("2", "type"),
			sql.NewSimpleCol("new_fingerprint", "fingerprint"),
			sql.NewSimpleCol("0", "timestamp_ms"),
			sql.NewSimpleCol("toFloat64(0)", "val"),
			sql.NewSimpleCol("new_labels", "labels")).
		From(sql.NewWithRef(withLabels))

	res := sql.NewSelect().With(withLabels, withMain).
		Select(sql.NewRawObject("*")).
		From(&unionAll{
			ISelect: values,
			unions:  []sql.ISelect{labelsReq},
		})
	return res, nil

}

func (s *AggPlanner) getLabels(withFp *sql.With, ctx *shared.PlannerContext) sql.ISelect {
	res := sql.NewSelect().Select(
		sql.NewSimpleCol("fingerprint", "old_fingerprint"),
		sql.NewSimpleCol("labels", "old_labels"),
		sql.NewCol(s.patchLabels(), "new_labels"),
		sql.NewSimpleCol("cityHash64(new_labels)", "new_fingerprint")).
		From(sql.NewRawObject(ctx.TimeSeriesTableName)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(
				clickhouse_planner.FormatFromDate(ctx.From))),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFp)))
	return res
}

func (s *AggPlanner) patchLabels() sql.SQLObject {
	sqlLabels := make([]sql.SQLObject, len(s.Labels))
	for i, label := range s.Labels {
		sqlLabels[i] = sql.NewStringVal(label)
	}

	return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		in := &notIn{In: sql.NewIn(sql.NewRawObject("x.1"), sqlLabels...), not: !s.By}
		sqlIn, err := in.String(ctx, options...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("toJSONString("+
			"mapFromArrays("+
			"arrayMap(x -> x.1,arrayFilter(x -> %s, JSONExtractKeysAndValues(labels, 'String')) as a), "+
			"arrayMap(x -> x.2, a)))", sqlIn), nil
	})
}

func (s *AggPlanner) patchVal() (sql.SQLObject, error) {
	switch s.Fn {
	case "sum":
		return sql.NewRawObject("sum(val)"), nil
	}
	return nil, fmt.Errorf("unknown function: %s", s.Fn)
}

type notIn struct {
	*sql.In
	not bool
}

func (n *notIn) String(ctx *sql.Ctx, options ...int) (string, error) {
	if !n.not {
		return n.In.String(ctx, options...)
	}
	left, err := n.In.GetEntity()[0].String(ctx, options...)
	if err != nil {
		return "", err
	}
	strRight := make([]string, len(n.In.GetEntity())-1)
	for i, v := range n.In.GetEntity()[1:] {
		strRight[i], err = v.String(ctx, options...)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%s NOT IN (%s)", left, strings.Join(strRight, ",")), nil
}
