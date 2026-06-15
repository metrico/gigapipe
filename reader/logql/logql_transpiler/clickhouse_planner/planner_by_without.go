package clickhouse_planner

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// canonicalFPCol is a SQL expression that computes a fingerprint from a labels Map
// expression using the same formula as the Go fingerprint() function in hash.go:
// keys are sorted (ClickHouse Maps are key-sorted), joined as "k=v,..." and
// hashed with CityHash64.
type canonicalFPCol struct {
	inner sql.SQLObject
}

func (c *canonicalFPCol) String(ctx *sql.Ctx, opts ...int) (string, error) {
	inner, err := c.inner.String(ctx, opts...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"cityHash64(arrayStringConcat(arrayMap((k,v)->concat(k,'=',v),mapKeys(%s),mapValues(%s)),','))",
		inner, inner,
	), nil
}

type ByWithoutPlanner struct {
	NoStreamSelect     bool
	Main               shared.SQLRequestPlanner
	Labels             []string
	By                 bool
	UseTimeSeriesTable bool
	LabelsCache        **sql.With
	FPCache            **sql.With
}

func (b *ByWithoutPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := b.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if b.UseTimeSeriesTable {
		return b.processTSTable(ctx, main)
	}
	return b.processSimple(ctx, main)
}

func (b *ByWithoutPlanner) processSimple(ctx *shared.PlannerContext,
	main sql.ISelect) (sql.ISelect, error) {
	withMain := sql.NewWith(main, fmt.Sprintf("pre_by_without_%d", ctx.Id()))
	filteredLabels := &byWithoutFilterCol{
		labelsCol: sql.NewRawObject(withMain.GetAlias() + ".labels"),
		labels:    b.Labels,
		by:        b.By,
	}
	return sql.NewSelect().With(withMain).
		Select(
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns"),
			sql.NewCol(&canonicalFPCol{inner: filteredLabels}, "fingerprint"),
			sql.NewCol(filteredLabels, "labels"),
			sql.NewSimpleCol("string", "string"),
			sql.NewSimpleCol("value", "value")).
		From(sql.NewWithRef(withMain)), nil
}

func (b *ByWithoutPlanner) processTSTable(ctx *shared.PlannerContext,
	main sql.ISelect) (sql.ISelect, error) {
	var labels sql.ISelect
	if b.LabelsCache != nil && *b.LabelsCache != nil {
		filteredLabels := &byWithoutFilterCol{
			labelsCol: sql.NewRawObject("a.labels"),
			labels:    b.Labels,
			by:        b.By,
		}
		labels = sql.NewSelect().Select(
			sql.NewRawObject("fingerprint"),
			sql.NewCol(&canonicalFPCol{inner: filteredLabels}, "new_fingerprint"),
			sql.NewCol(filteredLabels, "labels"),
		).From(sql.NewCol(sql.NewWithRef(*b.LabelsCache), "a"))
	} else {
		var fpCache *sql.With
		if !b.NoStreamSelect {
			fpCache = *b.FPCache
		}
		from, err := labelsFromScratch(ctx, fpCache)
		if err != nil {
			return nil, err
		}
		var filteredObj sql.SQLObject
		cols, err := patchCol(from.GetSelect(), "labels", func(object sql.SQLObject) (sql.SQLObject, error) {
			fo := &byWithoutFilterCol{
				labelsCol: object,
				labels:    b.Labels,
				by:        b.By,
			}
			filteredObj = fo
			return fo, nil
		})
		if err != nil {
			return nil, err
		}
		labels = from.Select(append(cols, sql.NewCol(&canonicalFPCol{inner: filteredObj}, "new_fingerprint"))...)
	}

	withLabels := sql.NewWith(labels, fmt.Sprintf("labels_%d", ctx.Id()))

	if b.LabelsCache != nil {
		*b.LabelsCache = withLabels
	}

	withMain := sql.NewWith(main, fmt.Sprintf("pre_without_%d", ctx.Id()))

	joinType := "ANY LEFT "
	if ctx.IsCluster {
		joinType = "GLOBAL ANY LEFT "
	}

	return sql.NewSelect().With(withMain, withLabels).
		Select(
			sql.NewSimpleCol(withLabels.GetAlias()+".new_fingerprint", "fingerprint"),
			sql.NewSimpleCol(withMain.GetAlias()+".timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol(withMain.GetAlias()+".value", "value"),
			sql.NewSimpleCol("''", "string"),
			sql.NewSimpleCol(withLabels.GetAlias()+".labels", "labels"),
		).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin(joinType, sql.NewWithRef(withLabels),
			sql.Eq(
				sql.NewRawObject(withMain.GetAlias()+".fingerprint"),
				sql.NewRawObject(withLabels.GetAlias()+".fingerprint"),
			))), nil
}

type byWithoutFilterCol struct {
	labelsCol sql.SQLObject
	labels    []string
	by        bool
}

func (b *byWithoutFilterCol) String(ctx *sql.Ctx, opts ...int) (string, error) {
	if len(b.labels) == 0 {
		return b.emptyLabels(ctx, opts...)
	}
	str, err := b.labelsCol.String(ctx, opts...)
	if err != nil {
		return "", err
	}

	sqlLabels := make([]string, len(b.labels))
	for i, label := range b.labels {
		sqlLabels[i], err = sql.NewStringVal(label).String(ctx, opts...)
		if err != nil {
			return "", err
		}
	}

	fn := "IN"
	if !b.by {
		fn = "NOT IN"
	}

	return fmt.Sprintf("mapFilter((k,v) -> k %s (%s), %s)", fn, strings.Join(sqlLabels, ","), str), nil
}

func (b *byWithoutFilterCol) emptyLabels(ctx *sql.Ctx, opts ...int) (string, error) {
	if b.by {
		return "map()", nil
	}
	str, err := b.labelsCol.String(ctx, opts...)
	return str, err
}
