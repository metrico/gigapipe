package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// BinaryExprSQLPlanner implements binary arithmetic between two metric sub-queries
// entirely in SQL via UNION ALL + GROUP BY with conditional aggregation.
//
// For a scalar right-hand side (RightScalar != "") the value column of the left
// query is patched in-place — no UNION ALL is needed.
type BinaryExprSQLPlanner struct {
	Left        shared.SQLRequestPlanner
	Op          string
	Right       shared.SQLRequestPlanner // nil when RightScalar is set
	RightScalar string                   // non-empty for e.g. "* 100"
}

func (p *BinaryExprSQLPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	leftSel, err := p.Left.Process(ctx)
	if err != nil {
		return nil, err
	}

	if p.RightScalar != "" {
		return p.processScalar(leftSel)
	}

	rightSel, err := p.Right.Process(ctx)
	if err != nil {
		return nil, err
	}
	return p.processBinary(leftSel, rightSel)
}

// processScalar patches the value column of the left query with the scalar operand.
func (p *BinaryExprSQLPlanner) processScalar(leftSel sql.ISelect) (sql.ISelect, error) {
	cols, err := patchCol(leftSel.GetSelect(), "value", func(expr sql.SQLObject) (sql.SQLObject, error) {
		exprStr, err := expr.String(sql.DefaultCtx())
		if err != nil {
			return nil, err
		}
		return sql.NewRawObject(fmt.Sprintf("(%s) %s %s", exprStr, p.Op, p.RightScalar)), nil
	})
	if err != nil {
		return nil, err
	}
	return leftSel.Select(cols...), nil
}

// processBinary builds:
//
//	SELECT fingerprint,
//	       anyIf(labels, side = 1)                          AS labels,
//	       sumIf(value, side = 1) <op> sumIf(value, side = 2) AS value,
//	       timestamp_ns
//	FROM (
//	    <left query with  1 as side>
//	    UNION ALL
//	    <right query with 2 as side>
//	) AS binary_op
//	GROUP BY fingerprint, timestamp_ns
//	ORDER BY fingerprint ASC, timestamp_ns ASC
func (p *BinaryExprSQLPlanner) processBinary(leftSel, rightSel sql.ISelect) (sql.ISelect, error) {
	addSide := func(sel sql.ISelect, side int) sql.ISelect {
		existing := sel.GetSelect()
		cols := make([]sql.SQLObject, len(existing)+1)
		copy(cols, existing)
		cols[len(existing)] = sql.FmtRawObject("%d as side", side)
		return sel.Select(cols...)
	}

	union := &UnionAll{
		ISelect:  addSide(leftSel, 1),
		Anothers: []sql.ISelect{addSide(rightSel, 2)},
	}

	valueExpr, err := p.arithmeticExpr()
	if err != nil {
		return nil, err
	}

	// Serialize the UNION ALL inline (no WITH hoisting) and use as a subquery
	// in FROM to avoid CTE alias conflicts between left and right.
	unionSubquery := sql.NewCustomCol(func(ctx *sql.Ctx, opts ...int) (string, error) {
		str, err := union.String(ctx, sql.STRING_OPT_INLINE_WITH)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s)", str), nil
	})

	return sql.NewSelect().
		Select(
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol("anyIf(labels, side = 1)", "labels"),
			sql.NewSimpleCol(valueExpr, "value"),
			sql.NewRawObject("timestamp_ns"),
		).
		From(sql.NewCol(unionSubquery, "binary_op")).
		GroupBy(
			sql.NewRawObject("fingerprint"),
			sql.NewRawObject("timestamp_ns"),
		).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_ASC),
		), nil
}

func (p *BinaryExprSQLPlanner) arithmeticExpr() (string, error) {
	left := "argMinIf(value, timestamp_ns, side = 1)"
	right := "argMinIf(value, timestamp_ns, side = 2)"
	switch p.Op {
	case "/":
		return left + " / " + right, nil
	case "*":
		return left + " * " + right, nil
	case "+":
		return left + " + " + right, nil
	case "-":
		return left + " - " + right, nil
	case "%":
		return left + " % " + right, nil
	}
	return "", fmt.Errorf("unsupported binary operator: %q", p.Op)
}
