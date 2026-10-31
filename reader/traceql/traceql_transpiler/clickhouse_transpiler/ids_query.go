package clickhouse_transpiler

import (
	stdsql "database/sql"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// IdsScan reads an ids statement's row: its trace id, its span ids and any
// further columns.
type IdsScan func(rows *stdsql.Rows) (traceId string, spanIds []string, err error)

// OnEmptyIds is what Query does when every ids table is empty.
type OnEmptyIds int

const (
	// RunOnEmptyIds runs the statement over the empty ids tables.
	RunOnEmptyIds OnEmptyIds = iota
	// SkipOnEmptyIds returns without running the statement.
	SkipOnEmptyIds
)

// Query runs the ids statements, reading their rows with idsScan, then
// statement over their tables, passing each row to scan. Without ids
// statements, statement runs alone.
func Query(ctx *shared.PlannerContext, ids []IdsStatement, idsScan IdsScan, statement shared.SQLRequestPlanner,
	onEmpty OnEmptyIds, scan func(*stdsql.Rows) error) error {
	tables, err := ReadIds(ctx, ids, idsScan)
	if err != nil {
		return err
	}
	if onEmpty == SkipOnEmptyIds && len(tables) > 0 && allEmpty(tables) {
		return nil
	}
	return QueryOver(ctx, statement, tables, scan)
}

// ReadIds runs each ids statement and returns its rows, read with scan or as
// plain idsColumns when scan is nil, as its external table.
func ReadIds(ctx *shared.PlannerContext, statements []IdsStatement, scan IdsScan) ([]*ext.Table, error) {
	if scan == nil {
		scan = scanIds
	}
	res := make([]*ext.Table, len(statements))
	for i, s := range statements {
		table, err := readIds(ctx, s, scan)
		if err != nil {
			return nil, unresolvedAncestryError(ctx, err)
		}
		res[i] = table
	}
	return res, nil
}

// QueryOver runs statement with ids attached as external tables, passing each
// row to scan.
func QueryOver(ctx *shared.PlannerContext, statement shared.SQLRequestPlanner, ids []*ext.Table,
	scan func(*stdsql.Rows) error) error {
	return unresolvedAncestryError(ctx, queryOver(ctx, statement, ids, scan))
}

func queryOver(ctx *shared.PlannerContext, statement shared.SQLRequestPlanner, ids []*ext.Table,
	scan func(*stdsql.Rows) error) error {
	str, err := render(ctx, statement)
	if err != nil {
		return err
	}
	queryCtx := ctx.Ctx
	if len(ids) > 0 {
		queryCtx = clickhouse.Context(queryCtx, clickhouse.WithExternalTable(ids...))
	}
	rows, err := ctx.CHDb.QueryCtx(queryCtx, str)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func readIds(ctx *shared.PlannerContext, s IdsStatement, scan IdsScan) (*ext.Table, error) {
	table, err := newIdsTable(s.Table)
	if err != nil {
		return nil, err
	}
	str, err := render(ctx, s.Statement)
	if err != nil {
		return nil, err
	}
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, str)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		traceId, spanIds, err := scan(rows)
		if err != nil {
			return nil, err
		}
		if err := table.Append(traceId, spanIds); err != nil {
			return nil, err
		}
	}
	return table, rows.Err()
}

// scanIds reads a row of idsColumns.
func scanIds(rows *stdsql.Rows) (traceId string, spanIds []string, err error) {
	err = rows.Scan(&traceId, &spanIds)
	return traceId, spanIds, err
}

// allEmpty reports whether every table has no rows.
func allEmpty(tables []*ext.Table) bool {
	for _, t := range tables {
		if t.Block().Rows() > 0 {
			return false
		}
	}
	return true
}

// render plans planner as SQL.
func render(ctx *shared.PlannerContext, planner shared.SQLRequestPlanner) (string, error) {
	req, err := planner.Process(ctx)
	if err != nil {
		return "", err
	}
	return req.String(sql.DefaultCtx())
}
