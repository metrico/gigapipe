package prof

import (
	"context"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/prof/parser"
	shared2 "github.com/metrico/qryn/reader/prof/shared"
	"github.com/metrico/qryn/reader/prof/transpiler"
	v1 "github.com/metrico/qryn/reader/prof/types/v1"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/metrico/qryn/reader/utils/tables"
	"time"
)

func PlanLabelNames(ctx context.Context, scripts []*parser.Script, from time.Time, to time.Time,
	db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := transpiler.PlanLabelNames(scripts)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanLabelValues(ctx context.Context, scripts []*parser.Script, labelName string, from time.Time, to time.Time,
	db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := transpiler.PlanLabelValues(scripts, labelName)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanMergeTraces(ctx context.Context, script *parser.Script, typeId *shared2.TypeId,
	from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := transpiler.PlanMergeTraces(script, typeId)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanSelectSeries(ctx context.Context, script *parser.Script, tId *shared2.TypeId, groupBy []string,
	agg v1.TimeSeriesAggregationType, step int64, from time.Time, to time.Time,
	db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := transpiler.PlanSelectSeries(script, tId, groupBy, agg, step)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanMergeProfiles(ctx context.Context, script *parser.Script, typeId *shared2.TypeId,
	from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := transpiler.PlanMergeProfiles(script, typeId)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanSeries(ctx context.Context, scripts []*parser.Script,
	labelNames []string, from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := transpiler.PlanSeries(scripts, labelNames)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanAnalyzeQuery(ctx context.Context, script *parser.Script,
	from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := transpiler.PlanAnalyzeQuery(script)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func plannerCtx(ctx context.Context, db *model.DataDatabasesMap, from, to time.Time) *shared.PlannerContext {
	sqlCtx := shared.PlannerContext{
		From: from,
		To:   to,
		Ctx:  ctx,
	}
	tables.PopulateTableNames(&sqlCtx, db)
	return &sqlCtx
}
