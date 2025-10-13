package prof

import (
	"context"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/prof/prof_parser"
	"github.com/metrico/qryn/v4/reader/prof/prof_transpiler"
	shared2 "github.com/metrico/qryn/v4/reader/prof/shared"
	v1 "github.com/metrico/qryn/v4/reader/prof/types/v1"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/metrico/qryn/v4/reader/utils/tables"
)

func PlanLabelNames(ctx context.Context, scripts []*prof_parser.Script, from time.Time, to time.Time,
	db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := prof_transpiler.PlanLabelNames(scripts)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanLabelValues(ctx context.Context, scripts []*prof_parser.Script, labelName string, from time.Time, to time.Time,
	db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := prof_transpiler.PlanLabelValues(scripts, labelName)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanMergeTraces(ctx context.Context, script *prof_parser.Script, typeId *shared2.TypeId,
	from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := prof_transpiler.PlanMergeTraces(script, typeId)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanSelectSeries(ctx context.Context, script *prof_parser.Script, tId *shared2.TypeId, groupBy []string,
	agg v1.TimeSeriesAggregationType, step int64, from time.Time, to time.Time,
	db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := prof_transpiler.PlanSelectSeries(script, tId, groupBy, agg, step)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanMergeProfiles(ctx context.Context, script *prof_parser.Script, typeId *shared2.TypeId,
	from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := prof_transpiler.PlanMergeProfiles(script, typeId)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanSeries(ctx context.Context, scripts []*prof_parser.Script,
	labelNames []string, from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := prof_transpiler.PlanSeries(scripts, labelNames)
	if err != nil {
		return nil, err
	}
	return planner.Process(plannerCtx(ctx, db, from, to))
}

func PlanAnalyzeQuery(ctx context.Context, script *prof_parser.Script,
	from time.Time, to time.Time, db *model.DataDatabasesMap) (sql.ISelect, error) {
	planner, err := prof_transpiler.PlanAnalyzeQuery(script)
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
