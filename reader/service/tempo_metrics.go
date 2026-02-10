package service

import (
	"context"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/traceql/traceql_metrics"
	"github.com/metrico/qryn/v4/reader/utils/tables"
)

// MetricsQueryRange executes a TraceQL metrics range query.
func (t *TempoService) MetricsQueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (*model.MetricsQueryRangeResult, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	// Get table names using PlannerContext
	isCluster := conn.Config.ClusterName != ""
	sqlCtx := &shared.PlannerContext{
		IsCluster: isCluster,
	}
	tables.PopulateTableNames(sqlCtx, conn)

	tracesTable := sqlCtx.TracesTable
	attrsTable := sqlCtx.TracesAttrsTable
	if isCluster {
		tracesTable = sqlCtx.TracesDistTable
		attrsTable = sqlCtx.TracesAttrsDistTable
	}

	// Create querier and engine
	querier := traceql_metrics.NewClickHouseQuerier(conn.Session, tracesTable, attrsTable, isCluster)
	engine := traceql_metrics.NewEngine(querier)

	// Execute query
	result, err := engine.ExecuteRange(ctx, query, start, end, step)
	if err != nil {
		return nil, err
	}

	// Convert to model result
	modelResult := &model.MetricsQueryRangeResult{
		Series: make([]model.MetricsSeries, len(result.Series)),
	}
	for i, s := range result.Series {
		modelResult.Series[i] = model.MetricsSeries{
			Labels: s.Labels,
			Values: s.Values,
			Times:  s.Times,
		}
	}

	return modelResult, nil
}

// MetricsQueryInstant executes a TraceQL metrics instant query.
func (t *TempoService) MetricsQueryInstant(ctx context.Context, query string, ts time.Time) (*model.MetricsQueryResult, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	// Get table names using PlannerContext
	isCluster := conn.Config.ClusterName != ""
	sqlCtx := &shared.PlannerContext{
		IsCluster: isCluster,
	}
	tables.PopulateTableNames(sqlCtx, conn)

	tracesTable := sqlCtx.TracesTable
	attrsTable := sqlCtx.TracesAttrsTable
	if isCluster {
		tracesTable = sqlCtx.TracesDistTable
		attrsTable = sqlCtx.TracesAttrsDistTable
	}

	// Create querier and engine
	querier := traceql_metrics.NewClickHouseQuerier(conn.Session, tracesTable, attrsTable, isCluster)
	engine := traceql_metrics.NewEngine(querier)

	// Execute query
	result, err := engine.ExecuteInstant(ctx, query, ts)
	if err != nil {
		return nil, err
	}

	// Convert to model result
	modelResult := &model.MetricsQueryResult{
		Series: make([]model.MetricsSeries, len(result.Series)),
	}
	for i, s := range result.Series {
		modelResult.Series[i] = model.MetricsSeries{
			Labels: s.Labels,
			Values: s.Values,
			Times:  s.Times,
		}
	}

	return modelResult, nil
}
