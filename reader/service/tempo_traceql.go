package service

import (
	"context"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	traceql_transpiler "github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	"github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/tables"
)

func (t *TempoService) SearchTraceQL(ctx context.Context,
	q string, limit int, spss int, from time.Time, to time.Time) (chan []model.TraceInfo, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	query, err := traceql_ir.Parse(q)
	if err != nil {
		return nil, err
	}
	planner, err := traceql_transpiler.Plan(query)
	if err != nil {
		return nil, err
	}
	reqCtx := ctx
	ctx, cancel := context.WithCancel(ctx)

	sqlCtx, err := traceQLPlannerContext(ctx, conn, q, from, to, limit)
	if err != nil {
		cancel()
		return nil, err
	}
	sqlCtx.CancelCtx = cancel
	sqlCtx.SpansPerSpanSet = int64(spss)

	ch, err := planner.Process(sqlCtx)

	if err != nil {
		return nil, err
	}
	traceql_transpiler.WarnUnresolved(reqCtx, query, sqlCtx)
	res := make(chan []model.TraceInfo)
	go func() {
		defer close(res)
		defer cancel()
		for ch := range ch {
			res <- ch
		}
	}()
	return res, nil
}

// traceQLPlannerContext builds the planner context shared by TraceQL search,
// autocomplete and metrics, with the server's version info resolved.
func traceQLPlannerContext(ctx context.Context, conn *model.DataDatabasesMap,
	query string, from time.Time, to time.Time, limit int) (*shared.PlannerContext, error) {
	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}
	planCtx := &shared.PlannerContext{
		IsCluster:   conn.Config.ClusterName != "",
		From:        from,
		To:          to,
		Limit:       int64(limit),
		Ctx:         ctx,
		CHDb:        conn.Session,
		VersionInfo: versionInfo,
		TraceQL:     query,

		AncestryRounds: traceqlconfig.AncestryRounds(),
	}
	tables.PopulateTableNames(planCtx, conn)
	return planCtx, nil
}
