package service

import (
	"context"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/traceql/tempo"
	traceql_transpiler_v2 "github.com/metrico/qryn/v4/reader/traceql/traceql_transpiler_v2"
	"github.com/metrico/qryn/v4/reader/utils/dbVersion"
	"github.com/metrico/qryn/v4/reader/utils/tables"
)

func (t *TempoService) SearchTraceQL(ctx context.Context,
	q string, limit int, from time.Time, to time.Time) (chan []model.TraceInfo, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	// Use Tempo parser for full TraceQL support (including {true}, status=error, etc.)
	ast, err := tempo.Parse(q)
	if err != nil {
		return nil, err
	}
	planner, err := traceql_transpiler_v2.Plan(ast)
	if err != nil {
		return nil, err
	}
	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	sqlCtx := &shared.PlannerContext{
		IsCluster:   conn.Config.ClusterName != "",
		From:        from,
		To:          to,
		Limit:       int64(limit),
		Ctx:         ctx,
		CHDb:        conn.Session,
		CancelCtx:   cancel,
		VersionInfo: versionInfo,
	}
	tables.PopulateTableNames(sqlCtx, conn)

	ch, err := planner.Process(sqlCtx)

	if err != nil {
		return nil, err
	}
	res := make(chan []model.TraceInfo)
	go func() {
		defer close(res)
		defer cancel()
		for traces := range ch {
			res <- traces
		}
	}()
	return res, nil
}
