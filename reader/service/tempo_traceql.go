package service

import (
	"context"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	traceql_transpiler "github.com/metrico/qryn/reader/traceql/transpiler"
	"github.com/metrico/qryn/reader/utils/dbVersion"
	"github.com/metrico/qryn/reader/utils/tables"
	"time"
)

func (t *TempoService) SearchTraceQL(ctx context.Context,
	q string, limit int, from time.Time, to time.Time) (chan []model.TraceInfo, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	script, err := traceql_parser.Parse(q)
	if err != nil {
		return nil, err
	}
	planner, err := traceql_transpiler.Plan(script)
	if err != nil {
		return nil, err
	}
	versionInfo, err := dbVersion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
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
		for ch := range ch {
			res <- ch
		}
	}()
	return res, nil
}
