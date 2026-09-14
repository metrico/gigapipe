package controller

import (
	"fmt"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"net/http"
	"time"

	"github.com/gorilla/schema"
)

type queryInstantProps struct {
	Raw struct {
		Time  string `form:"time"`
		Query string `form:"query"`
	}
	Time  time.Time
	Query string
}

func (q *PromQueryRangeController) QueryInstant(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	ctx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	req, err := parseQueryInstantProps(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	expr, err := promql_parser.Parse(req.Query)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	// The optimizers push rate/increase/aggregations down into metrics_15s;
	// skip them when the aggregation cannot cover the query window so the
	// engine evaluates the original expression over raw samples instead. The
	// same version info snapshot is passed down to Select so per-selector
	// routing agrees with this decision.
	//
	// A failed probe (nil) also skips them: the substitutes they install are
	// read by Select before its own routing check, so optimizing on an unknown
	// aggregate state can read an aggregate holding no metric rows and return
	// an empty result instead of an error. Raw samples always answer correctly.
	versionInfo := q.Storage.ResolveVersionInfo(ctx)
	earliestNS := promql_transpiler.EarliestReadNS(expr.Expr, req.Time)
	if versionInfo != nil && versionInfo.Metrics15sAvailable(earliestNS) {
		expr, err = promql_transpiler.TranspileExpressionV2(expr)
		if err != nil {
			logger.Error("[PQRC005] " + err.Error())
			PromError(500, err.Error(), w)
			return
		}
	}
	queryStorage := q.Storage.SetOidAndDB(ctx, expr)
	queryStorage.VersionInfo = versionInfo
	promQuery, err := q.Engine.NewInstantQuery(ctx, queryStorage, nil,
		expr.Expr.String(), req.Time)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	res := promQuery.Exec(ctx)
	if res.Err != nil {
		PromError(500, res.Err.Error(), w)
		return
	}
	err = writeResponse(res, w)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
}

func parseQueryInstantProps(r *http.Request) (queryInstantProps, error) {
	res := queryInstantProps{}
	var err error
	if r.Method == "POST" && r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
		err = r.ParseForm()
		if err != nil {
			return res, err
		}

		dec := schema.NewDecoder()
		err = dec.Decode(&res.Raw, r.Form)
		if err != nil {
			return res, err
		}
	}
	if res.Raw.Query == "" {
		res.Raw.Query = r.URL.Query().Get("query")
	}
	if res.Raw.Time == "" {
		res.Raw.Time = r.URL.Query().Get("time")
	}
	res.Time, err = ParseTimeSecOrRFC(res.Raw.Time, time.Now())
	if err != nil {
		return res, err
	}
	if res.Raw.Query == "" {
		return res, fmt.Errorf("query is undefined")
	}
	res.Query = res.Raw.Query
	return res, err
}
