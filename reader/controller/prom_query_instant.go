package controller

import (
	"fmt"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"net/http"
	"time"
)

type queryInstantProps struct {
	Raw struct {
		Time  string
		Query string
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
	versionInfo := q.Storage.ResolveVersionInfo(ctx)
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
	}
	res.Raw.Query = r.FormValue("query")
	res.Raw.Time = r.FormValue("time")
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
