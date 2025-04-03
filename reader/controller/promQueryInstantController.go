package controllerv1

import (
	"fmt"
	"github.com/gorilla/schema"
	"net/http"
	"time"
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
	promQuery, err := q.Api.QueryEngine.NewInstantQuery(q.Storage.SetOidAndDB(ctx), nil, req.Query, req.Time)
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
