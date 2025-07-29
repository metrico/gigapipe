package controller

import (
	"net/http"
	"time"

	"github.com/metrico/qryn/shared/validator"
)

type queryInstantProps struct {
	Raw struct {
		Time  string `form:"time"`
		Query string `form:"query"`
	}
	Time  time.Time
	Query string
}

type queryRequest struct {
	Time  validator.FlexibleTime `json:"time" schema:"time" validate:"required"`
	Query string                 `json:"query" schema:"query" validate:"required"`
}

func (q *PromQueryRangeController) QueryInstant(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	ctx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	p, err := validator.ValidateRequest[queryRequest](r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	promQuery, err := q.Api.QueryEngine.NewInstantQuery(q.Storage.SetOidAndDB(ctx), nil, p.Query, p.Time.Time())
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
