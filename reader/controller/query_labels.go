package controller

import (
	"net/http"
	"time"

	"github.com/metrico/qryn/v5/reader/service"
)

type QueryLabelsController struct {
	Controller
	QueryLabelsService *service.QueryLabelsService
}

type ValuesParams struct {
	Start time.Time
	End   time.Time
	Raw   struct {
		Start string
		End   string
	}
}

func (q *QueryLabelsController) Labels(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := ParseTimeParamsV2(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	res, err := q.QueryLabelsService.Labels(internalCtx, params.Start.UnixMilli(), params.End.UnixMilli(), 1,
		nil)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	SmartBufferServeStrings(w, res)
}

func (q *QueryLabelsController) Values(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := ParseLogSeriesParamsV2(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	name := r.PathValue("name")
	if name == "" {
		PromError(500, "label name is required", w)
		return
	}
	res, err := q.QueryLabelsService.Values(internalCtx, name, params.Match,
		params.Start.UnixMilli(), params.End.UnixMilli(), 1)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	SmartBufferServeStrings(w, res)
}

type SeriesParams struct {
	ValuesParams
	Match []string `query:"match[]"`
	Raw   struct {
		Match []string `query:"match[]"`
		Query string   `query:"query"`
	}
}

func (q *QueryLabelsController) Series(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := ParseLogSeriesParamsV2(r)
	if len(params.Match) == 0 {
		PromError(400, "match param is required", w)
		return
	}
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	res, err := q.QueryLabelsService.Series(internalCtx, params.Match,
		params.ValuesParams.Start.UnixMilli(), params.ValuesParams.End.UnixMilli(), 1)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	SmartBufferServeStrings(w, res)
}

func ParseLogSeriesParamsV2(r *http.Request) (SeriesParams, error) {
	res := SeriesParams{}
	var err error
	res.ValuesParams, err = ParseTimeParamsV2(r)
	if err != nil {
		return res, err
	}
	if r.Method == "POST" && r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
		err = r.ParseForm()
		if err != nil {
			return res, err
		}
		res.Raw.Match = append(res.Raw.Match, r.Form["match[]"]...)

	}
	res.Raw.Match = append(res.Raw.Match, r.URL.Query()["match[]"]...)
	if len(r.URL.Query()["query"]) > 0 {
		res.Raw.Query = r.URL.Query().Get("query")
	}
	res.Match = res.Raw.Match
	if res.Raw.Query != "" {
		res.Match = append(res.Match, res.Raw.Query)
	}
	return res, nil
}

func ParseTimeParamsV2(r *http.Request) (ValuesParams, error) {
	// TODO: Rewrite ParseTimeParams using http.Request instead of fiber.Ctx
	res := ValuesParams{}
	if r.Method == "POST" && r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
		err := r.ParseForm()
		if err != nil {
			return res, err
		}
	}
	res.Raw.Start = r.FormValue("start")
	res.Raw.End = r.FormValue("end")
	res.Start = time.Now().Add(time.Hour * -6)
	if res.Raw.Start != "" {
		t, err := ParseTimeSecOrRFC(res.Raw.Start, res.Start)
		if err != nil {
			return res, err
		}
		res.Start = t
	}
	res.End = time.Now()
	if res.Raw.End != "" {
		t, err := ParseTimeSecOrRFC(res.Raw.End, res.End)
		if err != nil {
			return res, err
		}
		res.End = t
	}
	return res, nil
}
