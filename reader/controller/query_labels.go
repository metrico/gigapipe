package controller

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/metrico/qryn/reader/service"
)

type QueryLabelsController struct {
	Controller
	QueryLabelsService *service.QueryLabelsService
}

type ValuesParams struct {
	Start time.Time
	End   time.Time
	Raw   struct {
		Start string `query:"start"`
		End   string `query:"end"`
	}
}

func (q *QueryLabelsController) Labels(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := ParseTimeParamsV2(r, time.Nanosecond)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	res, err := q.QueryLabelsService.Labels(internalCtx, params.Start.UnixMilli(), params.End.UnixMilli(), 1)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	for str := range res {
		w.Write([]byte(str))
	}
}

func (q *QueryLabelsController) Values(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := ParseLogSeriesParamsV2(r, time.Nanosecond)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	name := mux.Vars(r)["name"]
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
	for str := range res {
		w.Write([]byte(str))
	}
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
	params, err := ParseLogSeriesParamsV2(r, time.Nanosecond)
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
	for str := range res {
		w.Write([]byte(str))
	}
}

func ParseLogSeriesParamsV2(r *http.Request, unit time.Duration) (SeriesParams, error) {
	res := SeriesParams{}
	var err error
	res.ValuesParams, err = ParseTimeParamsV2(r, unit)
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

func ParseTimeParamsV2(r *http.Request, unit time.Duration) (ValuesParams, error) {
	//TODO: Rewrite ParseTimeParams using http.Request instead of fiber.Ctx
	res := ValuesParams{}
	if r.Method == "POST" && r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
		err := r.ParseForm()
		if err != nil {
			return res, err
		}
		dec := schema.NewDecoder()
		err = dec.Decode(&res.Raw, r.Form)
		if err != nil {
			return res, err
		}
	}
	if res.Raw.Start == "" {
		res.Raw.Start = r.URL.Query().Get("start")
	}
	if res.Raw.End == "" {
		res.Raw.End = r.URL.Query().Get("end")
	}
	res.Start = time.Now().Add(time.Hour * -6)
	if res.Raw.Start != "" {
		start, err := strconv.ParseInt(res.Raw.Start, 10, 64)
		if err != nil {
			return res, err
		}
		res.Start = time.Unix(0, 0).Add(time.Duration(start) * unit)
	}
	res.End = time.Now()
	if res.Raw.End != "" {
		end, err := strconv.ParseInt(res.Raw.End, 10, 64)
		if err != nil {
			return res, err
		}
		res.End = time.Unix(0, 0).Add(time.Duration(end) * unit)
	}
	return res, nil
}
