package controller

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/metrico/qryn/reader/service"
)

type PromQueryLabelsController struct {
	Controller
	QueryLabelsService *service.QueryLabelsService
}

type promLabelsParams struct {
	start time.Time
	end   time.Time
}

type rawPromLabelsParams struct {
	Start string `form:"start"`
	End   string `form:"end"`
}

type promSeriesParams struct {
	Match []string `form:"match[]"`
}

func (p *PromQueryLabelsController) PromLabels(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := getLabelsParams(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	res, err := p.QueryLabelsService.Labels(internalCtx, params.start.UnixMilli(), params.end.UnixMilli(), 2)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	for str := range res {
		w.Write([]byte(str))
	}
}

func (p *PromQueryLabelsController) LabelValues(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := ParseLogSeriesParamsV2(r, time.Second)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	name := mux.Vars(r)["name"]
	if name == "" {
		PromError(400, "label name is required", w)
		return
	}
	res, err := p.QueryLabelsService.PromValues(internalCtx, name, params.Match,
		params.ValuesParams.Start.UnixMilli(), params.ValuesParams.End.UnixMilli(), 2)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	for str := range res {
		w.Write([]byte(str))
	}
}

func (p *PromQueryLabelsController) Metadata(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	_, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.WriteHeader(200)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "success", "data": {}}`))
}

func (p *PromQueryLabelsController) Series(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := getLabelsParams(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	seriesParams, err := getPromSeriesParamsV2(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}

	res, err := p.QueryLabelsService.Series(internalCtx, seriesParams.Match, params.start.UnixMilli(),
		params.end.UnixMilli(), 2)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.WriteHeader(200)
	w.Header().Set("Content-Type", "application/json")
	for str := range res {
		w.Write([]byte(str))
	}
}

func getPromSeriesParamsV2(r *http.Request) (promSeriesParams, error) {
	res := promSeriesParams{}
	if r.Method == "POST" && r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
		err := r.ParseForm()
		if err != nil {
			return res, err
		}
		for key, value := range r.Form {
			if key == "match[]" {
				res.Match = append(res.Match, value...)
			}
		}
	}
	res.Match = append(res.Match, r.URL.Query()["match[]"]...)
	return res, nil
}

func parserTimeString(strTime string, def time.Time) time.Time {
	tTime, err := time.Parse(time.RFC3339, strTime)
	if err == nil {
		return tTime
	}
	iTime, err := strconv.ParseInt(strTime, 10, 63)
	if err == nil {
		return time.Unix(iTime, 0)
	}
	return def
}

func getLabelsParams(r *http.Request) (*promLabelsParams, error) {
	if r.Method == "POST" && r.Header.Get("content-type") == "application/x-www-form-urlencoded" {
		rawParams := rawPromLabelsParams{}
		dec := schema.NewDecoder()
		err := r.ParseForm()
		if err != nil {
			return nil, err
		}
		err = dec.Decode(&rawParams, r.Form)
		if err != nil {
			return nil, err
		}
		return &promLabelsParams{
			start: parserTimeString(rawParams.Start, time.Now().Add(time.Hour*-6)),
			end:   parserTimeString(rawParams.End, time.Now()),
		}, nil
	}

	return &promLabelsParams{
		start: parserTimeString(r.URL.Query().Get("start"), time.Now().Add(time.Hour*-6)),
		end:   parserTimeString(r.URL.Query().Get("end"), time.Now().Add(time.Hour*-6)),
	}, nil
}
