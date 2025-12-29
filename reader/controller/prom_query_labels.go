package controller

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/metrico/qryn/v4/reader/service"
)

type PromQueryLabelsController struct {
	Controller
	QueryLabelsService *service.QueryLabelsService
	MetadataService    *service.MetadataService
}

type promLabelsParams struct {
	start time.Time
	end   time.Time
	match []string
}

type rawPromLabelsParams struct {
	Start string   `form:"start"`
	End   string   `form:"end"`
	Match []string `form:"match[]"`
}

type promSeriesParams struct {
	Match []string `form:"match"`
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
	res, err := p.QueryLabelsService.Labels(internalCtx, params.start.UnixMilli(), params.end.UnixMilli(), 2,
		params.match)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	SmartBufferServeStrings(w, res)
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
	SmartBufferServeStrings(w, res)
}

func (p *PromQueryLabelsController) Metadata(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	metricFilter := r.URL.Query().Get("metric")
	limitStr := r.URL.Query().Get("limit")
	limitPerMetricStr := r.URL.Query().Get("limit_per_metric")

	limit := 0
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	limitPerMetric := 0
	if limitPerMetricStr != "" {
		if parsedLimit, err := strconv.Atoi(limitPerMetricStr); err == nil && parsedLimit > 0 {
			limitPerMetric = parsedLimit
		}
	}

	if p.MetadataService == nil {
		w.WriteHeader(200)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": "success", "data": {}}`))
		return
	}

	res, err := p.MetadataService.Metadata(internalCtx, metricFilter, limit, limitPerMetric)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	SmartBufferServeStrings(w, res)
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
	SmartBufferServeStrings(w, res)
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
		dec.IgnoreUnknownKeys(true)
		err := r.ParseForm()
		if err != nil {
			return nil, err
		}
		if matches, ok := r.Form["match[]"]; ok {
			rawParams.Match = matches
		}
		err = dec.Decode(&rawParams, r.Form)
		if err != nil {
			return nil, err
		}
		return &promLabelsParams{
			start: parserTimeString(rawParams.Start, time.Now().Add(time.Hour*-6)),
			end:   parserTimeString(rawParams.End, time.Now()),
			match: rawParams.Match,
		}, nil
	}

	return &promLabelsParams{
		start: parserTimeString(r.URL.Query().Get("start"), time.Now().Add(time.Hour*-6)),
		end:   parserTimeString(r.URL.Query().Get("end"), time.Now().Add(time.Hour*-6)),
	}, nil
}
