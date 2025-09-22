package controller

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/metrico/qryn/reader/prof"
	v1 "github.com/metrico/qryn/reader/prof/types/v1"
	"github.com/metrico/qryn/reader/service"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ProfController struct {
	Controller
	ProfService *service.ProfService
}

func (pc *ProfController) NotImplemented(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement this
	w.WriteHeader(http.StatusNotImplemented)
}

func (pc *ProfController) ProfileTypes(w http.ResponseWriter, r *http.Request) {
	var req prof.ProfileTypesRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	res, err := pc.ProfService.ProfileTypes(r.Context(),
		time.Unix(0, req.Start*1000000),
		time.Unix(0, req.End*1000000))
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	if len(res) == 0 {
		res = append(res, &v1.ProfileType{
			ID:         "",
			Name:       "",
			SampleType: "",
			SampleUnit: "",
			PeriodType: "",
			PeriodUnit: "",
		})
	}
	pc.writeResponse(w, r, &prof.ProfileTypesResponse{
		ProfileTypes: res,
	})
}

func (pc *ProfController) LabelNames(w http.ResponseWriter, r *http.Request) {
	var req v1.LabelNamesRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	res, err := pc.ProfService.LabelNames(
		r.Context(),
		req.Matchers,
		time.UnixMilli(req.Start),
		time.UnixMilli(req.End))
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	if len(res.Names) == 0 {
		res.Names = append(res.Names, "")
	}
	pc.writeResponse(w, r, res)
}

func (pc *ProfController) LabelValues(w http.ResponseWriter, r *http.Request) {
	var req v1.LabelValuesRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	res, err := pc.ProfService.LabelValues(
		r.Context(),
		req.Matchers,
		req.Name,
		time.UnixMilli(req.Start),
		time.UnixMilli(req.End))
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}

	pc.writeResponse(w, r, res)
}

func (pc *ProfController) SelectMergeStackTraces(w http.ResponseWriter, r *http.Request) {
	var req prof.SelectMergeStacktracesRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	res, err := pc.ProfService.MergeStackTraces(
		r.Context(),
		req.LabelSelector,
		req.ProfileTypeID,
		time.UnixMilli(req.Start),
		time.UnixMilli(req.End))
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	pc.writeResponse(w, r, res)
}

func (pc *ProfController) SelectSeries(w http.ResponseWriter, r *http.Request) {
	var req prof.SelectSeriesRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	agg := v1.TimeSeriesAggregationType_TIME_SERIES_AGGREGATION_TYPE_SUM
	if req.Aggregation != nil {
		agg = *req.Aggregation
	}
	res, err := pc.ProfService.SelectSeries(
		r.Context(),
		req.LabelSelector,
		req.ProfileTypeID,
		req.GroupBy,
		agg,
		int64(req.Step),
		time.UnixMilli(req.Start),
		time.UnixMilli(req.End))
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	pc.writeResponse(w, r, res)
}

func (pc *ProfController) MergeProfiles(w http.ResponseWriter, r *http.Request) {
	var req prof.SelectMergeProfileRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	res, err := pc.ProfService.MergeProfiles(
		r.Context(),
		req.LabelSelector,
		req.ProfileTypeID,
		time.UnixMilli(req.Start),
		time.UnixMilli(req.End))
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	pc.writeResponse(w, r, res)
}

func (pc *ProfController) Series(w http.ResponseWriter, r *http.Request) {
	var req prof.SeriesRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	res, err := pc.ProfService.TimeSeries(
		r.Context(),
		req.Matchers,
		req.LabelNames,
		time.UnixMilli(req.Start),
		time.UnixMilli(req.End))
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	pc.writeResponse(w, r, res)
}

func (pc *ProfController) ProfileStats(w http.ResponseWriter, r *http.Request) {
	res, err := pc.ProfService.ProfileStats(r.Context())
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}

	pc.writeResponse(w, r, res)
}

func (pc *ProfController) Settings(w http.ResponseWriter, r *http.Request) {
	res, err := pc.ProfService.Settings(r.Context())
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	pc.writeResponse(w, r, res)
}

func (pc *ProfController) RenderDiff(w http.ResponseWriter, r *http.Request) {
	for _, param := range []string{"leftQuery", "leftFrom", "leftUntil", "rightQuery", "rightFrom", "rightUntil"} {
		if len(r.URL.Query()[param]) == 0 || r.URL.Query()[param][0] == "" {
			defaultError(w, 400, fmt.Sprintf("Missing required parameter: %s", param))
			return
		}
	}

	leftQuery := r.URL.Query()["leftQuery"][0]
	rightQuery := r.URL.Query()["rightQuery"][0]
	var (
		leftFrom, leftTo, rightFrom, rightTo time.Time
	)
	for _, v := range [][2]any{
		{"leftFrom", &leftFrom}, {"leftUntil", &leftTo}, {"rightFrom", &rightFrom}, {"rightUntil", &rightTo}} {
		strVal := r.URL.Query()[v[0].(string)][0]
		iVal, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			defaultError(w, 400, fmt.Sprintf("Invalid value for %s: %s", html.EscapeString(v[0].(string)), html.EscapeString(strVal)))
			return
		}
		*(v[1].(*time.Time)) = time.Unix(iVal/1000, 0)
	}
	diff, err := pc.ProfService.RenderDiff(
		r.Context(),
		leftQuery,
		rightQuery,
		leftFrom,
		rightFrom,
		leftTo,
		rightTo)
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(diff.FlamebearerProfileV1)
}

func (pc *ProfController) AnalyzeQuery(w http.ResponseWriter, r *http.Request) {
	var req prof.AnalyzeQueryRequest
	err := defaultParser(r, &req)
	if err != nil {
		defaultError(w, 400, err.Error())
		return
	}
	res, err := pc.ProfService.AnalyzeQuery(
		r.Context(),
		req.Query,
		time.UnixMilli(req.Start),
		time.UnixMilli(req.End),
	)
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	pc.writeResponse(w, r, res)
}

func (pc *ProfController) writeResponse(w http.ResponseWriter, r *http.Request, data proto.Message) {
	contentType := r.Header.Get("Content-Type")
	bData, err := defaultMarshaller(r, data)
	if err != nil {
		defaultError(w, 500, err.Error())
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(bData)
}

func defaultParser(r *http.Request, res proto.Message) error {
	contentType := r.Header.Get("Content-Type")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if contentType == "application/json" {
		err = json.Unmarshal(body, res)
	} else {
		err = proto.Unmarshal(body, res)
	}
	if err != nil {
		return err
	}
	return nil
}

func defaultMarshaller[T proto.Message](r *http.Request, t T) ([]byte, error) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/json" {
		return protojson.MarshalOptions{
			UseEnumNumbers:  false,
			EmitUnpopulated: false,
			UseProtoNames:   false,
		}.Marshal(t)
	}

	data, err := proto.Marshal(t)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func defaultError(w http.ResponseWriter, code int, message string) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(strconv.Quote(message)))
}
