package controller

import (
	"net/http"
	"strings"

	"encoding/json/jsontext"
	"encoding/json/v2"

	"github.com/go-faster/jx"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/writer/config"
)

type VolumeController struct {
	Controller
	QueryRangeService *service.QueryRangeService
}

func (q *VolumeController) Volume(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	query := r.URL.Query().Get("query")
	if query == "" {
		PromError(400, "query parameter is required", w)
		return
	}
	req, err := parseQueryRangePropsV3(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	_targetLabels := r.URL.Query().Get("targetLabels")
	var targetLabels []string
	if _targetLabels != "" {
		targetLabels = strings.Split(_targetLabels, ",")
	}
	if req.Step == 0 {
		req.Step = 15000000000
	}
	res, err := q.QueryRangeService.QueryVolume(internalCtx, query, req.Start.UnixNano(), req.End.UnixNano(),
		int64(req.Step/1000000), targetLabels)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	bRes, err := json.Marshal(res, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	stream := &jx.Writer{}

	stream.ObjStart()
	stream.FieldStart("status")
	stream.Str("success")
	stream.Comma()
	stream.FieldStart("data")
	stream.ObjStart()
	stream.FieldStart("resultType")
	stream.Str("vector")
	stream.Comma()
	stream.FieldStart("result")
	stream.Raw(bRes)
	stream.ObjEnd()
	stream.ObjEnd()

	w.Write(stream.Buf)
}

func (q *VolumeController) DetectedLabels(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	query := r.URL.Query().Get("query")
	req, err := parseQueryRangePropsV3(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	if req.Step == 0 {
		req.Step = 15000000000
	}
	res, err := q.QueryRangeService.QueryDetectedLabels(internalCtx, query, req.Start.UnixNano(), req.End.UnixNano())
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	bRes, err := json.Marshal(res, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	stream := &jx.Writer{}

	stream.ObjStart()
	stream.FieldStart("detectedLabels")
	stream.Raw(bRes)
	stream.ObjEnd()

	w.Write(stream.Buf)
}

func (q *VolumeController) DetectedFields(w http.ResponseWriter, r *http.Request) {
	stream := &jx.Writer{}

	stream.ObjStart()
	stream.FieldStart("fields")
	stream.ArrStart()
	stream.ArrEnd()
	stream.ObjEnd()

	w.Write(stream.Buf)
}

func (q *VolumeController) Patterns(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	query := r.URL.Query().Get("query")
	if query == "" {
		PromError(400, "query parameter is required", w)
		return
	}
	req, err := parseQueryRangePropsV3(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	if req.Step == 0 {
		req.Step = 15000000000
	}
	req.Step = max(req.Step, 1000000000)
	limit := int64(config.Cloki.Setting.DRILLDOWN_SETTINGS.LogPatternsReadLimit)
	res, err := q.QueryRangeService.QueryPatterns(internalCtx, query, req.Start.UnixNano(), req.End.UnixNano(),
		int64(req.Step/1000000), limit)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	bRes, err := json.Marshal(res, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	stream := &jx.Writer{}

	stream.ObjStart()
	stream.FieldStart("status")
	stream.Str("success")
	stream.Comma()
	stream.FieldStart("data")
	stream.Raw(bRes)
	stream.ObjEnd()

	w.Write(stream.Buf)
}
