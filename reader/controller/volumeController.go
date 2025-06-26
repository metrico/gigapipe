package controllerv1

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/reader/service"
	"github.com/metrico/qryn/writer/config"
	"net/http"
	"strings"
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
	bRes, err := jsoniter.Marshal(res)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	json := jsoniter.ConfigFastest
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("status")
	stream.WriteString("success")
	stream.WriteMore()
	stream.WriteObjectField("data")
	stream.WriteObjectStart()
	stream.WriteObjectField("resultType")
	stream.WriteString("vector")
	stream.WriteMore()
	stream.WriteObjectField("result")
	stream.WriteRaw(string(bRes))
	stream.WriteObjectEnd()
	stream.WriteObjectEnd()

	w.Write(stream.Buffer())
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
	bRes, err := jsoniter.Marshal(res)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	json := jsoniter.ConfigFastest
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("detectedLabels")
	stream.WriteRaw(string(bRes))
	stream.WriteObjectEnd()

	w.Write(stream.Buffer())
}

func (q *VolumeController) DetectedFields(w http.ResponseWriter, r *http.Request) {
	json := jsoniter.ConfigFastest
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("fields")
	stream.WriteArrayStart()
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()

	w.Write(stream.Buffer())
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
	bRes, err := jsoniter.Marshal(res)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	json := jsoniter.ConfigFastest
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("status")
	stream.WriteString("success")
	stream.WriteMore()
	stream.WriteObjectField("data")
	stream.WriteRaw(string(bRes))
	stream.WriteObjectEnd()

	w.Write(stream.Buffer())
}
