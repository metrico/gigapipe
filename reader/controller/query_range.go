package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-faster/jx"
	"github.com/gorilla/websocket"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/reader/utils/logger"
)

const (
	tailDefaultLimit  int64 = 100  // lines returned when no ?limit= is supplied
	tailMaxLimit      int64 = 5000 // hard cap to prevent excessive memory use
	queryDefaultLimit int64 = 100  // matches Loki API default for /query and /query_range
)

type QueryRangeController struct {
	Controller
	QueryRangeService *service.QueryRangeService
}

func (q *QueryRangeController) QueryRange(w http.ResponseWriter, r *http.Request) {
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

	startRaw := r.URL.Query().Get("start")
	if startRaw == "" {
		PromError(400, "start parameter is required", w)
		return
	}
	startTime, err := ParseTimeSecOrRFC(startRaw, time.Time{})
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	endRaw := r.URL.Query().Get("end")
	if endRaw == "" {
		PromError(400, "end parameter is required", w)
		return
	}
	endTime, err := ParseTimeSecOrRFC(endRaw, time.Time{})
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	step, err := getRequiredDuration(r, "step", "1", nil)
	direction := r.URL.Query().Get("direction")
	_limit := r.URL.Query().Get("limit")
	limit := queryDefaultLimit
	if _limit != "" {
		limit, _ = strconv.ParseInt(_limit, 10, 64)
	}
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	ch, err := q.QueryRangeService.QueryRange(internalCtx, query, startTime.UnixNano(), endTime.UnixNano(), int64(step*1000),
		limit, direction == "forward")
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	SmartBufferServe(w, ch)
}

func (q *QueryRangeController) Query(w http.ResponseWriter, r *http.Request) {
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
	if query == "vector(1)+vector(1)" {
		w.Header().Set("Content-Type", "application/json")
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
		stream.ArrStart()

		stream.ObjStart()
		stream.FieldStart("metric")
		stream.RawStr("{}")
		stream.Comma()

		stream.FieldStart("value")
		stream.ArrStart()
		stream.Int64(time.Now().Unix()) // Unix timestamp
		stream.Comma()
		stream.Str("2")
		stream.ArrEnd()

		stream.ObjEnd() // End of result object
		stream.ArrEnd() // End of result array

		stream.ObjEnd() // End of data object
		stream.ObjEnd() // End of main object

		w.Write(stream.Buf)
		//w.Write([]byte(fmt.Sprintf(`{"status": "success", "data": {"resultType": "vector", "result": [{
		// "metric": {},
		// "value": [%d, "2"]
		//}]}}`, time.Now().Unix())))
		return
	}
	var iTime int64
	timeRaw := r.URL.Query().Get("time")
	if timeRaw == "" || timeRaw == "0" {
		iTime = time.Now().UnixNano()
	} else {
		queryTime, err := ParseTimeSecOrRFC(timeRaw, time.Now())
		if err != nil {
			PromError(400, err.Error(), w)
			return
		}
		iTime = queryTime.UnixNano()
	}

	step, err := getRequiredDuration(r, "step", "1", nil)
	_limit := r.URL.Query().Get("limit")
	limit := queryDefaultLimit
	if _limit != "" {
		limit, _ = strconv.ParseInt(_limit, 10, 64)
	}
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	ch, err := q.QueryRangeService.QueryInstant(internalCtx, query, iTime, int64(step*1000),
		limit)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	SmartBufferServe(w, ch)
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func (q *QueryRangeController) Tail(w http.ResponseWriter, r *http.Request) {
	watchCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	internalCtx, err := runPreWSRequestPlugins(watchCtx, r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	query := r.URL.Query().Get("query")
	if query == "" {
		logger.Error("tail: query parameter is required")
		return
	}

	tailLimit := tailDefaultLimit
	if _tl := r.URL.Query().Get("limit"); _tl != "" {
		if parsed, parseErr := strconv.ParseInt(_tl, 10, 64); parseErr == nil && parsed > 0 {
			tailLimit = parsed
		}
	}
	if tailLimit > tailMaxLimit {
		tailLimit = tailMaxLimit
	}

	var startNs int64
	if _s := r.URL.Query().Get("start"); _s != "" {
		if t, parseErr := ParseTimeSecOrRFC(_s, time.Time{}); parseErr == nil && t.UnixNano() > 0 {
			startNs = t.UnixNano()
		}
	}

	var watcher model.IWatcher
	watcher, err = q.QueryRangeService.Tail(internalCtx, query, tailLimit, startNs)
	if err != nil {
		logger.Error("tail: watcher create failed:", err)
		cancel()
		return
	}
	defer func() {
		go func() {
			for range watcher.GetRes() {
			}
		}()
	}()
	defer watcher.Close()
	con, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	defer con.Close()
	con.SetCloseHandler(func(code int, text string) error {
		msg := websocket.FormatCloseMessage(code, "")
		_ = con.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second))
		watcher.Close()
		cancel()
		return nil
	})
	go func() {
		_, _, readErr := con.ReadMessage()
		for readErr == nil {
			_, _, readErr = con.ReadMessage()
		}
		watcher.Close()
		cancel()
	}()
	for {
		select {
		case <-watchCtx.Done():
			return
		case str, ok := <-watcher.GetRes():
			if !ok {
				return
			}
			if str.Err != nil {
				logger.Error("tail: ws stream error:", str.Err)
				msg := websocket.FormatCloseMessage(websocket.CloseInternalServerErr, str.Err.Error())
				_ = con.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second))
				return
			}
			if err = con.WriteMessage(websocket.TextMessage, []byte(str.Str)); err != nil {
				logger.Error("tail: data write failed:", err)
				return
			}
		}
	}
}

// IndexStats handles GET /loki/api/v1/index/stats.
// Grafana Live tail calls this on activation to populate stream statistics.
func (q *QueryRangeController) IndexStats(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	query := r.URL.Query().Get("query")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	var fromNs, toNs int64
	if startStr != "" {
		if t, parseErr := ParseTimeSecOrRFC(startStr, time.Time{}); parseErr == nil {
			fromNs = t.UnixNano()
		}
	}
	if endStr != "" {
		if t, parseErr := ParseTimeSecOrRFC(endStr, time.Time{}); parseErr == nil {
			toNs = t.UnixNano()
		}
	}
	if toNs == 0 {
		toNs = time.Now().UnixNano()
	}
	if fromNs == 0 {
		fromNs = toNs - int64(time.Hour)
	}

	result, err := q.QueryRangeService.QueryIndexStats(internalCtx, query, fromNs, toNs)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
