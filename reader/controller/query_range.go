package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/service"
	"github.com/metrico/qryn/v4/reader/utils/logger"
)

const (
	tailDefaultLimit int64 = 100  // lines returned when no ?limit= is supplied
	tailMaxLimit     int64 = 5000 // hard cap to prevent excessive memory use
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

	start, err := getRequiredFloat(r, "start", "", nil)
	end, err := getRequiredFloat(r, "end", "", err)
	step, err := getRequiredDuration(r, "step", "1", err)
	direction := r.URL.Query().Get("direction")
	//if direction == "" {
	//	direction = "backward"
	//}
	_limit := r.URL.Query().Get("limit")
	limit := int64(0)
	if _limit != "" {
		limit, _ = strconv.ParseInt(_limit, 10, 64)
	}
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	ch, err := q.QueryRangeService.QueryRange(internalCtx, query, int64(start), int64(end), int64(step*1000),
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
		stream.WriteArrayStart()

		stream.WriteObjectStart()
		stream.WriteObjectField("metric")
		stream.WriteEmptyObject()
		stream.WriteMore()

		stream.WriteObjectField("value")
		stream.WriteArrayStart()
		stream.WriteInt64(time.Now().Unix()) // Unix timestamp
		stream.WriteMore()
		stream.WriteString("2")
		stream.WriteArrayEnd()

		stream.WriteObjectEnd() // End of result object
		stream.WriteArrayEnd()  // End of result array

		stream.WriteObjectEnd() // End of data object
		stream.WriteObjectEnd() // End of main object

		w.Write(stream.Buffer())
		//w.Write([]byte(fmt.Sprintf(`{"status": "success", "data": {"resultType": "vector", "result": [{
		// "metric": {},
		// "value": [%d, "2"]
		//}]}}`, time.Now().Unix())))
		return
	}
	iTime, err := getRequiredI64(r, "time", "0", nil)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	if iTime == 0 {
		iTime = time.Now().UnixNano()
	}

	step, err := getRequiredDuration(r, "step", "1", err)
	_limit := r.URL.Query().Get("limit")
	limit := int64(100)
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
		if parsed, parseErr := strconv.ParseInt(_s, 10, 64); parseErr == nil && parsed > 0 {
			startNs = parsed
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
		if parsed, parseErr := strconv.ParseInt(startStr, 10, 64); parseErr == nil {
			fromNs = parsed
		}
	}
	if endStr != "" {
		if parsed, parseErr := strconv.ParseInt(endStr, 10, 64); parseErr == nil {
			toNs = parsed
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
