package controller

import (
	"context"
	"net/http"
	"strconv"
	"time"

	ws "github.com/gofiber/websocket/v2"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/service"
	"github.com/metrico/qryn/reader/utils/logger"
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
	w.Header().Set("Content-Type", "application/json")
	for str := range ch {
		w.Write([]byte(str.Str))
	}
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
	w.Header().Set("Content-Type", "application/json")
	for str := range ch {
		w.Write([]byte(str.Str))
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
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
		logger.Error("query parameter is required")
		return
	}
	defer cancel()
	var watcher model.IWatcher
	watcher, err = q.QueryRangeService.Tail(internalCtx, query)
	if err != nil {
		logger.Error(err)
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
		watcher.Close()
		cancel()
		return nil
	})
	go func() {
		_, _, err := con.ReadMessage()
		for err == nil {
			_, _, err = con.ReadMessage()
		}
	}()
	pingTimer := time.NewTicker(time.Second)
	defer pingTimer.Stop()
	for {
		select {
		case <-watchCtx.Done():
			return
		case <-pingTimer.C:
			err := con.WriteMessage(ws.TextMessage, []byte(`{"streams":[]}`))
			if err != nil {
				logger.Error(err)
				return
			}
		case str := <-watcher.GetRes():
			err = con.WriteMessage(ws.TextMessage, []byte(str.Str))
			if err != nil {
				logger.Error(err)
				return
			}
		}
	}
}
