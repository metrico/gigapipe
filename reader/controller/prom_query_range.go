package controller

import (
	"fmt"
	"github.com/metrico/qryn/reader/promql/promql_parser"
	"github.com/metrico/qryn/reader/promql/promql_transpiler"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/schema"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/reader/service"
	"github.com/metrico/qryn/reader/utils/logger"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	api_v1 "github.com/prometheus/prometheus/web/api/v1"
)

type PromQueryRangeController struct {
	Controller
	Api     *api_v1.API
	Storage *service.CLokiQueriable
	Stats   bool
}
type QueryRangeProps struct {
	Start time.Time
	End   time.Time
	Query string
	Step  time.Duration
	Raw   struct {
		Start string `form:"start"`
		End   string `form:"end"`
		Query string `form:"query"`
		Step  string `form:"step"`
	}
}

func (q *PromQueryRangeController) QueryRange(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	req, err := parseQueryRangePropsV2(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	req.Start = time.Unix(req.Start.Unix()/15*15, 0)
	req.End = time.Unix(int64(math.Ceil(float64(req.End.Unix())/15)*15), 0)
	if req.Step <= 0 {
		PromError(400,
			"zero or negative query resolution step widths are not accepted. Try a positive integer",
			w)
		return
	}
	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if req.End.Sub(req.Start)/req.Step > 11000 {
		PromError(
			500,
			"exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)",
			w)
		return
	}
	expr, err := promql_parser.Parse(req.Query)
	if err != nil {
		logger.Error("[PQRC004] " + err.Error())
		PromError(400, err.Error(), w)
		return
	}
	if r.Header.Get("X-Experimental") == "1" {
		expr, err = promql_transpiler.TranspileExpressionV2(expr)
		if err != nil {
			logger.Error("[PQRC005] " + err.Error())
			PromError(500, err.Error(), w)
			return
		}
	}
	rangeQuery, err := q.Api.QueryEngine.NewRangeQuery(q.Storage.SetOidAndDB(internalCtx, expr), nil,
		expr.Expr.String(), req.Start, req.End, req.Step)
	if err != nil {
		logger.Error("[PQRC001] " + err.Error())
		PromError(500, err.Error(), w)
		return
	}
	res := rangeQuery.Exec(internalCtx)
	if res.Err != nil {
		logger.Error("[PQRC002] " + res.Err.Error())
		PromError(500, res.Err.Error(), w)
		return
	}
	err = writeResponse(res, w)
	if err != nil {
		logger.Error("[PQRC003] " + err.Error())
		PromError(500, err.Error(), w)
		return
	}
}

func parseQueryRangePropsV2(r *http.Request) (QueryRangeProps, error) {
	res, err := parseQueryRangePropsV3(r)
	if err != nil {
		return res, err
	}
	if res.Query == "" {
		return res, fmt.Errorf("query is undefined")
	}
	if res.Raw.Step == "" {
		return res, fmt.Errorf("step is undefined")
	}
	return res, nil
}

func parseQueryRangePropsV3(r *http.Request) (QueryRangeProps, error) {
	res := QueryRangeProps{}
	var err error
	if r.Method == "POST" && r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
		err = r.ParseForm()
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
	if res.Raw.Query == "" {
		res.Raw.Query = r.URL.Query().Get("query")
	}
	if res.Raw.Step == "" {
		res.Raw.Step = r.URL.Query().Get("step")
	}
	res.Start, err = ParseTimeSecOrRFC(res.Raw.Start, time.Now().Add(time.Hour*-6))
	if err != nil {
		return res, err
	}
	res.End, err = ParseTimeSecOrRFC(res.Raw.End, time.Now())
	if err != nil {
		return res, err
	}
	res.Query = res.Raw.Query
	if res.Raw.Step != "" {
		res.Step, err = parseDuration(res.Raw.Step)
	}
	return res, err
}

func PromError(code int, msg string, w http.ResponseWriter) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")

	json := jsoniter.ConfigFastest
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("status")
	stream.WriteString("error")
	stream.WriteMore()
	stream.WriteObjectField("errorType")
	stream.WriteString("error")
	stream.WriteMore()
	stream.WriteObjectField("error")
	stream.WriteString(msg)
	stream.WriteObjectEnd()

	w.Write(stream.Buffer())
}

func writeResponse(res *promql.Result, w http.ResponseWriter) error {
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
	stream.WriteString(string(res.Value.Type()))
	stream.WriteMore()
	stream.WriteObjectField("result")
	stream.WriteArrayStart()

	_, err := w.Write(stream.Buffer())
	if err != nil {
		return err
	}
	stream.Reset(nil)

	switch res.Value.(type) {
	case promql.Matrix:
		err = writeMatrix(res, w)
	case promql.Vector:
		err = writeVector(res, w)
	case promql.Scalar:
		err = writeScalar(res, w)
	}

	if err != nil {
		return err
	}

	w.Write([]byte("]}}"))
	return nil
}

func writeScalar(res *promql.Result, w http.ResponseWriter) error {
	val := res.Value.(promql.Scalar)
	w.Write([]byte(fmt.Sprintf(`%f, "%f"`, float64(val.T)/1000, val.V)))
	return nil
}

//	func writeMatrix(res *promql.Result, w http.ResponseWriter) error {
//		val := res.Value.(promql.Matrix)
//		for i, s := range val {
//			if i > 0 {
//				w.Write([]byte(","))
//			}
//			w.Write([]byte(`{"metric": {`))
//			for j, v := range s.Metric {
//				if j > 0 {
//					w.Write([]byte(","))
//				}
//				w.Write([]byte(fmt.Sprintf("%s:%s", strconv.Quote(v.Name), strconv.Quote(v.Value))))
//			}
//			w.Write([]byte(`},"values": [`))
//			for j, v := range s.Points {
//				if j > 0 {
//					w.Write([]byte(","))
//				}
//				w.Write([]byte(fmt.Sprintf(`[%f,"%f"]`, float64(v.T)/1000, v.V)))
//			}
//			w.Write([]byte("]}"))
//		}
//		return nil
//	}
func writeMatrix(res *promql.Result, w http.ResponseWriter) error {
	val := res.Value.(promql.Matrix)

	json := jsoniter.ConfigFastest

	for i, s := range val {
		if i > 0 {
			w.Write([]byte(","))
		}

		stream := json.BorrowStream(nil)

		stream.WriteObjectStart()
		stream.WriteObjectField("metric")
		stream.WriteObjectStart()

		for j, v := range s.Metric {
			if j > 0 {
				stream.WriteMore()
			}
			stream.WriteObjectField(v.Name)
			stream.WriteString(v.Value)
		}

		stream.WriteObjectEnd()
		stream.WriteMore()
		stream.WriteObjectField("values")
		stream.WriteArrayStart()

		for j, v := range s.Points {
			if j > 0 {
				stream.WriteMore()
			}
			stream.WriteArrayStart()
			stream.WriteFloat64(float64(v.T) / 1000)
			stream.WriteMore()
			stream.WriteString(strconv.FormatFloat(v.V, 'f', -1, 64))
			stream.WriteArrayEnd()
		}

		stream.WriteArrayEnd()
		stream.WriteObjectEnd()

		w.Write(stream.Buffer())
		json.ReturnStream(stream)
	}

	return nil
}

//func writeVector(res *promql.Result, w http.ResponseWriter) error {
//	val := res.Value.(promql.Vector)
//	for i, s := range val {
//		if i > 0 {
//			w.Write([]byte(","))
//		}
//		w.Write([]byte(`{"metric":{`))
//		for j, lbl := range s.Metric {
//			if j > 0 {
//				w.Write([]byte(","))
//			}
//			w.Write([]byte(fmt.Sprintf("%s:%s", strconv.Quote(lbl.Name), strconv.Quote(lbl.Value))))
//		}
//		w.Write([]byte(fmt.Sprintf(`},"value":[%f,"%f"]}`, float64(s.T/1000), s.V)))
//	}
//	return nil
//}

func writeVector(res *promql.Result, w http.ResponseWriter) error {
	val := res.Value.(promql.Vector)

	json := jsoniter.ConfigFastest

	for i, s := range val {
		if i > 0 {
			w.Write([]byte(","))
		}

		stream := json.BorrowStream(nil)

		stream.WriteObjectStart()
		stream.WriteObjectField("metric")
		stream.WriteObjectStart()

		for j, lbl := range s.Metric {
			if j > 0 {
				stream.WriteMore()
			}
			stream.WriteObjectField(lbl.Name)
			stream.WriteString(lbl.Value)
		}

		stream.WriteObjectEnd()
		stream.WriteMore()
		stream.WriteObjectField("value")
		stream.WriteArrayStart()
		stream.WriteFloat64(float64(s.T) / 1000)
		stream.WriteMore()
		stream.WriteString(strconv.FormatFloat(s.V, 'f', -1, 64))
		stream.WriteArrayEnd()
		stream.WriteObjectEnd()

		w.Write(stream.Buffer())
		json.ReturnStream(stream)
	}

	return nil
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}
