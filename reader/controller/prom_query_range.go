package controller

import (
	encjson "encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler"

	"github.com/go-faster/jx"
	"github.com/gorilla/schema"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
)

type PromQueryRangeController struct {
	Controller
	Engine  promql.QueryEngine
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
	req.Start, req.End = snapQueryRangeToNativeResolution(req.Start, req.End)
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
	// The optimizers push rate/increase/aggregations down into metrics_15s;
	// skip them when the aggregation cannot cover the query window so the
	// engine evaluates the original expression over raw samples instead. The
	// same version info snapshot is passed down to Select so per-selector
	// routing agrees with this decision.
	//
	// A failed probe (nil) also skips them: the substitutes they install are
	// read by Select before its own routing check, so optimizing on an unknown
	// aggregate state can read an aggregate holding no metric rows and return
	// an empty result instead of an error. Raw samples always answer correctly.
	versionInfo := q.Storage.ResolveVersionInfo(internalCtx)
	earliestNS := promql_transpiler.EarliestReadNS(expr.Expr, req.Start)
	if versionInfo != nil && versionInfo.Metrics15sAvailable(earliestNS) {
		expr, err = promql_transpiler.TranspileExpressionV2(expr)
		if err != nil {
			logger.Error("[PQRC005] " + err.Error())
			PromError(500, err.Error(), w)
			return
		}
	}
	queryStorage := q.Storage.SetOidAndDB(internalCtx, expr)
	queryStorage.VersionInfo = versionInfo
	rangeQuery, err := q.Engine.NewRangeQuery(internalCtx, queryStorage, nil,
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

// snapQueryRangeToNativeResolution aligns a query_range window to the
// metrics_15s table's native 15s grid before it is handed to the PromQL
// engine as the literal Start/End of the range query.
//
// Both bounds are floored (rounded towards -Inf), never ceiled: the engine
// evaluates a data point at every Start+k*Step <= End, so rounding End up
// to the next 15s boundary -- as this used to do -- fabricated one extra
// timestamp strictly after the caller's requested end whenever end wasn't
// already a multiple of 15. Flooring both bounds keeps Start <= End and
// guarantees the returned window never extends past what was asked for,
// matching real Prometheus (which never returns a point after `end`).
func snapQueryRangeToNativeResolution(start, end time.Time) (time.Time, time.Time) {
	return time.Unix(start.Unix()/15*15, 0), time.Unix(end.Unix()/15*15, 0)
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	encjson.NewEncoder(w).Encode(map[string]string{
		"status":    "error",
		"errorType": "error",
		"error":     msg,
	})
}

func writeResponse(res *promql.Result, w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")

	stream := &jx.Writer{}

	stream.ObjStart()
	stream.FieldStart("status")
	stream.Str("success")
	stream.Comma()
	stream.FieldStart("data")
	stream.ObjStart()
	stream.FieldStart("resultType")
	stream.Str(string(res.Value.Type()))
	stream.Comma()
	stream.FieldStart("result")
	stream.ArrStart()

	_, err := w.Write(stream.Buf)
	if err != nil {
		return err
	}
	stream.Reset()

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
	stream := &jx.Writer{}

	for i, s := range val {
		if i > 0 {
			w.Write([]byte(","))
		}

		stream.Reset()

		stream.ObjStart()
		stream.FieldStart("metric")
		stream.ObjStart()

		j := 0
		for name, value := range s.Metric.Map() {
			if j > 0 {
				stream.Comma()
			}
			stream.FieldStart(name)
			stream.Str(value)
			j++
		}

		stream.ObjEnd()
		stream.Comma()
		stream.FieldStart("values")
		stream.ArrStart()

		for j, v := range s.Floats {
			if j > 0 {
				stream.Comma()
			}
			stream.ArrStart()
			stream.Float64(float64(v.T) / 1000)
			stream.Comma()
			stream.Str(strconv.FormatFloat(v.F, 'f', -1, 64))
			stream.ArrEnd()
		}

		stream.ArrEnd()
		stream.ObjEnd()

		w.Write(stream.Buf)
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
	stream := &jx.Writer{}

	for i, s := range val {
		if i > 0 {
			w.Write([]byte(","))
		}

		stream.Reset()

		stream.ObjStart()
		stream.FieldStart("metric")
		stream.ObjStart()

		j := 0
		for name, value := range s.Metric.Map() {
			if j > 0 {
				stream.Comma()
			}
			stream.FieldStart(name)
			stream.Str(value)
			j++
		}

		stream.ObjEnd()
		stream.Comma()
		stream.FieldStart("value")
		stream.ArrStart()
		stream.Float64(float64(s.T) / 1000)
		stream.Comma()
		stream.Str(strconv.FormatFloat(s.F, 'f', -1, 64))
		stream.ArrEnd()
		stream.ObjEnd()

		w.Write(stream.Buf)
	}

	return nil
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
