package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/reader/utils/smart_buffer"
)

func getRequiredFloat(ctx *http.Request, name string, def string, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	strRes := ctx.URL.Query().Get(name)
	if strRes == "" {
		strRes = def
	}
	if strRes == "" {
		return 0, fmt.Errorf("%s parameter is required", name)
	}
	iRes, err := strconv.ParseFloat(strRes, 64)
	return iRes, err
}

func getRequiredDuration(ctx *http.Request, name string, def string, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	strRes := ctx.URL.Query().Get(name)
	if strRes == "" {
		strRes = def
	}
	if strRes == "" {
		return 0, fmt.Errorf("%s parameter is required", name)
	}
	duration, err := parseDuration(strRes)
	return float64(duration.Nanoseconds()) / 1e9, err
}

func getRequiredI64(ctx *http.Request, name string, def string, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	strRes := ctx.URL.Query().Get(name)
	if strRes == "" {
		strRes = def
	}
	if strRes == "" {
		return 0, fmt.Errorf("%s parameter is required", name)
	}
	iRes, err := strconv.ParseInt(strRes, 10, 64)
	return iRes, err
}

// Epoch magnitude thresholds. Each value is the epoch of ~1973-03-03 expressed
// in the corresponding unit, so any realistic timestamp (past or future) maps to
// exactly one unit and the ranges never overlap.
const (
	epochMilliMin = int64(1e11)
	epochMicroMin = int64(1e14)
	epochNanoMin  = int64(1e17)
)

var numericEpoch = regexp.MustCompile(`^[0-9.]+$`)

// ParseTimeSecOrRFC parses a time parameter that may be an RFC3339 string or a
// numeric UNIX epoch. Numeric epochs are disambiguated by magnitude: Prometheus
// clients send (fractional) seconds while Loki clients (e.g. Grafana Logs
// Drilldown) send nanoseconds. Treating nanoseconds as seconds produced
// astronomically distant time ranges, causing full table scans and
// "makeslice: len out of range" panics in downstream planners.
func ParseTimeSecOrRFC(raw string, def time.Time) (time.Time, error) {
	if raw == "" {
		return def, nil
	}
	if !numericEpoch.MatchString(raw) {
		return time.Parse(time.RFC3339, raw)
	}
	// Integer epochs are parsed exactly: nanosecond values exceed the 53-bit
	// mantissa of a float64, so a float round-trip would lose precision.
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return epochToTime(i, 0), nil
	}
	t, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return def, err
	}
	sec, frac := math.Modf(t)
	return epochToTime(int64(sec), frac), nil
}

// epochToTime converts a UNIX epoch of unknown resolution (seconds, milli-,
// micro- or nanoseconds), plus an optional fractional part of the same unit,
// into a time.Time. Shared by the Loki, Prometheus and Tempo query paths.
func epochToTime(v int64, frac float64) time.Time {
	switch {
	case v >= epochNanoMin:
		return time.Unix(v/1e9, v%1e9)
	case v >= epochMicroMin:
		return time.Unix(v/1e6, v%1e6*1e3+int64(frac*1e3))
	case v >= epochMilliMin:
		return time.Unix(v/1e3, v%1e3*1e6+int64(frac*1e6))
	default:
		return time.Unix(v, int64(frac*1e9))
	}
}

func tamePanic(w http.ResponseWriter, r *http.Request) {
	if err := recover(); err != nil {
		logger.Error("panic:", err, " stack:", string(debug.Stack()))
		logger.Error("query: ", r.URL.String())
		w.WriteHeader(500)
		w.Write([]byte("Internal Server Error"))
		recover()
	}
}

// writeEmptyIfDenied short-circuits federated reads whose tenant is
// absent/invalid: since every stored row has a non-empty oid, such a request
// can only match nothing, so we return the language's canonical empty payload
// without touching ClickHouse. Returns true when it handled the response.
//
// resultType is the Prometheus/Loki resultType for the empty data envelope
// ("streams", "matrix", "vector"); pass "" for the labels/series/values shape
// whose data is a bare empty array.
func writeEmptyIfDenied(w http.ResponseWriter, ctx context.Context, resultType string) bool {
	if !shared.OidFilterFromContext(ctx).Deny {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if resultType == "" {
		w.Write([]byte(`{"status":"success","data":[]}`))
		return true
	}
	w.Write([]byte(`{"status":"success","data":{"resultType":"` + resultType + `","result":[]}}`))
	return true
}

func RunPreRequestPlugins(r *http.Request) (context.Context, error) {
	ctx := r.Context()
	for _, plugin := range plugins.GetPreRequestPlugins() {
		_ctx, err := plugin(ctx, r)
		if err == nil {
			ctx = _ctx
			continue
		}
		if errors.Is(err, plugins.ErrPluginNotApplicable) {
			continue
		}
		return nil, err
	}
	return ctx, nil
}

func runPreWSRequestPlugins(ctx context.Context, r *http.Request) (context.Context, error) {
	for _, plugin := range plugins.GetPreWSRequestPlugins() {
		_ctx, err := plugin(ctx, r)
		if err == nil {
			ctx = _ctx
			continue
		}
		if errors.Is(err, plugins.ErrPluginNotApplicable) {
			continue
		}
		return nil, err
	}
	return ctx, nil
}

// SmartBufferServe buffers data from a QueryRangeOutput channel using SmartBuffer,
// checking for errors in the output before sending the response.
func SmartBufferServe(w http.ResponseWriter, ch <-chan model.QueryRangeOutput) {
	buffer := smart_buffer.New()
	defer buffer.Close()

	for output := range ch {
		if output.Err != nil {
			PromError(500, output.Err.Error(), w)
			return
		}
		_, err := buffer.Write([]byte(output.Str))
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	io.Copy(w, buffer)
}

// SmartBufferServeStrings buffers data from a string channel using SmartBuffer.
func SmartBufferServeStrings(w http.ResponseWriter, ch <-chan string) {
	buffer := smart_buffer.New()
	defer buffer.Close()

	for str := range ch {
		_, err := buffer.Write([]byte(str))
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	io.Copy(w, buffer)
}
