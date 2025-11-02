package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/metrico/qryn/v4/logger"
	"github.com/metrico/qryn/v4/reader/plugins"
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

func ParseTimeSecOrRFC(raw string, def time.Time) (time.Time, error) {
	if raw == "" {
		return def, nil
	}
	if regexp.MustCompile("^[0-9.]+$").MatchString(raw) {
		t, _ := strconv.ParseFloat(raw, 64)
		return time.Unix(int64(t), 0), nil
	}
	return time.Parse(time.RFC3339, raw)
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
