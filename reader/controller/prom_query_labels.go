package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	readermodel "github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/jsonutil"
)

// PromQueryLabelsController serves the Prometheus label, series, metadata and exemplar
// endpoints from the metric series index.
type PromQueryLabelsController struct {
	Controller
	MetricLabelsService *service.MetricLabelsService
}

const truncatedWarning = "results truncated due to limit"

var promParser = parser.NewParser(parser.Options{})

func (p *PromQueryLabelsController) PromLabels(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	ctx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	q, err := indexQuery(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	names, truncated, err := p.MetricLabelsService.LabelNames(ctx, q)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	promRespond(w, names, truncated)
}

func (p *PromQueryLabelsController) LabelValues(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	ctx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	name := mux.Vars(r)["name"]
	if strings.HasPrefix(name, "U__") {
		name = model.UnescapeName(name, model.ValueEncodingEscaping)
	}
	if !model.UTF8Validation.IsValidLabelName(name) {
		PromError(400, fmt.Sprintf("invalid label name: %q", name), w)
		return
	}
	q, err := indexQuery(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	values, truncated, err := p.MetricLabelsService.LabelValues(ctx, name, q)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	promRespond(w, values, truncated)
}

func (p *PromQueryLabelsController) Series(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	ctx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	q, err := indexQuery(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	if len(q.Selectors) == 0 {
		PromError(400, "no match[] parameter provided", w)
		return
	}
	series, truncated, err := p.MetricLabelsService.Series(ctx, q)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	promRespond(w, series, truncated)
}

// Metadata answers with one entry per family; limit counts families and limit_per_metric is
// accepted with no effect.
func (p *PromQueryLabelsController) Metadata(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	ctx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	if err := r.ParseForm(); err != nil {
		PromError(400, err.Error(), w)
		return
	}
	limit := -1
	if s := r.Form.Get("limit"); s != "" {
		if limit, err = strconv.Atoi(s); err != nil {
			PromError(400, "limit must be a number", w)
			return
		}
	}
	if s := r.Form.Get("limit_per_metric"); s != "" {
		if _, err := strconv.Atoi(s); err != nil {
			PromError(400, "limit_per_metric must be a number", w)
			return
		}
	}
	res, err := p.MetricLabelsService.Metadata(ctx, r.Form.Get("metric"), limit)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	promRespond(w, res, false)
}

// QueryExemplars answers with the exemplars in [start, end] of the series the query's
// selectors pick.
func (p *PromQueryLabelsController) QueryExemplars(w http.ResponseWriter, r *http.Request) {
	defer tamePanic(w, r)
	ctx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	q, err := exemplarQuery(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}
	if len(q.Selectors) == 0 {
		promRespond(w, nil, false)
		return
	}
	res, err := p.MetricLabelsService.Exemplars(ctx, q)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(marshalExemplars(res))
}

// exemplarQuery reads the query, start and end parameters of /api/v1/query_exemplars.
func exemplarQuery(r *http.Request) (readermodel.MetricIndexQuery, error) {
	var q readermodel.MetricIndexQuery
	if err := r.ParseForm(); err != nil {
		return q, err
	}
	var err error
	if q.StartMs, err = optionalTimeMs(r.Form.Get("start"), "start"); err != nil {
		return q, err
	}
	if q.EndMs, err = optionalTimeMs(r.Form.Get("end"), "end"); err != nil {
		return q, err
	}
	if q.StartMs != nil && q.EndMs != nil && *q.EndMs < *q.StartMs {
		return q, errors.New("end timestamp must not be before start timestamp")
	}
	expr, err := promParser.ParseExpr(r.Form.Get("query"))
	if err != nil {
		return q, err
	}
	q.Selectors = parser.ExtractSelectors(expr)
	return q, nil
}

// marshalExemplars writes Prometheus's exemplar response: values as strings, timestamps as
// seconds with a millisecond fraction.
func marshalExemplars(res []readermodel.ExemplarSeries) []byte {
	stream := jsoniter.ConfigCompatibleWithStandardLibrary.BorrowStream(nil)
	defer jsoniter.ConfigCompatibleWithStandardLibrary.ReturnStream(stream)
	stream.WriteRaw(`{"status":"success","data":[`)
	for i, s := range res {
		if i > 0 {
			stream.WriteMore()
		}
		stream.WriteRaw(`{"seriesLabels":`)
		stream.WriteVal(s.SeriesLabels)
		stream.WriteRaw(`,"exemplars":[`)
		for j, e := range s.Exemplars {
			if j > 0 {
				stream.WriteMore()
			}
			stream.WriteRaw(`{"labels":`)
			stream.WriteVal(e.Labels)
			stream.WriteRaw(`,"value":`)
			jsonutil.MarshalFloat(e.Value, stream)
			stream.WriteRaw(`,"timestamp":`)
			jsonutil.MarshalTimestamp(e.TimestampMs, stream)
			stream.WriteObjectEnd()
		}
		stream.WriteRaw(`]}`)
	}
	stream.WriteRaw(`]}`)
	return append([]byte(nil), stream.Buffer()...)
}

// promRespond writes data in Prometheus's success envelope, with the truncation warning
// when truncated.
func promRespond(w http.ResponseWriter, data any, truncated bool) {
	res := struct {
		Status   string   `json:"status"`
		Data     any      `json:"data"`
		Warnings []string `json:"warnings,omitempty"`
	}{Status: "success", Data: data}
	if truncated {
		res.Warnings = []string{truncatedWarning}
	}
	body, err := json.Marshal(res)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(body)
}

// indexQuery reads the match[], start, end and limit parameters of a label endpoint.
func indexQuery(r *http.Request) (readermodel.MetricIndexQuery, error) {
	var q readermodel.MetricIndexQuery
	if err := r.ParseForm(); err != nil {
		return q, err
	}
	var err error
	if q.Limit, err = parseLimit(r.Form.Get("limit")); err != nil {
		return q, err
	}
	if q.StartMs, err = optionalTimeMs(r.Form.Get("start"), "start"); err != nil {
		return q, err
	}
	if q.EndMs, err = optionalTimeMs(r.Form.Get("end"), "end"); err != nil {
		return q, err
	}
	q.Selectors, err = parseMatchers(r.Form["match[]"])
	return q, err
}

// parseMatchers parses match[] selectors; each needs a matcher that rejects the empty string.
func parseMatchers(matches []string) ([][]*labels.Matcher, error) {
	var res [][]*labels.Matcher
	for _, m := range matches {
		sel, err := promParser.ParseMetricSelector(m)
		if err != nil {
			return nil, err
		}
		if !hasNonEmptyMatcher(sel) {
			return nil, errors.New("match[] must contain at least one non-empty matcher")
		}
		res = append(res, sel)
	}
	return res, nil
}

func hasNonEmptyMatcher(sel []*labels.Matcher) bool {
	for _, m := range sel {
		if !m.Matches("") {
			return true
		}
	}
	return false
}

// parseLimit reads a non-negative limit; 0 or absent is no limit.
func parseLimit(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid parameter %q: %w", "limit", err)
	}
	if limit < 0 {
		return 0, fmt.Errorf("invalid parameter %q: limit must be non-negative", "limit")
	}
	return limit, nil
}

// Prometheus's formatted MinTime and MaxTime, which clients send for an open bound.
const (
	promMinTime = "-292273086-05-16T16:47:06Z"
	promMaxTime = "292277025-08-18T07:12:54.999999999Z"
)

// optionalTimeMs parses a time with ParseTimeSecOrRFC into unix milliseconds; an empty value
// or Prometheus's MinTime or MaxTime is nil.
func optionalTimeMs(s string, name string) (*int64, error) {
	if s == "" || s == promMinTime || s == promMaxTime {
		return nil, nil
	}
	t, err := ParseTimeSecOrRFC(s, time.Time{})
	if err != nil {
		return nil, fmt.Errorf("invalid parameter %q: invalid time value for '%s': cannot parse %q to a valid timestamp",
			name, name, s)
	}
	ms := t.UnixMilli()
	return &ms, nil
}
