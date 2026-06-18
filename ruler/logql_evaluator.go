package ruler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v4/reader/service"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

var vectorRe = regexp.MustCompile(`(?i)^\s*vector\(\s*([0-9.]+)\s*\)\s*$`)

const maxLogQLResultBytes = 10 * 1024 * 1024

// LogQLEvaluator evaluates LogQL recording-rule expressions via the reader's
// instant query service, converting the JSON response into a Prometheus vector.
type LogQLEvaluator struct {
	queryRangeService *service.QueryRangeService
}

// NewLogQLEvaluator builds a LogQL evaluator over the reader's query service.
func NewLogQLEvaluator(queryRangeService *service.QueryRangeService) *LogQLEvaluator {
	return &LogQLEvaluator{queryRangeService: queryRangeService}
}

// Evaluate runs query as an instant LogQL query at t. The synthetic constant
// vector(N) is answered directly without hitting the backend.
func (e *LogQLEvaluator) Evaluate(ctx context.Context, query string, t time.Time) (promql.Vector, error) {
	if query == "" {
		return nil, errors.New("query expression cannot be empty")
	}

	if m := vectorRe.FindStringSubmatch(query); m != nil {
		val, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return nil, fmt.Errorf("vector(): invalid value %q: %w", m[1], err)
		}
		return promql.Vector{{Metric: labels.EmptyLabels(), T: t.UnixMilli(), F: val}}, nil
	}

	outputChan, err := e.queryRangeService.QueryInstant(ctx, query, t.UnixNano(), 1000, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to execute LogQL query: %w", err)
	}

	var buf strings.Builder
	for output := range outputChan {
		if output.Err != nil {
			return nil, fmt.Errorf("query execution error: %w", output.Err)
		}
		if output.Str == "" {
			continue
		}
		if buf.Len()+len(output.Str) > maxLogQLResultBytes {
			return nil, fmt.Errorf("query result exceeded %d bytes", maxLogQLResultBytes)
		}
		buf.WriteString(output.Str)
	}

	return parseInstantVector(buf.String())
}

// instantResponse is the relevant shape of the reader's instant-query JSON.
type instantResponse struct {
	Data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"` // [seconds float, "value" string]
		} `json:"result"`
	} `json:"data"`
}

// parseInstantVector converts the reader's instant-query JSON into a vector.
func parseInstantVector(jsonStr string) (promql.Vector, error) {
	if strings.TrimSpace(jsonStr) == "" {
		return promql.Vector{}, nil
	}
	var resp instantResponse
	if err := json.Unmarshal([]byte(jsonStr), &resp); err != nil {
		return nil, fmt.Errorf("invalid query result JSON: %w", err)
	}

	var result promql.Vector
	for _, item := range resp.Data.Result {
		sample := promql.Sample{Metric: labels.FromMap(item.Metric)}
		if ts, ok := item.Value[0].(float64); ok {
			sample.T = int64(ts * 1000) // seconds → milliseconds
		}
		if val, ok := item.Value[1].(string); ok {
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid sample value %q: %w", val, err)
			}
			sample.F = f
		}
		result = append(result, sample)
	}
	return result, nil
}
