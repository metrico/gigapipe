package traceql_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"strconv"
)

type MetricsProcessor struct {
	main shared.SQLRequestPlanner
	fn   string
}

func (m *MetricsProcessor) Process(context *shared.PlannerContext) (model.TraceQLResponse, error) {
	req, err := m.main.Process(context)
	if err != nil {
		return model.TraceQLResponse{}, err
	}

	strReq, err := req.String(sql.DefaultCtx())
	if err != nil {
		return model.TraceQLResponse{}, err
	}

	println(strReq)

	rows, err := context.CHDb.QueryCtx(context.Ctx, strReq)
	if err != nil {
		return model.TraceQLResponse{}, err
	}

	c := make(chan model.TraceMetricStream)
	go func() {
		defer close(c)
		metric := model.TraceMetricStream{
			Labels: []model.SpanAttr{
				{"__name__", model.SpanAttrValue{m.fn}},
			},
		}
		for rows.Next() {
			var (
				timestampMs int64
				value       float64
			)
			rows.Scan(&timestampMs, &value)
			spl := model.TraceMetricSample{
				TimestampMs: strconv.FormatInt(timestampMs, 10),
				Value:       value,
			}
			metric.Samples = append(metric.Samples, spl)
		}
		c <- metric
	}()

	return model.TraceQLResponse{
		Metrics: c,
	}, nil
}
