package service

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// ExemplarsService answers Prometheus /api/v1/query_exemplars.
//
// Gigapipe records the trace id of a metric data point's first exemplar in the
// sample's string column at ingest. This reads those back, grouped into the
// series they belong to, in the shape Prometheus clients expect.
type ExemplarsService struct {
	model.ServiceData
}

func NewExemplarsService(sd *model.ServiceData) *ExemplarsService {
	return &ExemplarsService{ServiceData: *sd}
}

// exemplarTraceIDLabel is the label Grafana reads a trace id from. It matches
// the OpenTelemetry spelling, which is also what the OTLP ingest path that
// produced these ids uses elsewhere.
const exemplarTraceIDLabel = "trace_id"

// selectorsOf collects every vector selector in a PromQL expression.
//
// Prometheus answers this endpoint for all selectors in the expression, not
// just a bare one, so `rate(a[5m]) / rate(b[5m])` returns the exemplars of
// both a and b.
func selectorsOf(query string) ([][]*labels.Matcher, error) {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(query)
	if err != nil {
		return nil, err
	}
	var res [][]*labels.Matcher
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok {
			res = append(res, vs.LabelMatchers)
		}
		return nil
	})
	if len(res) == 0 {
		return nil, fmt.Errorf("no metric selector in query %q", query)
	}
	return res, nil
}

// matcherOps maps Prometheus matcher types onto the operators the stream
// selector planner understands. They are spelled the same in both languages.
var matcherOps = map[labels.MatchType]string{
	labels.MatchEqual:     "=",
	labels.MatchNotEqual:  "!=",
	labels.MatchRegexp:    "=~",
	labels.MatchNotRegexp: "!~",
}

func fingerprintPlanner(matchers []*labels.Matcher) (shared.SQLRequestPlanner, error) {
	var names, ops, values []string
	for _, m := range matchers {
		op, ok := matcherOps[m.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported matcher %q", m.String())
		}
		names = append(names, m.Name)
		ops = append(ops, op)
		values = append(values, m.Value)
	}
	return &clickhouse_planner.StreamSelectPlanner{
		LabelNames: names,
		Ops:        ops,
		Values:     values,
	}, nil
}

// exemplarJSON renders one exemplar in Prometheus's wire format.
//
// The value is a *quoted* string while the timestamp is a bare number, and
// that asymmetry is not a style choice. Prometheus registers a custom encoder
// for exemplars (web/api/v1/json_codec.go, marshalExemplarJSON) which sends
// the value through jsonutil.MarshalFloat -- that function wraps the number in
// quotes -- and the timestamp through jsonutil.MarshalTimestamp, which renders
// milliseconds as seconds. A client parsing the value strictly as a string
// rejects an unquoted number, and qryn-go already emits the quoted form, so an
// unquoted one puts the two readers on different wire formats.
func exemplarJSON(traceIDLabel, traceID string, value float64, timestampNs int64) string {
	return fmt.Sprintf(`{"labels":{%s:%s},"value":%s,"timestamp":%s}`,
		strconv.Quote(traceIDLabel), strconv.Quote(traceID),
		strconv.Quote(promFloat(value)),
		promTimestamp(timestampNs/int64(time.Millisecond)))
}

// promFloat mirrors jsonutil.MarshalFloat: shortest round-trip form, switching
// to exponent notation at the same bounds Prometheus uses.
func promFloat(f float64) string {
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	return strconv.FormatFloat(f, format, -1, 64)
}

// promTimestamp mirrors jsonutil.MarshalTimestamp: milliseconds rendered as
// seconds, carrying a three-digit fraction only when there is one, so a whole
// second comes out as `1600096945` rather than `1600096945.000`.
func promTimestamp(ms int64) string {
	sign := ""
	if ms < 0 {
		sign = "-"
		ms = -ms
	}
	if frac := ms % 1000; frac != 0 {
		return fmt.Sprintf("%s%d.%03d", sign, ms/1000, frac)
	}
	return fmt.Sprintf("%s%d", sign, ms/1000)
}

func (e *ExemplarsService) QueryExemplars(ctx context.Context, query string,
	start time.Time, end time.Time) (chan string, error) {
	selectors, err := selectorsOf(query)
	if err != nil {
		return nil, err
	}

	fpPlanners := make([]shared.SQLRequestPlanner, len(selectors))
	for i, matchers := range selectors {
		fpPlanners[i], err = fingerprintPlanner(matchers)
		if err != nil {
			return nil, err
		}
	}

	conn, err := e.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	planner := &clickhouse_planner.ExemplarsPlanner{
		Fingerprints: &clickhouse_planner.MultiStreamSelectPlanner{Mains: fpPlanners},
	}

	plannerCtx := shared.PlannerContext{
		IsCluster:   conn.Config.ClusterName != "",
		From:        start,
		To:          end,
		Limit:       10000,
		Ctx:         ctx,
		CHDb:        conn.Session,
		Type:        shared.SAMPLES_TYPE_METRICS,
		VersionInfo: versionInfo,
	}
	tables.PopulateTableNames(&plannerCtx, conn)

	req, err := planner.Process(&plannerCtx)
	if err != nil {
		return nil, err
	}
	strQuery, err := req.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(ctx, strQuery)
	if err != nil {
		return nil, err
	}

	res := make(chan string)
	go func() {
		defer rows.Close()
		defer close(res)

		res <- `{"status":"success","data":[`

		var (
			fingerprint, lastFP uint64
			seriesLabels        string
			traceID             string
			value               float64
			timestampNs         int64
			exemplars           []string
			written             int
		)
		// Rows arrive ordered by fingerprint, so a series can be emitted as
		// soon as the next one starts and nothing has to be buffered beyond
		// the series in hand.
		flush := func(lbls string) {
			if len(exemplars) == 0 {
				return
			}
			if written > 0 {
				res <- ","
			}
			res <- fmt.Sprintf(`{"seriesLabels":%s,"exemplars":[%s]}`,
				lbls, strings.Join(exemplars, ","))
			written++
			exemplars = exemplars[:0]
		}

		lastLabels := ""
		for rows.Next() {
			if err := rows.Scan(&fingerprint, &seriesLabels, &traceID, &value, &timestampNs); err != nil {
				logger.Error(err)
				break
			}
			if fingerprint != lastFP && lastFP != 0 {
				flush(lastLabels)
			}
			lastFP = fingerprint
			lastLabels = seriesLabels
			exemplars = append(exemplars,
				exemplarJSON(exemplarTraceIDLabel, traceID, value, timestampNs))
		}
		flush(lastLabels)

		res <- `]}`
	}()
	return res, nil
}
