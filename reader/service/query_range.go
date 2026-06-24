package service

import (
	"context"
	databaseSql "database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/plugins"
	dbversion "github.com/metrico/qryn/v4/reader/utils/dbVersion"
	"github.com/metrico/qryn/v4/reader/utils/logger"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/metrico/qryn/v4/reader/utils/tables"
)

const (
	tailPollInterval     = time.Second // how often new entries are fetched from ClickHouse
	tailIncrementalLimit = 1000        // max entries per poll tick
	tailWatcherBufSize   = 10          // channel buffer between poller and WebSocket handler
)

type QueryRangeService struct {
	model.ServiceData
	plugin plugins.QueryRangeServicePlugin
}

func NewQueryRangeService(data *model.ServiceData) *QueryRangeService {
	res := &QueryRangeService{
		ServiceData: *data,
	}
	p := plugins.GetQueryRangeServicePlugin()
	if p != nil {
		(*p).SetServiceData(data)
		res.plugin = *p
	}
	return res
}

func onErr(err error, res chan model.QueryRangeOutput) {
	logger.Error(err)
	res <- model.QueryRangeOutput{
		Str: "]}}",
		Err: err,
	}
}

func (q *QueryRangeService) exportStreamsValue(out chan []shared.LogEntry,
	res chan model.QueryRangeOutput,
) {
	defer close(res)

	json := jsoniter.ConfigFastest
	stream := json.BorrowStream(nil)
	defer json.ReturnStream(stream)

	// Write initial part of response
	stream.WriteObjectStart()
	stream.WriteObjectField("status")
	stream.WriteString("success")
	stream.WriteMore()
	stream.WriteObjectField("data")
	stream.WriteObjectStart()
	stream.WriteObjectField("resultType")
	stream.WriteString("streams")
	stream.WriteMore()
	stream.WriteObjectField("result")
	stream.WriteArrayStart()

	res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
	stream.Reset(nil)

	var lastFp uint64
	i := 0
	j := 0

	for entries := range out {
		for _, e := range entries {
			if e.Err == io.EOF {
				continue
			}
			if e.Err != nil {
				onErr(e.Err, res)
				return
			}
			if lastFp != e.Fingerprint {
				if i > 0 {
					// Close previous stream entry
					stream.WriteArrayEnd()
					stream.WriteObjectEnd()
					stream.WriteMore()
					res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
					stream.Reset(nil)
				}
				lastFp = e.Fingerprint
				i = 1
				j = 0

				// Write new stream entry
				stream.WriteObjectStart()
				stream.WriteObjectField("stream")
				writeMap(stream, e.Labels)
				stream.WriteMore()
				stream.WriteObjectField("values")
				stream.WriteArrayStart()
			}
			if j > 0 {
				stream.WriteMore()
			}
			j = 1

			// Write value entry
			stream.WriteArrayStart()
			stream.WriteString(fmt.Sprintf("%d", e.TimestampNS))
			stream.WriteMore()
			stream.WriteString(e.Message)
			stream.WriteArrayEnd()

			res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
			stream.Reset(nil)
		}
	}

	if i > 0 {
		// Close last stream entry
		stream.WriteArrayEnd()
		stream.WriteObjectEnd()
	}

	// Close result array and response object
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()
	stream.WriteObjectEnd()

	res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
}

func (q *QueryRangeService) getLabelsForVolume(query string) ([]string, error) {
	script, err := logql_parser.Parse(query)
	if err != nil {
		return nil, err
	}
	if script.Head.StrSelector == nil {
		return nil, fmt.Errorf("invalid query: %s", query)
	}
	labels := make([]string, len(script.Head.StrSelector.StrSelCmds))
	for i, cmd := range script.Head.StrSelector.StrSelCmds {
		labels[i] = cmd.Label.Name
	}
	return labels, nil
}

type QueryVolumeResult struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
}

func (q *QueryRangeService) QueryVolume(ctx context.Context, query string, fromNs int64, toNs int64,
	stepMs int64, aggregateByLabels []string,
) ([]QueryVolumeResult, error) {
	var err error
	if len(aggregateByLabels) == 0 {
		aggregateByLabels, err = q.getLabelsForVolume(query)
		if err != nil {
			return nil, err
		}
	}
	req := fmt.Sprintf("sum(bytes_over_time(%s [%dms])) by (%s)", query, stepMs,
		strings.Join(aggregateByLabels, ","))
	c, _, err := q.prepareOutput(ctx, req, fromNs, toNs, stepMs, 1000, true)
	if err != nil {
		return nil, err
	}
	res := []QueryVolumeResult{}

	lastFp := uint64(0)
	lastMetric := make(map[string]string)
	value := float64(0)
	putData := func() {
		metric := make(map[string]string)
		for k, v := range lastMetric {
			metric[k] = v
		}
		res = append(res, QueryVolumeResult{
			Metric: metric,
			Value:  []any{float64(toNs / 1000000000), strconv.FormatFloat(value, 'f', -1, 32)},
		})
	}
	for p := range c {
		for _, e := range p {
			if e.Fingerprint != lastFp {
				if lastFp != 0 {
					putData()
				}
				lastMetric = e.Labels
				value = 0
				lastFp = e.Fingerprint
			}
			value += e.Value
		}
	}
	if lastFp != 0 {
		putData()
	}

	return res, nil
}

type QueryDetectedLabelsResult struct {
	Label       string `json:"label"`
	Cardinality int64  `json:"cardinality"`
}

func (q *QueryRangeService) QueryDetectedLabels(ctx context.Context, query string, fromNs int64,
	toNs int64,
) ([]QueryDetectedLabelsResult, error) {
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	var script *logql_parser.LogQLScript
	if query != "" {
		script, err = logql_parser.Parse(query)
		if err != nil {
			return nil, err
		}
	}

	_ctx, cancel := context.WithCancel(ctx)
	plannerCtx := tables.PopulateTableNames(&shared.PlannerContext{
		IsCluster:  conn.Config.ClusterName != "",
		From:       time.Unix(fromNs/1000000000, 0),
		To:         time.Unix(toNs/1000000000, 0),
		Ctx:        _ctx,
		CancelCtx:  cancel,
		CHDb:       conn.Session,
		CHFinalize: true,
		CHSqlCtx: &sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		},
		VersionInfo: versionInfo,
	}, conn)

	sqlReq, err := logql_transpiler.PlanDetectLabels(script)
	if err != nil {
		return nil, err
	}
	objReq, err := sqlReq.Process(plannerCtx)
	if err != nil {
		return nil, err
	}
	var opts []int
	if plannerCtx.IsCluster {
		opts = append(opts, sql.STRING_OPT_INLINE_WITH)
	}
	strReq, err := objReq.String(plannerCtx.CHSqlCtx, opts...)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Session.QueryCtx(_ctx, strReq)
	if err != nil {
		return nil, err
	}
	var res []QueryDetectedLabelsResult
	for rows.Next() {
		var label string
		var cardinality int64
		err = rows.Scan(&label, &cardinality)
		if err != nil {
			return nil, err
		}
		res = append(res, QueryDetectedLabelsResult{
			Label:       label,
			Cardinality: cardinality,
		})
	}
	return res, nil
}

type PatternsResult struct {
	Pattern string     `json:"pattern"`
	Samples [][2]int32 `json:"samples"`
}

func (q *QueryRangeService) QueryPatterns(ctx context.Context, query string, fromNs int64, toNs int64,
	stepMs int64, limit int64,
) ([]PatternsResult, error) {
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	var script *logql_parser.LogQLScript
	script, err = logql_parser.Parse(query)
	if err != nil {
		return nil, err
	}

	_ctx, cancel := context.WithCancel(ctx)
	plannerCtx := tables.PopulateTableNames(&shared.PlannerContext{
		IsCluster:  conn.Config.ClusterName != "",
		From:       time.Unix(fromNs/1000000000, 0),
		To:         time.Unix(toNs/1000000000, 0),
		Step:       time.Millisecond * time.Duration(stepMs),
		Ctx:        _ctx,
		CancelCtx:  cancel,
		CHDb:       conn.Session,
		CHFinalize: true,
		CHSqlCtx: &sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		},
		VersionInfo: versionInfo,
		Limit:       limit,
	}, conn)

	sqlReq, err := logql_transpiler.PlanPatterns(script)
	if err != nil {
		return nil, err
	}
	objReq, err := sqlReq.Process(plannerCtx)
	if err != nil {
		return nil, err
	}
	var opts []int
	if plannerCtx.IsCluster {
		opts = append(opts, sql.STRING_OPT_INLINE_WITH)
	}
	strReq, err := objReq.String(plannerCtx.CHSqlCtx, opts...)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Session.QueryCtx(_ctx, strReq)
	if err != nil {
		return nil, err
	}
	var res []PatternsResult
	for rows.Next() {
		_pattern, err := q.scan(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, _pattern)
	}
	return res, nil
}

func (q *QueryRangeService) buildPattern(pattern []string) string {
	patternBld := strings.Builder{}
	for i, p := range pattern {
		if p == "<_>" && i > 0 && pattern[i-1] == "<_>" {
			continue
		}
		patternBld.WriteString(p)
	}
	return patternBld.String()
}

func (q *QueryRangeService) scan(rows *databaseSql.Rows) (PatternsResult, error) {
	var pattern []string
	var samplesV1 []map[string]any
	var samplesV2 [][]any
	err := rows.Scan(&pattern, &samplesV1)
	if err == nil {
		res := PatternsResult{
			Pattern: q.buildPattern(pattern),
		}
		for _, s := range samplesV1 {
			res.Samples = append(res.Samples, [2]int32{
				int32(s["timestamp_s"].(uint64)),
				int32(s["count"].(uint64)),
			})
		}
		return res, nil
	}
	if !strings.Contains(
		err.Error(),
		"storing driver.Value type [][]interface {} into type *[]map[string]interface {}") {
		return PatternsResult{}, err
	}
	err = rows.Scan(&pattern, &samplesV2)
	if err != nil {
		return PatternsResult{}, err
	}
	res := PatternsResult{
		Pattern: q.buildPattern(pattern),
	}
	for _, s := range samplesV2 {
		res.Samples = append(res.Samples, [2]int32{
			int32(s[0].(uint64)),
			int32(s[1].(uint64)),
		})
	}
	return res, nil
}

func (q *QueryRangeService) QueryRange(ctx context.Context, query string, fromNs int64, toNs int64, stepMs int64,
	limit int64, forward bool,
) (chan model.QueryRangeOutput, error) {
	out, isMatrix, err := q.prepareOutput(ctx, query, fromNs, toNs, stepMs, limit, forward)
	if err != nil {
		return nil, err
	}
	res := make(chan model.QueryRangeOutput)

	if !isMatrix {
		go func() {
			q.exportStreamsValue(out, res)
		}()
		return res, nil
	}
	go func() {
		defer close(res)

		json := jsoniter.ConfigFastest
		stream := json.BorrowStream(nil)
		defer json.ReturnStream(stream)

		// Write initial part of response
		stream.WriteObjectStart()
		stream.WriteObjectField("status")
		stream.WriteString("success")
		stream.WriteMore()
		stream.WriteObjectField("data")
		stream.WriteObjectStart()
		stream.WriteObjectField("resultType")
		stream.WriteString("matrix")
		stream.WriteMore()
		stream.WriteObjectField("result")
		stream.WriteArrayStart()

		res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
		stream.Reset(nil)

		var lastFp uint64
		i := 0
		j := 0

		for entries := range out {
			for _, e := range entries {
				if e.Err != nil && e.Err != io.EOF {
					onErr(e.Err, res)
					return
				}
				if e.Err == io.EOF {
					break
				}
				if i == 0 || lastFp != e.Fingerprint {
					if i > 0 {

						//]},
						// Close previous metric entry
						stream.WriteArrayEnd()
						stream.WriteObjectEnd()
						stream.WriteMore()
						res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
						stream.Reset(nil)
					}
					lastFp = e.Fingerprint
					i = 1
					j = 0

					// Write new metric entry
					stream.WriteObjectStart()
					stream.WriteObjectField("metric")
					writeMap(stream, e.Labels)
					stream.WriteMore()
					stream.WriteObjectField("values")
					stream.WriteArrayStart()
				}
				if j > 0 {
					stream.WriteMore()
				}
				j = 1

				// Format value
				val := strconv.FormatFloat(e.Value, 'f', -1, 64)
				if strings.Contains(val, ".") {
					val = strings.TrimSuffix(val, "0")
					val = strings.TrimSuffix(val, ".")
				}

				// Write value entry
				stream.WriteArrayStart()
				// Intentional WriteRaw to fix precision in response
				stream.WriteRaw(fmt.Sprintf("%f", float64(e.TimestampNS)/1e9))
				stream.WriteMore()
				stream.WriteString(val)
				stream.WriteArrayEnd()

				res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
				stream.Reset(nil)
			}
		}

		if i > 0 {
			// Close last metric entry
			stream.WriteArrayEnd()
			stream.WriteObjectEnd()
		}

		// Close result array and response object
		stream.WriteArrayEnd()
		stream.WriteObjectEnd()
		stream.WriteObjectEnd()

		res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
	}()
	return res, nil
}

func (q *QueryRangeService) prepareOutput(ctx context.Context, query string, fromNs int64, toNs int64, stepMs int64,
	limit int64, forward bool,
) (chan []shared.LogEntry, bool, error) {
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, false, err
	}
	chain, err := logql_transpiler.Transpile(query)
	if err != nil {
		return nil, false, err
	}
	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, false, err
	}

	_ctx, cancel := context.WithCancel(ctx)

	plannerCtx := tables.PopulateTableNames(&shared.PlannerContext{
		IsCluster:  conn.Config.ClusterName != "",
		From:       time.Unix(fromNs/1000000000, 0),
		To:         time.Unix(toNs/1000000000, 0),
		OrderASC:   forward,
		Limit:      int64(limit),
		Ctx:        _ctx,
		CancelCtx:  cancel,
		CHDb:       conn.Session,
		CHFinalize: true,
		Step:       time.Duration(stepMs) * time.Millisecond,
		CHSqlCtx: &sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		},
		VersionInfo: versionInfo,
	}, conn)
	res, err := chain[0].Process(plannerCtx, nil)
	return res, chain[0].IsMatrix(), err
}

func (q *QueryRangeService) QueryInstant(ctx context.Context, query string, timeNs int64, stepMs int64,
	limit int64,
) (chan model.QueryRangeOutput, error) {
	out, isMatrix, err := q.prepareOutput(ctx, query, timeNs-300000000000, timeNs, stepMs, limit, false)
	if err != nil {
		return nil, err
	}
	res := make(chan model.QueryRangeOutput)
	if !isMatrix {
		go func() {
			q.exportStreamsValue(out, res)
		}()
		return res, nil
	}

	go func() {
		defer close(res)
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

		res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
		stream.Reset(nil)
		i := 0
		lastValues := make(map[uint64]shared.LogEntry)
		for entries := range out {
			for _, e := range entries {
				if e.Err != nil && e.Err != io.EOF {
					onErr(e.Err, res)
					return
				}
				if e.Err == io.EOF {
					break
				}
				if _, ok := lastValues[e.Fingerprint]; !ok {
					lastValues[e.Fingerprint] = e
					continue
				}
				if lastValues[e.Fingerprint].TimestampNS < e.TimestampNS {
					lastValues[e.Fingerprint] = e
					continue
				}
			}
		}
		for _, e := range lastValues {
			if i > 0 {
				stream.WriteMore()
			}
			stream.WriteObjectStart()
			stream.WriteObjectField("metric")
			stream.WriteObjectStart()
			j := 0
			for k, v := range e.Labels {
				if j > 0 {
					stream.WriteMore()
				}
				stream.WriteObjectField(k)
				stream.WriteString(v)
				j++
			}
			stream.WriteObjectEnd()
			stream.WriteMore()

			val := strconv.FormatFloat(e.Value, 'f', -1, 64)
			if strings.Contains(val, ".") {
				val = strings.TrimSuffix(val, "0")
				val = strings.TrimSuffix(val, ".")
			}

			stream.WriteObjectField("value")
			stream.WriteArrayStart()
			stream.WriteInt64(e.TimestampNS / 1000000000)
			stream.WriteMore()
			stream.WriteString(val)
			stream.WriteArrayEnd()
			stream.WriteObjectEnd()
			res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
			stream.Reset(nil)
			i++
		}
		stream.WriteArrayEnd()
		stream.WriteObjectEnd()
		stream.WriteObjectEnd()
		res <- model.QueryRangeOutput{Str: string(stream.Buffer())}
	}()

	return res, nil
}

func (q *QueryRangeService) Tail(ctx context.Context, query string, tailLimit int64, startNs int64) (model.IWatcher, error) {
	if q.plugin != nil {
		return q.plugin.Tail(ctx, query, tailLimit, startNs)
	}

	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	sqlQuery, err := logql_transpiler.Transpile(query)
	if err != nil {
		return nil, err
	}

	res := NewWatcher(make(chan model.QueryRangeOutput, tailWatcherBufSize))

	var from time.Time
	if startNs > 0 {
		from = time.Unix(0, startNs)
	} else {
		from = time.Now().Add(-1 * time.Hour)
	}

	_ctx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(tailPollInterval)
		defer cancel()
		defer close(res.GetRes())
		defer ticker.Stop()
		json := jsoniter.ConfigFastest

		stream := json.BorrowStream(nil)
		defer json.ReturnStream(stream)
		for range ticker.C {
			versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
			if err != nil {
				logger.Error(err)
				return
			}

			select {
			case <-res.Done():
				return
			default:
			}

			limit := int64(tailIncrementalLimit)
			if tailLimit > 0 && tailLimit < limit {
				limit = tailLimit
			}
			out, err := sqlQuery[0].Process(tables.PopulateTableNames(&shared.PlannerContext{
				IsCluster:  conn.Config.ClusterName != "",
				From:       from,
				To:         time.Now(),
				OrderASC:   false,
				Limit:      limit,
				Ctx:        _ctx,
				CHDb:       conn.Session,
				CHFinalize: true,
				CHSqlCtx: &sql.Ctx{
					Params: map[string]sql.SQLObject{},
					Result: map[string]sql.SQLObject{},
				},
				CancelCtx:   cancel,
				VersionInfo: versionInfo,
			}, conn), nil)
			if err != nil {
				logger.Error(err)
				return
			}
			var lastFp uint64
			i := 0
			j := 0
			stream.WriteObjectStart()
			stream.WriteObjectField("streams")
			stream.WriteArrayStart()
			for entries := range out {
				for _, e := range entries {
					if e.Err == io.EOF {
						continue
					}
					if e.Err != nil {
						onErr(e.Err, res.GetRes())
						return
					}
					if lastFp != e.Fingerprint {
						if i > 0 {
							stream.WriteArrayEnd()
							stream.WriteObjectEnd()
							stream.WriteMore()
						}
						lastFp = e.Fingerprint
						i = 1
						j = 0

						stream.WriteObjectStart()
						stream.WriteObjectField("stream")
						writeMap(stream, e.Labels)
						stream.WriteMore()
						stream.WriteObjectField("values")
						stream.WriteArrayStart()
					}
					if j > 0 {
						stream.WriteMore()
					}
					j = 1
					stream.WriteArrayStart()
					stream.WriteString(fmt.Sprintf("%d", e.TimestampNS))
					stream.WriteMore()
					stream.WriteString(e.Message)
					stream.WriteArrayEnd()
					if from.UnixNano() < e.TimestampNS {
						from = time.Unix(0, e.TimestampNS+1)
					}
				}
			}
			if i > 0 {
				stream.WriteArrayEnd()
				stream.WriteObjectEnd()
			}
			stream.WriteArrayEnd()
			stream.WriteMore()
			stream.WriteObjectField("dropped_entries")
			stream.WriteArrayStart()
			stream.WriteArrayEnd()
			stream.WriteObjectEnd()
			res.GetRes() <- model.QueryRangeOutput{Str: string(stream.Buffer())}
			stream.Reset(nil)
		}
	}()
	return res, nil
}

// QueryIndexStats returns stream statistics for GET /loki/api/v1/index/stats.
// streams = distinct fingerprints, entries = total log lines, bytes = uncompressed volume,
// chunks = distinct (stream, day) pairs (proxy for object-storage chunk count).
func (q *QueryRangeService) QueryIndexStats(ctx context.Context, query string, fromNs, toNs int64) (*model.IndexStatsResult, error) {
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	var script *logql_parser.LogQLScript
	if query != "" {
		script, err = logql_parser.Parse(query)
		if err != nil {
			return nil, err
		}
	}

	_ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	plannerCtx := tables.PopulateTableNames(&shared.PlannerContext{
		IsCluster:  conn.Config.ClusterName != "",
		From:       time.Unix(0, fromNs),
		To:         time.Unix(0, toNs),
		Ctx:        _ctx,
		CancelCtx:  cancel,
		CHDb:       conn.Session,
		CHFinalize: true,
		CHSqlCtx: &sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		},
		VersionInfo: versionInfo,
	}, conn)

	statsQuery := sql.NewSelect().
		Select(
			sql.NewRawObject("toInt64(uniqExact(fingerprint))"),
			sql.NewRawObject("toInt64(COUNT(*))"),
			sql.NewRawObject("toInt64(SUM(length(string)))"),
			sql.NewRawObject("toInt64(uniqExact(fingerprint, toDate(toDateTime(intDiv(timestamp_ns, 1000000000)))))"),
		).
		From(sql.NewRawObject(plannerCtx.SamplesDistTableName)).
		AndPreWhere(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(fromNs)),
			sql.Lt(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(toNs)),
			sql.NewIn(sql.NewRawObject("type_v2"), sql.NewIntVal(0), sql.NewIntVal(1)),
		)

	if script != nil && script.Head.StrSelector != nil && len(script.Head.StrSelector.StrSelCmds) > 0 {
		fpPlanner, err := logql_transpiler.PlanFingerprints(script)
		if err != nil {
			return nil, err
		}
		fpSelect, err := fpPlanner.Process(plannerCtx)
		if err != nil {
			return nil, err
		}
		fpWith := sql.NewWith(fpSelect, "fp_sel")
		statsQuery = statsQuery.
			With(fpWith).
			AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(fpWith)))
	}

	var opts []int
	if plannerCtx.IsCluster {
		opts = append(opts, sql.STRING_OPT_INLINE_WITH)
	}
	strQuery, err := statsQuery.String(plannerCtx.CHSqlCtx, opts...)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(_ctx, strQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result model.IndexStatsResult
	if rows.Next() {
		if err = rows.Scan(&result.Streams, &result.Entries, &result.Bytes, &result.Chunks); err != nil {
			return nil, err
		}
	}
	return &result, nil
}

type Watcher struct {
	res       chan model.QueryRangeOutput
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func NewWatcher(res chan model.QueryRangeOutput) model.IWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Watcher{
		res:    res,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (w *Watcher) Done() <-chan struct{} {
	return w.ctx.Done()
}

func (w *Watcher) GetRes() chan model.QueryRangeOutput {
	return w.res
}

func (w *Watcher) Close() {
	w.closeOnce.Do(w.cancel)
}

func writeMap(stream *jsoniter.Stream, m map[string]string) {
	i := 0
	stream.WriteObjectStart()
	for k, v := range m {
		if i > 0 {
			stream.WriteMore()
		}
		stream.WriteObjectField(k)
		stream.WriteString(v)
		i++
	}
	stream.WriteObjectEnd()
}
