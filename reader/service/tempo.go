package service

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/plugins"
	"github.com/metrico/qryn/reader/tempo"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	traceql_transpiler "github.com/metrico/qryn/reader/traceql/transpiler"
	"github.com/metrico/qryn/reader/utils/dbversion"
	sqlselect "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/metrico/qryn/reader/utils/tables"
	"github.com/valyala/fastjson"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type zipkinPayload struct {
	payload     string
	startTimeNs int64
	durationNs  int64
	traceId     string
	spanId      string
	payloadType int
	parentId    string
}

type TempoService struct {
	model.ServiceData
	plugin plugins.TempoServicePlugin
}

func NewTempoService(data model.ServiceData) model.ITempoService {
	var p plugins.TempoServicePlugin
	_p := plugins.GetTempoServicePlugin()
	if _p != nil {
		p = *_p
	}
	return &TempoService{
		ServiceData: data,
		plugin:      p,
	}
}

func (t *TempoService) GetQueryRequest(ctx context.Context, startNS int64, endNS int64, traceId []byte,
	conn *model.DataDatabasesMap) sqlselect.ISelect {
	if t.plugin != nil {
		return t.plugin.GetQueryRequest(ctx, startNS, endNS, traceId, conn)
	}
	tableName := tables.GetTableName("tempo_traces")
	if conn.Config.ClusterName != "" {
		tableName = tables.GetTableName("tempo_traces_dist")
	}
	oRequest := sqlselect.NewSelect().
		Select(
			sqlselect.NewRawObject("trace_id"),
			sqlselect.NewRawObject("span_id"),
			sqlselect.NewRawObject("parent_id"),
			sqlselect.NewRawObject("timestamp_ns"),
			sqlselect.NewRawObject("duration_ns"),
			sqlselect.NewRawObject("payload_type"),
			sqlselect.NewRawObject("payload")).
		From(sqlselect.NewRawObject(tableName)).
		AndWhere(
			sqlselect.Eq(sqlselect.NewRawObject("trace_id"), sqlselect.NewCustomCol(
				func(ctx *sqlselect.Ctx, options ...int) (string, error) {
					strTraceId, err := sqlselect.NewStringVal(string(traceId)).String(ctx, options...)
					if err != nil {
						return "", err
					}
					return fmt.Sprintf("unhex(%s)", strTraceId), nil
				}),
			)).
		OrderBy(sqlselect.NewRawObject("timestamp_ns")).
		Limit(sqlselect.NewIntVal(2000))
	if startNS != 0 {
		oRequest = oRequest.AndWhere(sqlselect.Ge(sqlselect.NewRawObject("timestamp_ns"), sqlselect.NewIntVal(startNS)))
	}
	if endNS != 0 {
		oRequest = oRequest.AndWhere(sqlselect.Lt(sqlselect.NewRawObject("timestamp_ns"), sqlselect.NewIntVal(endNS)))
	}
	witORequest := sqlselect.NewWith(oRequest, "raw")
	oRequest = sqlselect.NewSelect().With(witORequest).
		Select(
			sqlselect.NewRawObject("trace_id"),
			sqlselect.NewRawObject("span_id"),
			sqlselect.NewRawObject("parent_id"),
			sqlselect.NewRawObject("timestamp_ns"),
			sqlselect.NewRawObject("duration_ns"),
			sqlselect.NewRawObject("payload_type"),
			sqlselect.NewRawObject("payload")).
		From(sqlselect.NewWithRef(witORequest)).
		OrderBy(sqlselect.NewOrderBy(sqlselect.NewRawObject("timestamp_ns"), sqlselect.ORDER_BY_DIRECTION_ASC))
	return oRequest
}

func (t *TempoService) OutputQuery(binIds bool, rows *sql.Rows) (chan *model.SpanResponse, error) {
	res := make(chan *model.SpanResponse)
	go func() {
		defer close(res)
		parser := fastjson.Parser{}
		for rows.Next() {
			var zipkin zipkinPayload
			err := rows.Scan(&zipkin.traceId, &zipkin.spanId, &zipkin.parentId,
				&zipkin.startTimeNs, &zipkin.durationNs, &zipkin.payloadType, &zipkin.payload)
			if err != nil {
				fmt.Println(err)
				return
			}
			var (
				span        *v1.Span
				serviceName string
			)
			switch zipkin.payloadType {
			case 1:
				span, serviceName, err = parseZipkinJSON(&zipkin, &parser, binIds)
			case 2:
				span, serviceName, err = parseOTLP(&zipkin)
			}
			if err != nil {
				fmt.Println(err)
				return
			}
			res <- &model.SpanResponse{
				Span: span, ServiceName: serviceName,
			}
		}
	}()
	return res, nil
}

func (t *TempoService) Query(ctx context.Context, startNS int64, endNS int64, traceId []byte,
	binIds bool) (chan *model.SpanResponse, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	oRequest := t.GetQueryRequest(ctx, startNS, endNS, traceId, conn)
	request, err := oRequest.String(&sqlselect.Ctx{
		Params: map[string]sqlselect.SQLObject{},
		Result: map[string]sqlselect.SQLObject{},
	})
	if err != nil {
		return nil, err
	}
	rows, err := conn.Session.QueryCtx(ctx, request)
	if err != nil {
		return nil, err
	}
	return t.OutputQuery(binIds, rows)
}

func (t *TempoService) GetTagsRequest(ctx context.Context, conn *model.DataDatabasesMap) sqlselect.ISelect {
	tableName := tables.GetTableName("tempo_traces_kv")
	if conn.Config.ClusterName != "" {
		tableName = tables.GetTableName("tempo_traces_kv_dist")
	}
	oQuery := sqlselect.NewSelect().
		Distinct(true).
		Select(sqlselect.NewRawObject("key")).
		From(sqlselect.NewRawObject(tableName)).
		OrderBy(sqlselect.NewRawObject("key"))
	return oQuery
}

func (t *TempoService) Tags(ctx context.Context) (chan string, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	oQuery := t.GetTagsRequest(ctx, conn)
	query, err := oQuery.String(&sqlselect.Ctx{
		Params: map[string]sqlselect.SQLObject{},
		Result: map[string]sqlselect.SQLObject{},
	})
	if err != nil {
		return nil, err
	}
	rows, err := conn.Session.QueryCtx(ctx, query)
	if err != nil {
		return nil, err
	}
	res := make(chan string)
	go func() {
		defer close(res)
		for rows.Next() {
			var k string
			err = rows.Scan(&k)
			if err != nil {
				return
			}
			res <- k
		}
	}()
	return res, nil
}

func (t *TempoService) TagsV2(ctx context.Context, query string, from time.Time, to time.Time,
	limit int) (chan string, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	var oScript *traceql_parser.TraceQLScript
	if query != "" {
		oScript, err = traceql_parser.Parse(query)
		if err != nil {
			return nil, err
		}
	}

	planCtx := shared.PlannerContext{
		IsCluster: conn.Config.ClusterName != "",
		From:      from,
		To:        to,
		Limit:     int64(limit),
		CHDb:      conn.Session,
		Ctx:       ctx,
	}

	tables.PopulateTableNames(&planCtx, conn)

	planner, err := traceql_transpiler.PlanTagsV2(oScript)
	if err != nil {
		return nil, err
	}

	req, err := planner.Process(&planCtx)
	if err != nil {
		return nil, err
	}

	res := make(chan string)
	go func() {
		defer close(res)
		for tags := range req {
			for _, value := range tags {
				res <- value
			}
		}
	}()

	return res, nil
}

func (t *TempoService) ValuesV2(ctx context.Context, key string, query string, from time.Time, to time.Time,
	limit int) (chan string, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	var oScript *traceql_parser.TraceQLScript
	if query != "" {
		oScript, err = traceql_parser.Parse(query)
		if err != nil {
			return nil, err
		}
	}

	planCtx := shared.PlannerContext{
		IsCluster: conn.Config.ClusterName != "",
		From:      from,
		To:        to,
		Limit:     int64(limit),
		CHDb:      conn.Session,
		Ctx:       ctx,
	}

	tables.PopulateTableNames(&planCtx, conn)

	planner, err := traceql_transpiler.PlanValuesV2(oScript, key)
	if err != nil {
		return nil, err
	}

	req, err := planner.Process(&planCtx)
	if err != nil {
		return nil, err
	}

	res := make(chan string)
	go func() {
		defer close(res)
		for tags := range req {
			for _, value := range tags {
				res <- value
			}
		}
	}()

	return res, nil
}

func (t *TempoService) GetValuesRequest(ctx context.Context, tag string, conn *model.DataDatabasesMap) sqlselect.ISelect {
	tableName := tables.GetTableName("tempo_traces_kv")
	if conn.Config.ClusterName != "" {
		tableName = tables.GetTableName("tempo_traces_kv_dist")
	}
	oRequest := sqlselect.NewSelect().
		Distinct(true).
		Select(sqlselect.NewRawObject("val")).
		From(sqlselect.NewRawObject(tableName)).
		AndWhere(sqlselect.Eq(sqlselect.NewRawObject("key"), sqlselect.NewStringVal(tag))).
		OrderBy(sqlselect.NewRawObject("val"))
	return oRequest
}

func (t *TempoService) Values(ctx context.Context, tag string) (chan string, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	tag = strings.TrimPrefix(tag, "span.")
	tag = strings.TrimPrefix(tag, ".")
	tag = strings.TrimPrefix(tag, "resource.")
	oRequest := t.GetValuesRequest(ctx, tag, conn)
	query, err := oRequest.String(&sqlselect.Ctx{
		Params: map[string]sqlselect.SQLObject{},
		Result: map[string]sqlselect.SQLObject{},
	})
	if err != nil {
		return nil, err
	}
	rows, err := conn.Session.QueryCtx(ctx, query)
	if err != nil {
		return nil, err
	}
	res := make(chan string)
	go func() {
		defer close(res)
		for rows.Next() {
			var v string
			err = rows.Scan(&v)
			if err != nil {
				return
			}
			res <- v
		}
	}()
	return res, nil
}

func (t *TempoService) Search(ctx context.Context,
	tags string, minDurationNS int64, maxDurationNS int64, limit int, fromNS int64, toNS int64) (chan *model.TraceResponse, error) {
	conn, err := t.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	var idxQuery *tempo.SQLIndexQuery = nil
	distributed := conn.Config.ClusterName != ""
	if tags != "" {
		ver, err := dbversion.GetVersionInfo(ctx, distributed, conn.Session)
		if err != nil {
			return nil, err
		}
		idxQuery = &tempo.SQLIndexQuery{
			Tags:          tags,
			Ctx:           ctx,
			FromNS:        fromNS,
			ToNS:          toNS,
			MinDurationNS: minDurationNS,
			MaxDurationNS: maxDurationNS,
			Distributed:   false,
			Database:      conn.Config.Name,
			Ver:           ver,
			Limit:         int64(limit),
		}
	}
	request, err := tempo.GetTracesQuery(ctx, idxQuery, limit, fromNS, toNS, distributed, minDurationNS, maxDurationNS)
	if err != nil {
		return nil, err
	}
	strRequest, err := request.String(&sqlselect.Ctx{})
	if err != nil {
		return nil, err
	}
	rows, err := conn.Session.QueryCtx(ctx, strRequest)
	if err != nil {
		return nil, err
	}
	res := make(chan *model.TraceResponse)
	go func() {
		defer close(res)
		for rows.Next() {
			row := model.TraceResponse{}
			err = rows.Scan(&row.TraceID,
				&row.RootServiceName,
				&row.RootTraceName,
				&row.StartTimeUnixNano,
				&row.DurationMs)
			if err != nil {
				fmt.Println(err)
				return
			}
			res <- &row
		}
	}()
	return res, nil
}

func decodeParentId(parentId []byte) ([]byte, error) {
	if len(parentId) < 16 {
		return nil, nil
	}
	if len(parentId) > 16 {
		return nil, fmt.Errorf("parent id is too big")
	}
	res := make([]byte, 8)
	_, err := hex.Decode(res, parentId)
	return res, err
}

func parseZipkinJSON(payload *zipkinPayload, parser *fastjson.Parser, _ bool) (*v1.Span, string, error) {
	root, err := parser.Parse(payload.payload)
	if err != nil {
		return nil, "", err
	}
	kind := v1.Span_SPAN_KIND_UNSPECIFIED
	switch string(root.GetStringBytes("kind")) {
	case "CLIENT":
		kind = v1.Span_SPAN_KIND_CLIENT
	case "SERVER":
		kind = v1.Span_SPAN_KIND_SERVER
	case "PRODUCER":
		kind = v1.Span_SPAN_KIND_PRODUCER
	case "CONSUMER":
		kind = v1.Span_SPAN_KIND_CONSUMER
	}
	traceId := payload.traceId
	/*if binIds {
		_traceId := make([]byte, 32)
		_, err := hex.Decode(_traceId, traceId)
		if err != nil {
			fmt.Println(traceId)
			fmt.Println(err)
			return nil, "", err
		}
		traceId = _traceId
	}*/
	id := payload.spanId
	/*if binIds {
		_id := make([]byte, 16)
		_, err := hex.Decode(_id, id)
		if err != nil {
			fmt.Println(id)
			fmt.Println(err)
			return nil, "", err
		}
		id = _id
	}*/
	span := v1.Span{
		TraceId:                []byte(traceId[:16]),
		SpanId:                 []byte(id[:8]),
		TraceState:             "",
		ParentSpanId:           nil,
		Name:                   string(root.GetStringBytes("name")),
		Kind:                   kind,
		StartTimeUnixNano:      uint64(payload.startTimeNs),
		EndTimeUnixNano:        uint64(payload.startTimeNs + payload.durationNs),
		Attributes:             make([]*common.KeyValue, 0, 10),
		DroppedAttributesCount: 0,
		Events:                 make([]*v1.Span_Event, 0, 10),
		DroppedEventsCount:     0,
		Links:                  nil,
		DroppedLinksCount:      0,
		Status:                 nil, // todo we set status here.
	}
	parentId := root.GetStringBytes("parentId")
	if parentId != nil {
		bParentId, err := decodeParentId(parentId)
		if err == nil {
			span.ParentSpanId = bParentId
		}
	}
	attrs := root.GetObject("tags")
	serviceName := ""
	if attrs != nil {
		attrs.Visit(func(key []byte, v *fastjson.Value) {
			if v.Type() != fastjson.TypeString {
				return
			}
			span.Attributes = append(span.Attributes, &common.KeyValue{
				Key: string(key),
				Value: &common.AnyValue{
					Value: &common.AnyValue_StringValue{StringValue: string(v.GetStringBytes())},
				},
			})
		})
	}
	for _, endpoint := range []string{"localEndpoint", "remoteEndpoint"} {
		ep := root.GetObject(endpoint)
		if ep == nil {
			continue
		}
		for _, attr := range []string{"serviceName", "ipv4", "ipv6"} {
			_val := ep.Get(attr)
			if _val == nil || _val.Type() != fastjson.TypeString {
				continue
			}
			if serviceName == "" && attr == "serviceName" {
				serviceName = string(_val.GetStringBytes())
			}
			span.Attributes = append(span.Attributes, &common.KeyValue{
				Key: endpoint + "." + attr,
				Value: &common.AnyValue{
					Value: &common.AnyValue_StringValue{StringValue: string(_val.GetStringBytes())},
				},
			})
		}
		port := root.GetInt64(endpoint, "port")
		if port != 0 {
			span.Attributes = append(span.Attributes, &common.KeyValue{
				Key: endpoint + ".port",
				Value: &common.AnyValue{
					Value: &common.AnyValue_IntValue{IntValue: port},
				},
			})
		}
	}
	span.Attributes = append(span.Attributes, &common.KeyValue{
		Key:   "service.name",
		Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: serviceName}},
	})
	for _, anno := range root.GetArray("annotations") {
		ts := anno.GetUint64("timestamp") * 1000
		if ts == 0 {
			continue
		}
		span.Events = append(span.Events, &v1.Span_Event{
			TimeUnixNano: ts,
			Name:         string(anno.GetStringBytes("value")),
		})
	}

	if span.Status == nil {
		span.Status = &v1.Status{
			Code: v1.Status_STATUS_CODE_UNSET,
		}
	}
	return &span, serviceName, nil
}

func parseOTLP(payload *zipkinPayload) (*v1.Span, string, error) {
	var (
		span *v1.Span
		err  error
	)
	if payload.payload[0] == '{' {
		span, err = parseOTLPJson(payload)
	} else {
		span, err = parseOTLPPB(payload)
	}
	if err != nil {
		return nil, "", err
	}
	firstLevelMap := make(map[string]*common.KeyValue)
	for _, kv := range span.Attributes {
		firstLevelMap[kv.Key] = kv
	}
	serviceName := ""
	for _, attr := range []string{"peer.service", "service.name", "faas.name",
		"k8s.deployment.name", "process.executable.name"} {
		if val, ok := firstLevelMap[attr]; ok && val.Value.GetStringValue() != "" {
			serviceName = val.Value.GetStringValue()
			break
		}
	}
	if serviceName == "" {
		serviceName = "OTLPResourceNoServiceName"
	}
	firstLevelMap["service.name"] = &common.KeyValue{
		Key:   "service.name",
		Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: serviceName}},
	}
	span.Attributes = make([]*common.KeyValue, 0, len(firstLevelMap))
	for _, kv := range firstLevelMap {
		span.Attributes = append(span.Attributes, kv)
	}
	if span.Status == nil {
		span.Status = &v1.Status{
			Code: v1.Status_STATUS_CODE_UNSET,
		}
	}
	return span, serviceName, nil

}

func parseOTLPPB(payload *zipkinPayload) (*v1.Span, error) {
	span := &v1.Span{}
	err := proto.Unmarshal([]byte(payload.payload), span)
	return span, err
}
