package service

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/tempo"
	traceql_parser "github.com/metrico/qryn/v5/reader/traceql/traceql_parser"
	traceql_transpiler "github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sqlselect "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/reader/utils/tables"
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
				span, serviceName, err = parseZipkinJSON(&zipkin)
			case 2:
				span, serviceName, err = parseOTLP(&zipkin)
			}
			if err != nil {
				fmt.Println(err)
				return
			}
			if span == nil {
				continue
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
	request, err := oRequest.String(sqlselect.DefaultCtx())
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
	query, err := oQuery.String(sqlselect.DefaultCtx())
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
	query, err := oRequest.String(sqlselect.DefaultCtx())
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

// zipkinEndpoint holds the string fields of a zipkin endpoint in the order
// they become span attributes: serviceName, ipv4, ipv6.
type zipkinEndpoint struct {
	vals [3]string
	set  [3]bool
	port int64
}

var zipkinEndpointAttrs = [3]string{"serviceName", "ipv4", "ipv6"}

func (e *zipkinEndpoint) decode(d *jx.Decoder) error {
	return zipkinObj(d, func(d *jx.Decoder, key string) (err error) {
		for i, attr := range zipkinEndpointAttrs {
			if key == attr {
				e.vals[i], e.set[i], err = zipkinStr(d)
				return err
			}
		}
		if key == "port" {
			e.port, err = zipkinInt(d)
			return err
		}
		return d.Skip()
	})
}

// zipkinStr, zipkinInt, zipkinObj and zipkinArr skip values of an unexpected
// type instead of failing: a stored span with an odd field is still shown.
func zipkinStr(d *jx.Decoder) (string, bool, error) {
	if d.Next() != jx.String {
		return "", false, d.Skip()
	}
	s, err := d.Str()
	return s, err == nil, err
}

// zipkinInt returns 0 for anything but a plain decimal integer.
func zipkinInt(d *jx.Decoder) (int64, error) {
	if d.Next() != jx.Number {
		return 0, d.Skip()
	}
	n, err := d.Num()
	if err != nil {
		return 0, err
	}
	v, _ := strconv.ParseInt(n.String(), 10, 64)
	return v, nil
}

func zipkinObj(d *jx.Decoder, f func(d *jx.Decoder, key string) error) error {
	if d.Next() != jx.Object {
		return d.Skip()
	}
	return d.Obj(f)
}

func zipkinArr(d *jx.Decoder, f func(d *jx.Decoder) error) error {
	if d.Next() != jx.Array {
		return d.Skip()
	}
	return d.Arr(f)
}

func stringAttr(key, val string) *common.KeyValue {
	return &common.KeyValue{
		Key:   key,
		Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: val}},
	}
}

func parseZipkinJSON(payload *zipkinPayload) (*v1.Span, string, error) {
	var (
		kindStr, name, parentId string
		hasParentId             bool
		endpoints               [2]zipkinEndpoint
	)
	span := v1.Span{
		TraceId:           []byte(payload.traceId[:16]),
		SpanId:            []byte(payload.spanId[:8]),
		StartTimeUnixNano: uint64(payload.startTimeNs),
		EndTimeUnixNano:   uint64(payload.startTimeNs + payload.durationNs),
		Attributes:        make([]*common.KeyValue, 0, 10),
		Events:            make([]*v1.Span_Event, 0, 10),
		Status:            &v1.Status{Code: v1.Status_STATUS_CODE_UNSET},
	}
	err := zipkinObj(jx.DecodeStr(payload.payload), func(d *jx.Decoder, key string) (err error) {
		switch key {
		case "kind":
			kindStr, _, err = zipkinStr(d)
		case "name":
			name, _, err = zipkinStr(d)
		case "parentId":
			parentId, hasParentId, err = zipkinStr(d)
		case "tags":
			err = zipkinObj(d, func(d *jx.Decoder, key string) error {
				val, ok, err := zipkinStr(d)
				if ok {
					span.Attributes = append(span.Attributes, stringAttr(key, val))
				}
				return err
			})
		case "localEndpoint":
			err = endpoints[0].decode(d)
		case "remoteEndpoint":
			err = endpoints[1].decode(d)
		case "annotations":
			err = zipkinArr(d, func(d *jx.Decoder) error {
				var (
					ts  int64
					val string
				)
				err := zipkinObj(d, func(d *jx.Decoder, key string) (err error) {
					switch key {
					case "timestamp":
						ts, err = zipkinInt(d)
					case "value":
						val, _, err = zipkinStr(d)
					default:
						err = d.Skip()
					}
					return err
				})
				if ts > 0 {
					span.Events = append(span.Events, &v1.Span_Event{TimeUnixNano: uint64(ts) * 1000, Name: val})
				}
				return err
			})
		default:
			err = d.Skip()
		}
		return err
	})
	if err != nil {
		return nil, "", err
	}

	span.Name = name
	switch kindStr {
	case "CLIENT":
		span.Kind = v1.Span_SPAN_KIND_CLIENT
	case "SERVER":
		span.Kind = v1.Span_SPAN_KIND_SERVER
	case "PRODUCER":
		span.Kind = v1.Span_SPAN_KIND_PRODUCER
	case "CONSUMER":
		span.Kind = v1.Span_SPAN_KIND_CONSUMER
	}
	if hasParentId {
		if bParentId, err := decodeParentId([]byte(parentId)); err == nil {
			span.ParentSpanId = bParentId
		}
	}
	// service.name belongs in the resource (added by tempoController), not in span attributes.
	// Adding it here would cause Grafana to emit duplicate service_name matchers in trace-to-logs queries.
	serviceName := ""
	for i, prefix := range [2]string{"localEndpoint", "remoteEndpoint"} {
		ep := &endpoints[i]
		for j, attr := range zipkinEndpointAttrs {
			if !ep.set[j] {
				continue
			}
			if serviceName == "" && j == 0 {
				serviceName = ep.vals[j]
			}
			span.Attributes = append(span.Attributes, stringAttr(prefix+"."+attr, ep.vals[j]))
		}
		if ep.port != 0 {
			span.Attributes = append(span.Attributes, &common.KeyValue{
				Key:   prefix + ".port",
				Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: ep.port}},
			})
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
