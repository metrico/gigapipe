package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/metrico/qryn/v4/logger"
	"github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/plugins"
	dbversion "github.com/metrico/qryn/v4/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/metrico/qryn/v4/reader/utils/tables"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

type QueryLabelsService struct {
	model.ServiceData
	plugin plugins.QueryLabelsServicePlugin
}

func NewQueryLabelsService(sd *model.ServiceData) *QueryLabelsService {
	p := plugins.GetQueryLabelsServicePlugin()
	res := &QueryLabelsService{
		ServiceData: *sd,
	}
	if p != nil {
		(*p).SetServiceData(sd)
		res.plugin = *p
	}
	return res
}

func (q *QueryLabelsService) GenericLabelReq(ctx context.Context, query string, args ...interface{}) (chan string, error) {
	fmt.Println(query)
	session, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := session.Session.QueryCtx(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	res := make(chan string)
	go func() {
		defer close(res)
		defer rows.Close()
		res <- `{"status": "success","data": [`
		i := 0
		for rows.Next() {
			strLbl := ""
			err = rows.Scan(&strLbl)
			if err != nil {
				logger.Error(err)
				break
			}
			qStrLbl, err := json.Marshal(strLbl)
			if err != nil {
				logger.Error(err)
				break
			}
			if i != 0 {
				res <- ","
			}
			res <- string(qStrLbl)
			i++
		}
		res <- "]}"
	}()
	return res, nil
}

func (q *QueryLabelsService) isDistributed(db *model.DataDatabasesMap) bool {
	return db.Config.ClusterName != ""
}

func (q *QueryLabelsService) GetEstimateKVComplexityRequest(ctx context.Context,
	conn *model.DataDatabasesMap,
) sql.ISelect {
	if q.plugin != nil {
		return q.plugin.EstimateKVComplexity(ctx, conn)
	}
	tableName := tables.GetTableName("time_series")
	if q.isDistributed(conn) {
		tableName = tables.GetTableName("time_series_dist")
	}
	fpRequest := sql.NewSelect().
		Distinct(true).
		Select(sql.NewRawObject("fingerprint")).
		From(sql.NewRawObject(tableName)).
		Limit(sql.NewRawObject("10001"))
	withFpRequest := sql.NewWith(fpRequest, "fp_request")
	fpRequest = sql.NewSelect().
		With(withFpRequest).
		Select(sql.NewSimpleCol("COUNT(1)", "cnt")).
		From(sql.NewWithRef(withFpRequest))
	return fpRequest
}

func (q *QueryLabelsService) Labels(ctx context.Context, startMs int64, endMs int64, labelsType uint16) (chan string, error) {
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	samplesKVTable := tables.GetTableName("time_series_gin")
	if conn.Config.ClusterName != "" {
		samplesKVTable = tables.GetTableName("time_series_gin_dist")
	}
	sel := sql.NewSelect().Distinct(true).
		Select(sql.NewRawObject("key")).
		From(sql.NewSimpleCol(samplesKVTable, "samples")).
		AndWhere(
			sql.NewIn(sql.NewRawObject("type"), sql.NewIntVal(int64(labelsType)), sql.NewIntVal(int64(0))),
			sql.Ge(sql.NewRawObject("date"),
				sql.NewStringVal(FormatFromDate(time.Unix(startMs/1000, 0)))),
			sql.Le(sql.NewRawObject("date"),
				sql.NewStringVal(time.Unix(endMs/1000, 0).UTC().Format("2006-01-02"))),
		)
	query, err := sel.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}
	return q.GenericLabelReq(ctx, query)
}

func (q *QueryLabelsService) PromValues(ctx context.Context, label string, match []string, startMs int64, endMs int64,
	labelsType uint16,
) (chan string, error) {
	lMatchers := make([]string, len(match))
	var err error
	for i, m := range match {
		lMatchers[i], err = q.Prom2LogqlMatch(m)
		if err != nil {
			return nil, err
		}
	}
	return q.Values(ctx, label, lMatchers, startMs, endMs, labelsType)
}

func (q *QueryLabelsService) Prom2LogqlMatch(match string) (string, error) {
	e := promql.NewEngine(promql.EngineOpts{})
	rq, err := e.NewRangeQuery(nil, nil, match, time.Now(), time.Now(), time.Second)
	if err != nil {
		panic(err)
	}
	var getMatchers func(node parser.Node) []*labels.Matcher
	getMatchers = func(node parser.Node) []*labels.Matcher {
		var res []*labels.Matcher
		vs, ok := node.(*parser.VectorSelector)
		if ok {
			return vs.LabelMatchers
		}
		for _, c := range parser.Children(node) {
			res = append(res, getMatchers(c)...)
		}
		return res
	}
	matchers := getMatchers(rq.Statement())
	strMatchers := make([]string, len(matchers))
	for i, m := range matchers {
		strMatchers[i] = m.String()
	}
	return fmt.Sprintf("{%s}", strings.Join(strMatchers, ",")), nil
}

func (q *QueryLabelsService) Values(ctx context.Context, label string, match []string, startMs int64, endMs int64,
	labelsType uint16,
) (chan string, error) {
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	if label == "" {
		res := make(chan string, 1)
		defer close(res)
		res <- "{\"status\": \"success\",\"data\": []}"
		return res, nil
	}

	var planner shared.SQLRequestPlanner
	// TODO: add pluggable extension
	if len(match) > 0 {
		planner, err = q.getMultiMatchValuesPlanner(match, label)
		if err != nil {
			return nil, err
		}
	} else {
		planner = clickhouse_planner.NewValuesPlanner(nil, label, nil)
	}

	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	plannerCtx := shared.PlannerContext{
		IsCluster:   conn.Config.ClusterName != "",
		From:        time.Unix(startMs/1000, 0),
		To:          time.Unix(endMs/1000, 0),
		Limit:       10000,
		UseCache:    false,
		Ctx:         ctx,
		CHDb:        conn.Session,
		CHSqlCtx:    nil,
		Type:        uint8(labelsType),
		VersionInfo: versionInfo,
	}
	tables.PopulateTableNames(&plannerCtx, conn)
	query, err := planner.Process(&plannerCtx)
	if err != nil {
		return nil, err
	}
	strQuery, err := query.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}
	return q.GenericLabelReq(ctx, strQuery)
}

func (q *QueryLabelsService) getMultiMatchValuesPlanner(match []string, key string) (shared.SQLRequestPlanner, error) {
	matchScripts := make([]*logql_parser.LogQLScript, len(match))
	var err error
	for i, m := range match {
		matchScripts[i], err = logql_parser.Parse(m)
		if err != nil {
			return nil, err
		}
	}
	selects := make([]shared.SQLRequestPlanner, len(matchScripts))
	for i, m := range matchScripts {
		selects[i], err = logql_transpiler.PlanFingerprints(m)
		if err != nil {
			return nil, err
		}
	}
	var planner shared.SQLRequestPlanner = &clickhouse_planner.MultiStreamSelectPlanner{Mains: selects}
	planner = clickhouse_planner.NewValuesPlanner(planner, key, nil)
	return planner, nil
}

func (q *QueryLabelsService) Series(ctx context.Context, requests []string, startMs int64, endMs int64,
	labelsType uint16,
) (chan string, error) {
	res := make(chan string)
	if requests == nil {
		go func() {
			defer close(res)
			res <- `{"status":"success", "data":[]}`
		}()
		return res, nil
	}
	conn, err := q.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	planner, err := q.querySeries(requests)
	if err != nil {
		return nil, err
	}

	versionInfo, err := dbversion.GetVersionInfo(ctx, conn.Config.ClusterName != "", conn.Session)
	if err != nil {
		return nil, err
	}

	plannerCtx := shared.PlannerContext{
		IsCluster:   conn.Config.ClusterName != "",
		From:        time.Unix(startMs/1000, 0),
		To:          time.Unix(endMs/1000, 0),
		Limit:       10000,
		Ctx:         ctx,
		CHDb:        conn.Session,
		Type:        uint8(labelsType),
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
	go func() {
		defer rows.Close()
		defer close(res)
		lbls := ""
		i := 0
		res <- `{"status":"success", "data":[`
		for rows.Next() {
			err = rows.Scan(&lbls)
			if err != nil {
				logger.Error(err)
				break
			}
			if i != 0 {
				res <- ","
			}
			res <- lbls
			i++
		}
		res <- `]}`
	}()
	return res, nil
}

func (q *QueryLabelsService) querySeries(requests []string) (shared.SQLRequestPlanner, error) {
	fpPlanners := make([]shared.SQLRequestPlanner, len(requests))
	for i, req := range requests {
		script, err := logql_parser.ParseSeries(req)
		if err != nil {
			return nil, err
		}
		fpPlanners[i], err = logql_transpiler.PlanFingerprints(script)
		if err != nil {
			return nil, err
		}
	}
	var planner shared.SQLRequestPlanner = &clickhouse_planner.MultiStreamSelectPlanner{Mains: fpPlanners}
	planner = clickhouse_planner.NewSeriesPlanner(planner, nil)
	return planner, nil
}
