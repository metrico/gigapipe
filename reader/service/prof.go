package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/prof"
	"github.com/metrico/qryn/reader/prof/prof_parser"
	"github.com/metrico/qryn/reader/prof/shared"
	v1 "github.com/metrico/qryn/reader/prof/types/v1"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/metrico/qryn/reader/utils/tables"
	"google.golang.org/protobuf/proto"
)

var (
	ErrTypeIDsMismatch = fmt.Errorf("left and right queries must have the same type ID")
)

type ProfService struct {
	DataSession model.IDBRegistry
}

func (ps *ProfService) ProfileTypes(ctx context.Context, start time.Time, end time.Time) ([]*v1.ProfileType, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	table := getTableName(db, tables.GetTableName("profiles_series"))
	query := sql.NewSelect().
		Distinct(true).
		Select(
			sql.NewRawObject("type_id"),
			sql.NewRawObject("sample_type_unit")).
		From(sql.NewRawObject(table)).
		Join(sql.NewJoin("array", sql.NewSimpleCol("sample_types_units", "sample_type_unit"), nil)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(start.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(end.Format("2006-01-02"))))
	strQ, err := query.String(sql.DefaultCtx())
	if err != nil {
		return nil, err
	}
	result := []*v1.ProfileType{}
	rows, err := db.Session.QueryCtx(ctx, strQ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var typeId string
		var sampleTypeUnit []any
		err = rows.Scan(&typeId, &sampleTypeUnit)
		if err != nil {
			return nil, err
		}
		namePeriodTypeUnit := strings.SplitN(typeId, ":", 3)
		result = append(result, &v1.ProfileType{
			ID: fmt.Sprintf("%s:%s:%s:%s:%s",
				namePeriodTypeUnit[0],
				sampleTypeUnit[0].(string),
				sampleTypeUnit[1].(string),
				namePeriodTypeUnit[1],
				namePeriodTypeUnit[2]),
			Name:       namePeriodTypeUnit[0],
			SampleType: sampleTypeUnit[0].(string),
			SampleUnit: sampleTypeUnit[1].(string),
			PeriodType: namePeriodTypeUnit[1],
			PeriodUnit: namePeriodTypeUnit[2],
		})
	}
	return result, nil
}

func (ps *ProfService) LabelNames(ctx context.Context, strScripts []string, start time.Time,
	end time.Time) (*v1.LabelNamesResponse, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	scripts, err := ps.parseScripts(strScripts)
	if err != nil {
		return nil, err
	}
	sel, err := prof.PlanLabelNames(ctx, scripts, start, end, db)
	if err != nil {
		return nil, err
	}
	var col string
	result := &v1.LabelNamesResponse{}
	err = ps.queryCols(ctx, db, sel, func() error {
		result.Names = append(result.Names, col)
		return nil
	}, []any{&col})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (ps *ProfService) LabelValues(ctx context.Context, strScripts []string, labelName string, start time.Time,
	end time.Time) (*v1.LabelValuesResponse, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	scripts, err := ps.parseScripts(strScripts)
	if err != nil {
		return nil, err
	}

	sel, err := prof.PlanLabelValues(ctx, scripts, labelName, start, end, db)
	if err != nil {
		return nil, err
	}
	result := &v1.LabelValuesResponse{}
	var col string
	err = ps.queryCols(ctx, db, sel, func() error {
		result.Names = append(result.Names, col)
		return nil
	}, []any{&col})
	return result, err
}

func (ps *ProfService) MergeStackTraces(ctx context.Context, strScript string, strTypeID string, start time.Time,
	end time.Time) (*prof.SelectMergeStacktracesResponse, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	scripts, err := ps.parseScripts([]string{strScript})
	if err != nil {
		return nil, err
	}
	script := scripts[0]

	typeId, err := shared.ParseTypeId(strTypeID)
	if err != nil {
		return nil, err
	}

	tree, err := ps.getTree(ctx, script, &typeId, start, end, db)
	if err != nil {
		return nil, err
	}

	sampleTypeUnit := fmt.Sprintf("%s:%s", typeId.SampleType, typeId.SampleUnit)

	levels := tree.BFS(sampleTypeUnit)

	res := &prof.SelectMergeStacktracesResponse{
		Flamegraph: &prof.FlameGraph{
			Names:   tree.Names,
			Levels:  levels,
			Total:   tree.Total()[0],
			MaxSelf: tree.MaxSelf()[0],
		},
	}
	return res, nil
}

func (ps *ProfService) SelectSeries(ctx context.Context, strScript string, strTypeID string, groupBy []string,
	agg v1.TimeSeriesAggregationType, step int64, start time.Time, end time.Time) (*prof.SelectSeriesResponse, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	scripts, err := ps.parseScripts([]string{strScript})
	if err != nil {
		return nil, err
	}
	script := scripts[0]

	typeId, err := shared.ParseTypeId(strTypeID)
	if err != nil {
		return nil, err
	}

	sel, err := prof.PlanSelectSeries(ctx, script, &typeId, groupBy, agg, step, start, end, db)
	if err != nil {
		return nil, err
	}
	var (
		tsMs   int64
		fp     uint64
		labels [][]any
		value  float64
		res    prof.SelectSeriesResponse
		lastFp uint64
	)

	err = ps.queryCols(ctx, db, sel, func() error {
		if lastFp != fp || lastFp == 0 {
			res.Series = append(res.Series, &v1.Series{
				Labels: nil,
				Points: []*v1.Point{{Value: value, Timestamp: tsMs}},
			})
			lastFp = fp
			for _, pair := range labels {
				res.Series[len(res.Series)-1].Labels = append(res.Series[len(res.Series)-1].Labels, &v1.LabelPair{
					Name:  pair[0].(string),
					Value: pair[1].(string),
				})
			}
			return nil
		}
		res.Series[len(res.Series)-1].Points = append(res.Series[len(res.Series)-1].Points, &v1.Point{
			Value:     value,
			Timestamp: tsMs,
		})
		return nil
	}, []any{&tsMs, &fp, &labels, &value})
	return &res, err
}

func (ps *ProfService) MergeProfiles(ctx context.Context, strScript string, strTypeID string, start time.Time,
	end time.Time) (*prof.Profile, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}
	scripts, err := ps.parseScripts([]string{strScript})
	if err != nil {
		return nil, err
	}
	script := scripts[0]

	typeId, err := shared.ParseTypeId(strTypeID)
	if err != nil {
		return nil, err
	}

	sel, err := prof.PlanMergeProfiles(ctx, script, &typeId, start, end, db)
	if err != nil {
		return nil, err
	}

	var (
		payload []byte
		merger  = NewProfileMergeV2()
		p       prof.Profile
	)

	err = ps.queryCols(ctx, db, sel, func() error {
		p.Reset()
		if err != nil {
			return err
		}
		err = proto.Unmarshal(payload, &p)
		if err != nil {
			return err
		}
		return merger.Merge(&p)
	}, []any{&payload})
	if err != nil {
		return nil, err
	}
	return merger.Profile(), nil
}

func (ps *ProfService) TimeSeries(ctx context.Context, strScripts []string, labels []string,
	start time.Time, end time.Time) (*prof.SeriesResponse, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	scripts, err := ps.parseScripts(strScripts)
	if err != nil {
		return nil, err
	}

	sel, err := prof.PlanSeries(ctx, scripts, labels, start, end, db)
	if err != nil {
		return nil, err
	}

	var (
		tags           [][]any
		typeId         string
		sampleTypeUnit []any
		res            prof.SeriesResponse
	)

	err = ps.queryCols(ctx, db, sel, func() error {
		parsedTypeId, err := shared.ParseShortTypeId(typeId)
		if err != nil {
			return err
		}
		ls := &v1.Labels{}
		ls.Labels = append(ls.Labels, &v1.LabelPair{Name: "__name__", Value: parsedTypeId.Tp})
		ls.Labels = append(ls.Labels, &v1.LabelPair{Name: "__period_type__", Value: parsedTypeId.PeriodType})
		ls.Labels = append(ls.Labels, &v1.LabelPair{Name: "__period_unit__", Value: parsedTypeId.PeriodUnit})
		ls.Labels = append(ls.Labels, &v1.LabelPair{Name: "__sample_type__", Value: sampleTypeUnit[0].(string)})
		ls.Labels = append(ls.Labels, &v1.LabelPair{Name: "__sample_unit__", Value: sampleTypeUnit[1].(string)})
		ls.Labels = append(ls.Labels, &v1.LabelPair{Name: "__profile_type__", Value: fmt.Sprintf(
			"%s:%s:%s:%s:%s",
			parsedTypeId.Tp,
			sampleTypeUnit[0].(string),
			sampleTypeUnit[1].(string),
			parsedTypeId.PeriodType,
			parsedTypeId.PeriodUnit)})
		for _, tag := range tags {
			ls.Labels = append(ls.Labels, &v1.LabelPair{Name: tag[0].(string), Value: tag[1].(string)})
		}
		res.LabelsSet = append(res.LabelsSet, ls)
		return nil
	}, []any{&tags, &typeId, &sampleTypeUnit})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (ps *ProfService) ProfileStats(ctx context.Context) (*v1.GetProfileStatsResponse, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	profilesTableName := tables.GetTableName("profiles")
	profilesSeriesTableName := tables.GetTableName("profiles_series")
	if db.Config.ClusterName != "" {
		profilesTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, profilesTableName)
		profilesSeriesTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, profilesSeriesTableName)
	}

	brackets := func(object sql.SQLObject) sql.SQLObject {
		return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strObject, err := object.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s)", strObject), nil
		})
	}

	dateToNS := func(object sql.SQLObject) sql.SQLObject {
		return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strObject, err := object.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("toUnixTimestamp((%s)) * 1000000000", strObject), nil
		})
	}

	nonEmptyReq := sql.NewSelect().
		Select(sql.NewSimpleCol("any(1::Int8)", "non_empty")).
		From(sql.NewRawObject(profilesTableName))
	withNonEmpty := sql.NewWith(nonEmptyReq, "non_empty")

	minDateReq := sql.NewSelect().
		Select(sql.NewSimpleCol("min(date)", "min_date"),
			sql.NewSimpleCol("max(date)", "max_date")).
		From(sql.NewRawObject(profilesSeriesTableName))
	withMinDate := sql.NewWith(minDateReq, "min_date")

	minTimeReq := sql.NewSelect().
		Select(
			sql.NewSimpleCol("intDiv(min(timestamp_ns), 1000000)", "min_time"),
			sql.NewSimpleCol("intDiv(max(timestamp_ns), 1000000)", "max_time")).
		From(sql.NewRawObject(profilesTableName)).
		OrWhere(
			sql.Lt(
				sql.NewRawObject("timestamp_ns"),
				dateToNS(sql.NewSelect().Select(sql.NewRawObject("any(min_date + INTERVAL '1 day')")).
					From(sql.NewWithRef(withMinDate)))),
			sql.Ge(
				sql.NewRawObject("timestamp_ns"),
				dateToNS(sql.NewSelect().Select(sql.NewRawObject("any(max_date)")).
					From(sql.NewWithRef(withMinDate)))),
		)
	withMinTime := sql.NewWith(minTimeReq, "min_time")

	req := sql.NewSelect().
		With(withNonEmpty, withMinDate, withMinTime).
		Select(
			sql.NewCol(
				brackets(sql.NewSelect().Select(sql.NewRawObject("any(non_empty)")).From(sql.NewWithRef(withNonEmpty))),
				"non_empty"),
			sql.NewCol(
				brackets(sql.NewSelect().Select(sql.NewRawObject("any(min_time)")).From(sql.NewWithRef(withMinTime))),
				"min_date"),
			sql.NewCol(
				brackets(sql.NewSelect().Select(sql.NewRawObject("any(max_time)")).From(sql.NewWithRef(withMinTime))),
				"min_time"))

	var (
		nonEmpty int8
		minTime  int64
		maxTime  int64
		res      v1.GetProfileStatsResponse
	)

	err = ps.queryCols(ctx, db, req, func() error {
		res.DataIngested = nonEmpty != 0
		res.OldestProfileTime = minTime
		res.NewestProfileTime = maxTime
		return nil
	}, []any{&nonEmpty, &minTime, &maxTime})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (ps *ProfService) Settings(ctx context.Context) (*prof.GetSettingsResponse, error) {
	return &prof.GetSettingsResponse{
		Settings: []*prof.Setting{
			{
				Name:       "pluginSettings",
				Value:      "{}",
				ModifiedAt: time.Now().UnixMilli(),
			},
		},
	}, nil
}

func (ps *ProfService) RenderDiff(ctx context.Context,
	strLeftQuery string, strRightQuery string,
	leftFrom time.Time, rightFrom time.Time,
	leftTo time.Time, rightTo time.Time) (*Flamebearer, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	strLeftTypeId, strLeftScript, err := ps.detachTypeId(strLeftQuery)
	if err != nil {
		return nil, err
	}

	strRightTypeId, strRightScript, err := ps.detachTypeId(strRightQuery)
	if err != nil {
		return nil, err
	}

	if strLeftTypeId != strRightTypeId {
		return nil, ErrTypeIDsMismatch
	}

	scripts, err := ps.parseScripts([]string{strLeftScript, strRightScript})
	if err != nil {
		return nil, err
	}

	leftTypeId, err := shared.ParseTypeId(strLeftTypeId)
	if err != nil {
		return nil, err
	}

	rightTypeId, err := shared.ParseTypeId(strRightTypeId)
	if err != nil {
		return nil, err
	}

	leftTree, err := ps.getTree(ctx, scripts[0], &leftTypeId, leftFrom, leftTo, db)
	if err != nil {
		return nil, err
	}

	rightTree, err := ps.getTree(ctx, scripts[1], &rightTypeId, rightFrom, rightTo, db)
	if err != nil {
		return nil, err
	}

	if !assertPositive(leftTree) {
		return nil, fmt.Errorf("left tree is not positive")
	}

	if !assertPositive(rightTree) {
		return nil, fmt.Errorf("right tree is not positive")
	}

	synchronizeNames(leftTree, rightTree)
	mergeNodes(leftTree, rightTree)
	diff := computeFlameGraphDiff(leftTree, rightTree)
	fb := ps.diffToFlameBearer(diff, &leftTypeId)
	return fb, nil
}

func (ps *ProfService) AnalyzeQuery(ctx context.Context, strQuery string,
	from time.Time, to time.Time) (*prof.AnalyzeQueryResponse, error) {
	db, err := ps.DataSession.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	query, err := prof_parser.Parse(strQuery)
	if err != nil {
		return nil, err
	}

	req, err := prof.PlanAnalyzeQuery(ctx, query, from, to, db)
	if err != nil {
		return nil, err
	}

	var (
		size int64
		fps  int64
	)

	err = ps.queryCols(ctx, db, req, func() error { return nil }, []any{&size, &fps})
	if err != nil {
		return nil, err
	}

	return &prof.AnalyzeQueryResponse{
		QueryScopes: []*prof.QueryScope{
			{ComponentType: "store", ComponentCount: 1},
		},
		QueryImpact: &prof.QueryImpact{
			TotalBytesInTimeRange: uint64(size),
			TotalQueriedSeries:    uint64(fps),
		},
	}, nil
}

func (ps *ProfService) getTree(ctx context.Context, script *prof_parser.Script, typeId *shared.TypeId,
	start, end time.Time, db *model.DataDatabasesMap) (*Tree, error) {
	sel, err := prof.PlanMergeTraces(ctx, script, typeId, start, end, db)
	if err != nil {
		return nil, err
	}

	var (
		treeNodes [][]any
		functions [][]any
	)

	err = ps.queryCols(ctx, db, sel, func() error { return nil }, []any{&treeNodes, &functions})
	if err != nil {
		return nil, err
	}

	sampleTypeUnit := fmt.Sprintf("%s:%s", typeId.SampleType, typeId.SampleUnit)
	tree := NewTree()
	tree.SampleTypes = []string{sampleTypeUnit}
	tree.MergeTrie(treeNodes, functions, sampleTypeUnit)
	return tree, nil
}

func (ps *ProfService) parseScripts(strScripts []string) ([]*prof_parser.Script, error) {
	var err error
	scripts := make([]*prof_parser.Script, len(strScripts))
	for i, strScript := range strScripts {
		scripts[i], err = prof_parser.Parse(strScript)
		if err != nil {
			return nil, err
		}
	}
	return scripts, err
}

func (ps *ProfService) queryCols(ctx context.Context, db *model.DataDatabasesMap, sel sql.ISelect,
	f func() error, col []any) error {
	strSel, err := sel.String(sql.DefaultCtx())
	if err != nil {
		return err
	}
	fmt.Println(strSel)
	rows, err := db.Session.QueryCtx(ctx, strSel)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(col...)
		if err != nil {
			return err
		}
		err = f()
		if err != nil {
			return err
		}
	}
	return nil
}

func (ps *ProfService) detachTypeId(strQuery string) (string, string, error) {
	typeAndQuery := strings.SplitN(strQuery, "{", 2)
	if len(typeAndQuery) != 2 {
		return "", "", fmt.Errorf("invalid query format: %s", strQuery)
	}
	typeId := strings.TrimSpace(typeAndQuery[0])
	query := "{" + strings.TrimSpace(typeAndQuery[1])
	return typeId, query, nil
}

type units = string
type Flamebearer struct {
	Version              int                    `json:"version"`
	FlamebearerProfileV1 FlamebearerProfileV1   `json:"flamebearerProfileV1"`
	Telemetry            map[string]interface{} `json:"telemetry,omitempty"`
}

type FlamebearerProfileV1 struct {
	Flamebearer *FlamebearerV1                   `json:"flamebearer"`
	Metadata    FlamebearerMetadataV1            `json:"metadata"`
	Timeline    *FlamebearerTimelineV1           `json:"timeline"`
	Groups      map[string]FlamebearerTimelineV1 `json:"groups"`
	Heatmap     *Heatmap                         `json:"heatmap"`
	LeftTicks   int64                            `json:"leftTicks"`
	RightTicks  int64                            `json:"rightTicks"`
}

type FlamebearerV1 struct {
	Names    []string  `json:"names"`
	Levels   [][]int64 `json:"levels"`
	NumTicks int       `json:"numTicks"`
	MaxSelf  int       `json:"maxSelf"`
}

type FlamebearerMetadataV1 struct {
	Format     string `json:"format"`
	SpyName    string `json:"spyName"`
	SampleRate int64  `json:"sampleRate"`
	Units      units  `json:"units"`
	Name       string `json:"name"`
}

type FlamebearerTimelineV1 struct {
	StartTime     int64         `json:"startTime"`
	Samples       []uint64      `json:"samples"`
	DurationDelta int64         `json:"durationDelta"`
	Watermarks    map[int]int64 `json:"watermarks"`
}

type Heatmap struct {
	Values       [][]uint64 `json:"values"`
	TimeBuckets  int64      `json:"timeBuckets"`
	ValueBuckets int64      `json:"valueBuckets"`
	StartTime    int64      `json:"startTime"`
	EndTime      int64      `json:"endTime"`
	MinValue     uint64     `json:"minValue"`
	MaxValue     uint64     `json:"maxValue"`
	MinDepth     uint64     `json:"minDepth"`
	MaxDepth     uint64     `json:"maxDepth"`
}

func (ps *ProfService) diffToFlameBearer(diff *prof.FlameGraphDiff, typeId *shared.TypeId) *Flamebearer {
	flameGraph := &prof.FlameGraph{
		Names:   diff.Names,
		Levels:  diff.Levels,
		Total:   diff.Total,
		MaxSelf: diff.MaxSelf,
	}
	flameBearer := ps.flameGraphToFlameBearer(flameGraph, typeId)
	flameBearer.FlamebearerProfileV1.LeftTicks = diff.LeftTicks
	flameBearer.FlamebearerProfileV1.RightTicks = diff.RightTicks
	flameBearer.FlamebearerProfileV1.Metadata.Format = "double"
	return flameBearer
}

func (ps *ProfService) flameGraphToFlameBearer(flameGraph *prof.FlameGraph, typeId *shared.TypeId) *Flamebearer {
	if flameGraph == nil {
		flameGraph = &prof.FlameGraph{}
	}
	unit := typeId.SampleUnit
	sampleRate := 100
	switch typeId.SampleType {
	case "inuse_objects", "alloc_objects", "goroutine", "samples":
		unit = "objects"
	case "cpu":
		unit = "samples"
		sampleRate = 1000000000
	}

	flameBearer := &FlamebearerV1{
		Names:    flameGraph.Names,
		NumTicks: int(flameGraph.GetTotal()),
		MaxSelf:  int(flameGraph.GetMaxSelf()),
	}
	for _, l := range flameGraph.Levels {
		level := make([]int64, len(l.Values))
		copy(level, l.Values)
		flameBearer.Levels = append(flameBearer.Levels, level)
	}

	metadata := &FlamebearerMetadataV1{
		Format:     "single",
		SampleRate: int64(sampleRate),
		Units:      unit,
		Name:       typeId.SampleType,
	}

	return &Flamebearer{
		Version: 1,
		FlamebearerProfileV1: FlamebearerProfileV1{
			Flamebearer: flameBearer,
			Metadata:    *metadata,
		},
	}
}
