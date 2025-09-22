package promql_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	"strings"

	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func GetLabelMatchersDownsampleRequest(hints *storage.SelectHints,
	ctx *shared.PlannerContext, matchers ...*labels.Matcher) (sql.ISelect, error) {
	plannerV2 := NewInitDownsamplePlanner()
	selectStream := &StreamSelectPlanner{Matchers: matchers}
	plannerV2 = &StreamSelectCombiner{
		Main:           plannerV2,
		StreamSelector: selectStream,
	}
	plannerV2 = &DownsampleHintsPlanner{
		Main:    plannerV2,
		Partial: false,
		Hints:   hints,
	}
	query, err := plannerV2.Process(ctx)
	/* TODO: move to pro !!!TURNED OFF
	supportV5 := ctx.VersionInfo.IsVersionSupported("v5", ctx.From.UnixNano(), ctx.To.UnixNano())
	var plannerV2 shared.SQLRequestPlanner = &InitDownsamplePlanner{
		Use15SV2: true,
		Partial:  !supportV5,
	}
	selectStream := &StreamSelectPlanner{Matchers: matchers}
	plannerV2 = &StreamSelectCombiner{
		Main:           plannerV2,
		StreamSelector: selectStream,
	}
	plannerV2 = &DownsampleHintsPlanner{
		Main:    plannerV2,
		Partial: !supportV5,
		Hints:   hints,
	}

	if !supportV5 {
		var plannerV1 shared.SQLRequestPlanner = &InitDownsamplePlanner{
			Use15SV2: false,
			Partial:  true,
		}
		plannerV1 = &StreamSelectCombiner{
			Main:           plannerV1,
			StreamSelector: selectStream,
		}
		plannerV1 = &DownsampleHintsPlanner{
			Main:    plannerV1,
			Partial: true,
			Hints:   hints,
		}
		plannerV2 = &UnionPlanner{
			Main1: plannerV2,
			Main2: plannerV1,
			Hints: hints,
		}
	}
	query, err := plannerV2.Process(ctx)
	*/
	return query, err
}

func TranspileLabelMatchersDownsample(hints *storage.SelectHints,
	ctx *shared.PlannerContext, matchers ...*labels.Matcher) (*TranspileResponse, error) {
	query, err := GetLabelMatchersDownsampleRequest(hints, ctx, matchers...)
	if err != nil {
		return nil, err
	}
	if hints.Func == "count_over_time" {
		return &TranspileResponse{func(samples []model.Sample) []model.Sample {
			res := make([]model.Sample, 0, 10000)
			for _, sample := range samples {
				_samples := make([]model.Sample, int64(sample.Value))
				for i := range _samples {
					_samples[i].TimestampMs = sample.TimestampMs
					_samples[i].Value = 1
				}
				res = append(res, _samples...)
			}
			return res
		}, query}, nil
	}
	return &TranspileResponse{nil, query}, nil
}

func trimLabelsExperimental(query sql.ISelect, hints *storage.SelectHints) sql.ISelect {
	var labelsCol sql.SQLObject = nil
	var sel []sql.SQLObject = nil
	for _, col := range query.GetSelect() {
		if col.(sql.Aliased).GetAlias() == "labels" {
			labelsCol = col.(sql.Aliased).GetExpr()
			continue
		}
		sel = append(sel, col)
	}
	if labelsCol == nil {
		return query
	}
	patchedLabels := sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		strLabels, err := labelsCol.String(ctx, options...)
		if err != nil {
			return "", err
		}
		op := "IN"
		if !hints.By {
			op = "NOT IN"
		}
		return fmt.Sprintf("arrayFilter(x -> x.1 %s ('%s'), %s)",
			op, strings.Join(hints.Grouping, `','`), strLabels), nil
	})
	sel = append(sel, sql.NewCol(patchedLabels, "labels"))
	query.Select(sel...)
	return query
}

func patchField(query sql.ISelect, alias string, newField sql.Aliased) sql.ISelect {
	_select := make([]sql.SQLObject, len(query.GetSelect()))
	for i, f := range query.GetSelect() {
		if f.(sql.Aliased).GetAlias() != alias {
			_select[i] = f
			continue
		}
		_select[i] = newField
	}
	query.Select(_select...)
	return query
}
