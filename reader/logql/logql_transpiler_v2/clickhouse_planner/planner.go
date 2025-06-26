package clickhouse_planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_parser"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func Plan(script *logql_parser.LogQLScript, finalize bool) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script, finalize: finalize}).plan()
}

func PlanFingerprints(script *logql_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).planFingerprints()
}

func PlanDetectLabels(script *logql_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).planDetectLabels()
}

func PlanPatterns(script *logql_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).planPatterns()
}

type planner struct {
	script   *logql_parser.LogQLScript
	finalize bool
	//Analyze parameters
	labelsJoinIdx            int
	fpCache                  *sql.With
	labelsCache              *sql.With
	simpleLabelOperation     []bool //if a pipeline operation can be made on time_series labels
	renewMainAfter           []bool
	fastUnwrap               bool
	matrixFunctionsOrder     []func() error
	matrixFunctionsLabelsIDX int
	metrics15Shortcut        bool
	offsetModifier           *time.Duration

	//SQL Planners
	fpPlanner      shared.SQLRequestPlanner
	samplesPlanner shared.SQLRequestPlanner
}

func (p *planner) plan() (shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}
	p.analyzeScript()

	err = p.planTS()
	if err != nil {
		return nil, err
	}

	if p.metrics15Shortcut {
		p.matrixFunctionsLabelsIDX = -1
		err = p.planMetrics15Shortcut(p.script)
		if err != nil {
			return nil, err
		}
	} else {
		err = p.planSpl()
		if err != nil {
			return nil, err
		}

		for _, f := range p.matrixFunctionsOrder {
			err = f()
			if err != nil {
				return nil, err
			}
		}
	}

	if p.script.StrSelector != nil {
		p.samplesPlanner = &MainOrderByPlanner{[]string{"timestamp_ns"}, p.samplesPlanner}
		if p.finalize {
			p.samplesPlanner = &MainLimitPlanner{p.samplesPlanner}
		}
	}

	if p.script.StrSelector == nil {
		duration, err := shared.GetDuration(p.script)
		if err != nil {
			return nil, err
		}
		p.samplesPlanner = &StepFixPlanner{
			Main:     p.samplesPlanner,
			Duration: duration,
		}
	}

	if p.labelsJoinIdx == -1 && p.matrixFunctionsLabelsIDX == -1 {
		p.samplesPlanner = &LabelsJoinPlanner{
			Main:         p.samplesPlanner,
			Fingerprints: p.fpPlanner,
			TimeSeries:   NewTimeSeriesInitPlanner(p.offsetModifier),
			FpCache:      &p.fpCache,
		}
	}

	p.samplesPlanner = &MainFinalizerPlanner{
		Main:     p.samplesPlanner,
		IsMatrix: p.script.StrSelector == nil,
		IsFinal:  p.finalize,
	}

	/*chGetter := &ClickhouseGetterPlanner{
		ClickhouseRequestPlanner: p.samplesPlanner,
		isMatrix:                 p.script.StrSelector == nil,
	}*/
	return p.samplesPlanner, nil
}

func (p *planner) planMetrics15Shortcut(script any) error {
	dfs := func(nodes ...any) error {
		for _, n := range nodes {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				return p.planMetrics15Shortcut(n)
			}
		}
		return nil
	}
	switch script.(type) {
	case *logql_parser.LogQLScript:
		script := script.(*logql_parser.LogQLScript)
		return dfs(script.TopK, script.AggOperator, script.LRAOrUnwrap)
	case *logql_parser.TopK:
		script := script.(*logql_parser.TopK)
		err := dfs(script.AggOperator, script.LRAOrUnwrap)
		if err != nil {
			return err
		}
		err = p.planTopK(script)
		if err != nil {
			return err
		}
		return p.planComparison(script.Comparison)
	case *logql_parser.AggOperator:
		script := script.(*logql_parser.AggOperator)
		err := dfs(&script.LRAOrUnwrap)
		if err != nil {
			return err
		}
		withLabels := false
		if script.ByOrWithoutPrefix != nil || script.ByOrWithoutSuffix != nil {
			withLabels = true
			p.matrixFunctionsLabelsIDX = 0
		}
		err = p.planAgg(script, withLabels)
		if err != nil {
			return err
		}
		return p.planComparison(script.Comparison)
	case *logql_parser.LRAOrUnwrap:
		script := script.(*logql_parser.LRAOrUnwrap)
		duration, err := time.ParseDuration(script.Time + script.TimeUnit)
		if err != nil {
			return err
		}
		p.samplesPlanner = &FingerprintFilterPlanner{
			FingerprintsSelectPlanner:  p.fpPlanner,
			MainRequestPlanner:         NewMetrics15ShortcutPlanner(script.Fn, duration, p.offsetModifier),
			FingerprintSelectWithCache: &p.fpCache,
		}
		return p.planComparison(script.Comparison)
	}
	return nil
}

func (p *planner) planDetectLabels() (shared.SQLRequestPlanner, error) {
	if p.script == nil {
		return &DetectLabelsPlanner{}, nil
	}
	if p.script.StrSelector == nil {
		return nil, fmt.Errorf("Unsupported query")
	}
	err := p.planTS()
	if err != nil {
		return nil, err
	}
	return &DetectLabelsPlanner{fpPlanner: p.fpPlanner}, nil
}

func (p *planner) planPatterns() (shared.SQLRequestPlanner, error) {
	if p.script == nil {
		return nil, fmt.Errorf("Unsupported query")
	}
	if p.script.StrSelector == nil {
		return nil, fmt.Errorf("Unsupported query")
	}
	err := p.planTS()
	if err != nil {
		return nil, err
	}
	return &PatternsPlanner{fpPlanner: p.fpPlanner}, nil
}

func (p *planner) planTS() error {
	streamSelector := getStreamSelector(p.script)
	var (
		labelNames []string
		ops        []string
		values     []string
	)
	for _, cmd := range streamSelector.StrSelCmds {
		labelNames = append(labelNames, cmd.Label.Name)
		ops = append(ops, cmd.Op)
		val, err := cmd.Val.Unquote()
		if err != nil {
			return err
		}
		values = append(values, val)
	}
	_fpPlanner := NewStreamSelectPlanner(labelNames, ops, values, p.offsetModifier)

	p.fpPlanner = _fpPlanner

	for i, isSimpleLabelFilter := range p.simpleLabelOperation {
		if !isSimpleLabelFilter {
			continue
		}
		ppl := getPipeline(p.script)[i]
		if ppl.LabelFilter != nil {
			p.fpPlanner = &SimpleLabelFilterPlanner{
				Expr:  getPipeline(p.script)[i].LabelFilter,
				FPSel: p.fpPlanner,
			}
		}
	}

	return nil
}

func (p *planner) planSpl() error {
	streamSelector := getStreamSelector(p.script)

	p.samplesPlanner = &FingerprintFilterPlanner{
		FingerprintsSelectPlanner:  p.fpPlanner,
		MainRequestPlanner:         NewSQLMainInitPlanner(p.offsetModifier),
		FingerprintSelectWithCache: &p.fpCache,
	}
	for i, ppl := range streamSelector.Pipelines {
		if i == p.labelsJoinIdx {
			p.samplesPlanner = &LabelsJoinPlanner{
				Main:         &MainOrderByPlanner{[]string{"timestamp_ns"}, p.samplesPlanner},
				Fingerprints: p.fpPlanner,
				TimeSeries:   NewTimeSeriesInitPlanner(p.offsetModifier),
				FpCache:      &p.fpCache,
				LabelsCache:  &p.labelsCache,
			}
		}
		var err error
		if ppl.LineFormat != nil {
			err = p.planLineFormat(&ppl)
		} else if ppl.LabelFilter != nil {
			err = p.planLabelFilter(&ppl, i)
		} else if ppl.LineFilter != nil {
			err = p.planLineFilter(&ppl, i)
		} else if ppl.Parser != nil {
			err = p.planParser(&ppl)
		} else if ppl.Unwrap != nil {
			err = p.planUnwrap(&ppl)
		} else if ppl.Drop != nil {
			err = p.planDrop(i, &ppl)
		}

		if err != nil {
			return err
		}

		if p.renewMainAfter[i] {
			p.samplesPlanner = &MainRenewPlanner{
				p.samplesPlanner,
				p.labelsJoinIdx != -1 && p.labelsJoinIdx <= i,
			}
		}
	}

	return nil
}

func (p *planner) planDrop(i int, ppl *logql_parser.StrSelectorPipeline) error {
	if p.simpleLabelOperation[i] {
		return nil
	}

	labels, values, err := getLabelsAndValuesFromDrop(ppl.Drop)
	if err != nil {
		return err
	}
	p.samplesPlanner = &PlannerDrop{
		Labels:      labels,
		Vals:        values,
		LabelsCache: &p.labelsCache,
		fpCache:     &p.fpCache,
		Main:        p.samplesPlanner,
	}
	return nil
}

func (p *planner) planLRA(lra *logql_parser.LRAOrUnwrap) error {
	duration, err := time.ParseDuration(lra.Time + lra.TimeUnit)
	if err != nil {
		return err
	}
	p.samplesPlanner = &LRAPlanner{
		Main:       p.samplesPlanner,
		Duration:   duration,
		Func:       lra.Fn,
		WithLabels: p.labelsJoinIdx != -1,
	}
	return nil
}

func (p *planner) planUnwrapFn(lra *logql_parser.LRAOrUnwrap) error {
	err := p.planByWithout(lra.ByOrWithoutPrefix, lra.ByOrWithoutSuffix)
	if err != nil {
		return err
	}
	duration, err := time.ParseDuration(lra.Time + lra.TimeUnit)
	if err != nil {
		return err
	}
	p.samplesPlanner = &UnwrapFunctionPlanner{
		Main:     p.samplesPlanner,
		Func:     lra.Fn,
		Duration: duration,
	}
	return nil
}

func (p *planner) planByWithout(byWithout ...*logql_parser.ByOrWithout) error {
	var _byWithout *logql_parser.ByOrWithout
	for _, b := range byWithout {
		if b != nil {
			_byWithout = b
		}
	}
	if _byWithout == nil {
		return nil
	}
	labels := make([]string, len(_byWithout.Labels))
	for i, l := range _byWithout.Labels {
		labels[i] = l.Name
	}

	p.samplesPlanner = &ByWithoutPlanner{
		Main:               p.samplesPlanner,
		Labels:             labels,
		By:                 strings.ToLower(_byWithout.Fn) == "by",
		UseTimeSeriesTable: p.labelsJoinIdx == -1,
		LabelsCache:        &p.labelsCache,
		FPCache:            &p.fpCache,
	}
	return nil
}

func (p *planner) planAgg(agg *logql_parser.AggOperator, withLabels bool) error {
	err := p.planByWithout(agg.ByOrWithoutPrefix, agg.ByOrWithoutSuffix)
	if err != nil {
		return err
	}
	p.samplesPlanner = &AggOpPlanner{
		Main:       p.samplesPlanner,
		Func:       agg.Fn,
		WithLabels: p.labelsJoinIdx != -1 || withLabels,
	}
	return nil
}

func (p *planner) planTopK(topK *logql_parser.TopK) error {
	_len, err := strconv.Atoi(topK.Param)
	if err != nil {
		return err
	}
	p.samplesPlanner = &TopKPlanner{
		Main:  p.samplesPlanner,
		Len:   _len,
		IsTop: topK.Fn == "topk",
	}
	return nil
}

func (p *planner) planQuantileOverTime(script *logql_parser.QuantileOverTime) error {
	err := p.planByWithout(script.ByOrWithoutPrefix, script.ByOrWithoutSuffix)
	if err != nil {
		return err
	}

	param, err := strconv.ParseFloat(script.Param, 64)
	if err != nil {
		return err
	}

	duration, err := time.ParseDuration(script.Time + script.TimeUnit)
	if err != nil {
		return err
	}

	p.samplesPlanner = &QuantilePlanner{
		Main:     p.samplesPlanner,
		Param:    param,
		Duration: duration,
	}
	return nil
}

func (p *planner) planComparison(script *logql_parser.Comparison) error {
	if script == nil {
		return nil
	}
	val, err := strconv.ParseFloat(script.Val, 64)
	if err != nil {
		return err
	}
	p.samplesPlanner = &ComparisonPlanner{
		Main:  p.samplesPlanner,
		Fn:    script.Fn,
		Param: val,
	}
	return nil
}

func (p *planner) planLineFormat(ppl *logql_parser.StrSelectorPipeline) error {
	val, err := ppl.LineFormat.Val.Unquote()
	if err != nil {
		return err
	}

	p.samplesPlanner = &LineFormatPlanner{
		Main:     p.samplesPlanner,
		Template: val,
	}
	return nil

}

func (p *planner) planLabelFilter(ppl *logql_parser.StrSelectorPipeline, idx int) error {
	if p.simpleLabelOperation[idx] {
		return nil
	}
	p.samplesPlanner = &LabelFilterPlanner{
		Expr: ppl.LabelFilter,
		Main: p.samplesPlanner,
	}
	return nil
}

func (p *planner) planLineFilter(ppl *logql_parser.StrSelectorPipeline, idx int) error {
	val, err := ppl.LineFilter.Val.Unquote()
	if err != nil {
		return err
	}
	p.samplesPlanner = &LineFilterPlanner{
		Op:   ppl.LineFilter.Fn,
		Val:  val,
		Main: p.samplesPlanner,
	}
	return nil
}

func (p *planner) planUnwrap(ppl *logql_parser.StrSelectorPipeline) error {
	p.samplesPlanner = &UnwrapPlanner{
		Main:               p.samplesPlanner,
		Label:              ppl.Unwrap.Label.Name,
		UseTimeSeriesTable: p.fastUnwrap,
		labelsCache:        &p.labelsCache,
		fpCache:            &p.fpCache,
	}
	return nil
}

func (p *planner) planParser(ppl *logql_parser.StrSelectorPipeline) error {
	var (
		labels []string
		vals   []string
	)
	for _, p := range ppl.Parser.ParserParams {
		label := ""
		if p.Label != nil {
			label = p.Label.Name
		}
		labels = append(labels, label)

		val, err := p.Val.Unquote()
		if err != nil {
			return err
		}
		vals = append(vals, val)
	}
	p.samplesPlanner = &ParserPlanner{
		Op:     ppl.Parser.Fn,
		labels: labels,
		Vals:   vals,
		Main:   p.samplesPlanner,
	}
	return nil
}

func (p *planner) check() error {
	if p.script.Macros != nil {
		return &shared.NotSupportedError{"not implemented"}
	}
	return nil
}

func getPipeline(script *logql_parser.LogQLScript) []logql_parser.StrSelectorPipeline {
	return getStreamSelector(script).Pipelines
}

func getStreamSelector(script *logql_parser.LogQLScript) *logql_parser.StrSelector {
	if script.StrSelector != nil {
		return script.StrSelector
	}
	if script.LRAOrUnwrap != nil {
		return &script.LRAOrUnwrap.StrSel
	}
	if script.AggOperator != nil {
		return &script.AggOperator.LRAOrUnwrap.StrSel
	}
	if script.TopK != nil {
		if script.TopK.LRAOrUnwrap != nil {
			return &script.TopK.LRAOrUnwrap.StrSel
		}
		if script.TopK.AggOperator != nil {
			return &script.TopK.AggOperator.LRAOrUnwrap.StrSel
		}
		return &script.TopK.QuantileOverTime.StrSel
	}
	if script.QuantileOverTime != nil {
		return &script.QuantileOverTime.StrSel
	}
	return nil
}

func getLabelsAndValuesFromDrop(drop *logql_parser.Drop) ([]string, []string, error) {
	labels := make([]string, len(drop.Params))
	vals := make([]string, len(drop.Params))
	for i, l := range drop.Params {
		labels[i] = l.Label.Name
		var (
			err error
			val string
		)
		if l.Val != nil {
			val, err = l.Val.Unquote()
			if err != nil {
				return nil, nil, err
			}
		}
		vals[i] = val
	}
	return labels, vals, nil
}
