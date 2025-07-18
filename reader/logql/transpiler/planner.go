package transpiler

import (
	"reflect"

	"github.com/metrico/qryn/reader/logql/parser"
	"github.com/metrico/qryn/reader/logql/transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/transpiler/internal/planner"
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	"github.com/metrico/qryn/reader/plugins"
)

const (
	BreakpointNo  = -1
	BreakpointLra = -2
)

func Plan(script *parser.LogQLScript) (shared.RequestProcessorChain, error) {
	cancelJsonAndLogFmt(script)
	for _, plugin := range plugins.GetLogQLPlannerPlugins() {
		res, err := plugin.Plan(script)
		if err == nil {
			return res, nil
		}
	}

	breakpoint, err := GetBreakpoint(script)
	if err != nil {
		return nil, err
	}

	var proc shared.RequestProcessor
	if breakpoint == BreakpointNo || clickhouse_planner.AnalyzeMetrics15sShortcut(script) {
		plan, err := clickhouse_planner.Plan(script, true)
		if err != nil {
			return nil, err
		}

		proc = &shared.ClickhouseGetterPlanner{
			ClickhouseRequestPlanner: plan,
			Matrix:                   script.StrSelector == nil,
		}

	} else {
		chScript, internalScript, err := breakScript(breakpoint, script, script)
		if err != nil {
			return nil, err
		}
		plan, err := clickhouse_planner.Plan(chScript, false)
		if err != nil {
			return nil, err
		}
		proc = &shared.ClickhouseGetterPlanner{
			ClickhouseRequestPlanner: plan,
			Matrix:                   chScript.StrSelector == nil,
		}

		proc, err = planner.Plan(internalScript, proc)
		if err != nil {
			return nil, err
		}
	}

	proc, err = MatrixPostProcessors(script, proc)
	return shared.RequestProcessorChain{proc}, err
}

func MatrixPostProcessors(script *parser.LogQLScript,
	proc shared.RequestProcessor) (shared.RequestProcessor, error) {
	if !proc.IsMatrix() {
		return proc, nil
	}
	duration, err := shared.GetDuration(script)
	if err != nil {
		return nil, err
	}
	proc = &ZeroEaterPlanner{planner.GenericPlanner{Main: proc}}
	proc = &FixPeriodPlanner{
		Main:     proc,
		Duration: duration,
	}
	return proc, nil
}

func PlanFingerprints(script *parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return clickhouse_planner.PlanFingerprints(script)
}

func PlanDetectLabels(script *parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return clickhouse_planner.PlanDetectLabels(script)
}

func PlanPatterns(script *parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return clickhouse_planner.PlanPatterns(script)
}

func GetBreakpoint(node any) (int, error) {
	dfs := func(node ...any) (int, error) {
		for _, n := range node {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				return GetBreakpoint(n)
			}
		}
		return BreakpointNo, nil
	}

	switch script := node.(type) {
	case *parser.LogQLScript:
		return dfs(script.TopK, script.QuantileOverTime, script.AggOperator, script.LRAOrUnwrap,
			script.StrSelector)
	case *parser.TopK:
		return dfs(script.QuantileOverTime, script.AggOperator, script.LRAOrUnwrap)
	case *parser.QuantileOverTime:
		return dfs(&script.StrSel)
	case *parser.AggOperator:
		return dfs(&script.LRAOrUnwrap)
	case *parser.LRAOrUnwrap:
		bp, err := dfs(&script.StrSel)
		if script.Fn == "absent_over_time" && bp < 0 && err == nil {
			return BreakpointLra, nil
		}
		return bp, err
	case *parser.StrSelector:
		for i, ppl := range script.Pipelines {
			if ppl.Parser != nil &&
				((ppl.Parser.Fn == "json" && len(ppl.Parser.ParserParams) == 0) ||
					ppl.Parser.Fn == "logfmt") {
				return i, nil
			}
			if ppl.LineFormat != nil {
				return i, nil
			}
		}
		return BreakpointNo, nil
	}
	return BreakpointNo, nil
}

// TODO: this should be replaced wit a semistructured log ingestor
func cancelJsonAndLogFmt(script *parser.LogQLScript) {
	strSel := shared.GetStrSelector(script)
	for i := len(strSel.Pipelines) - 2; i >= 0; i-- {
		ppl := &strSel.Pipelines[i]
		if ppl.Parser != nil && strSel.Pipelines[i+1].Parser != nil &&
			ppl.Parser.Fn == "json" && strSel.Pipelines[i+1].Parser.Fn == "logfmt" {
			copy(strSel.Pipelines[i:], strSel.Pipelines[i+2:])
			strSel.Pipelines = strSel.Pipelines[:len(strSel.Pipelines)-2]
		}
	}
}

func breakScript(breakpoint int, script *parser.LogQLScript,
	node any) (*parser.LogQLScript, *parser.LogQLScript, error) {
	dfs := func(node ...any) (*parser.LogQLScript, *parser.LogQLScript, error) {
		for _, n := range node {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				return breakScript(breakpoint, script, n)
			}
		}
		return script, nil, nil
	}
	switch _script := node.(type) {
	case *parser.LogQLScript:
		return dfs(_script.TopK, _script.AggOperator, _script.StrSelector, _script.LRAOrUnwrap,
			_script.QuantileOverTime)
	case *parser.TopK:
		return nil, nil, &shared.NotSupportedError{Msg: "TopK is not supported for this query"}
	case *parser.AggOperator:
		return dfs(&_script.LRAOrUnwrap)
	case *parser.StrSelector:
		if breakpoint < 0 {
			return script, nil, nil
		}
		chScript := &parser.LogQLScript{
			StrSelector: &parser.StrSelector{
				StrSelCmds: _script.StrSelCmds,
				Pipelines:  _script.Pipelines[:breakpoint],
			},
		}
		_script.Pipelines = _script.Pipelines[breakpoint:]
		return chScript, script, nil
	case *parser.LRAOrUnwrap:
		if breakpoint != BreakpointLra {
			return dfs(&_script.StrSel)
		}
		chScript := &parser.LogQLScript{
			StrSelector: &parser.StrSelector{
				StrSelCmds: _script.StrSel.StrSelCmds,
				Pipelines:  _script.StrSel.Pipelines,
			},
		}
		_script.StrSel = parser.StrSelector{}
		return chScript, script, nil
	case *parser.QuantileOverTime:
		return nil, nil, &shared.NotSupportedError{Msg: "QuantileOverTime is not supported for this query"}
	}
	return nil, nil, nil
}
