package logql_transpiler

import (
	"fmt"
	"reflect"
	"time"

	log_parser "github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/internal/planner"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
)

const (
	BreakpointNo  = -1
	BreakpointLra = -2
)

type planMode int

const (
	// planModeClickhouse means the expression can be fully evaluated inside ClickHouse.
	planModeClickhouse planMode = iota
	// planModeInternal means at least part of the expression must be evaluated in Go
	// (e.g. json/logfmt parsing, absent_over_time, or a binary operand that itself
	// needs internal processing).
	planModeInternal
)

// decidePlanMode returns the execution mode for any LogQLScript.
// For binary expressions every operand is checked recursively; the expression
// runs internally as soon as any operand requires it.
func decidePlanMode(script *log_parser.LogQLScript) planMode {
	script = unwrapParens(script)
	if script.IsBinary() {
		for _, atom := range allBinaryAtoms(script) {
			if decidePlanModeAtom(atom) == planModeInternal {
				return planModeInternal
			}
		}
		return planModeClickhouse
	}
	bp, _ := GetBreakpoint(script)
	if bp == BreakpointNo || clickhouse_planner.AnalyzeMetrics15sShortcut(script) {
		return planModeClickhouse
	}
	return planModeInternal
}

func decidePlanModeAtom(atom log_parser.AtomExpr) planMode {
	if atom.Scalar != "" {
		return planModeClickhouse
	}
	if atom.Paren != nil {
		return decidePlanMode(atom.Paren)
	}
	return decidePlanMode(&log_parser.LogQLScript{Head: atom})
}

func allBinaryAtoms(script *log_parser.LogQLScript) []log_parser.AtomExpr {
	atoms := make([]log_parser.AtomExpr, 0, 1+len(script.BinOps))
	atoms = append(atoms, script.Head)
	for _, b := range script.BinOps {
		atoms = append(atoms, b.Right)
	}
	return atoms
}

func unwrapParens(script *log_parser.LogQLScript) *log_parser.LogQLScript {
	for !script.IsBinary() && script.Head.Paren != nil {
		script = script.Head.Paren
	}
	return script
}

// validateBinaryOps returns an error if any operand of a binary expression is a
// raw log stream selector. Arithmetic can only be applied to matrix (metric) outputs.
func validateBinaryOps(script *log_parser.LogQLScript) error {
	if !script.IsBinary() {
		return nil
	}
	operands := make([]log_parser.AtomExpr, 0, 1+len(script.BinOps))
	operands = append(operands, script.Head)
	for _, b := range script.BinOps {
		operands = append(operands, b.Right)
	}
	for _, atom := range operands {
		if err := validateAtomForBinaryOp(atom); err != nil {
			return err
		}
	}
	return nil
}

func validateAtomForBinaryOp(a log_parser.AtomExpr) error {
	if a.Paren != nil {
		inner := unwrapParens(a.Paren)
		if err := validateBinaryOps(inner); err != nil {
			return err
		}
		if !inner.IsBinary() {
			return validateAtomForBinaryOp(inner.Head)
		}
		return nil
	}
	if a.StrSelector != nil {
		return fmt.Errorf("syntax error: binary arithmetic cannot be applied to a log stream selector; wrap it in a range aggregation such as rate() or count_over_time() first")
	}
	return nil
}

func Plan(script *log_parser.LogQLScript) (shared.RequestProcessorChain, error) {
	script = unwrapParens(script)
	if err := validateBinaryOps(script); err != nil {
		return nil, err
	}

	mode := decidePlanMode(script)

	if script.IsBinary() {
		if mode == planModeClickhouse {
			return planBinaryExpr(script)
		}
		return planBinaryExprRAM(script)
	}

	// cancelJsonAndLogFmt mutates the pipeline list, so the plan mode decided
	// above may no longer hold: dropping a `json | logfmt` pair can remove the
	// only stage that forced internal (Go-side) evaluation. Re-derive it, or
	// breakScript would hand internal/planner.Plan a nil script.
	cancelJsonAndLogFmt(script)
	mode = decidePlanMode(script)

	for _, plugin := range plugins.GetLogQLPlannerPlugins() {
		res, err := plugin.Plan(script)
		if err == nil {
			return res, nil
		}
	}

	var (
		proc shared.RequestProcessor
		err  error
	)
	if mode == planModeClickhouse {
		plan, err := clickhouse_planner.Plan(script, true)
		if err != nil {
			return nil, err
		}
		proc = &shared.ClickhouseGetterPlanner{
			ClickhouseRequestPlanner: plan,
			Matrix:                   script.Head.StrSelector == nil,
		}
	} else {
		breakpoint, err := GetBreakpoint(script)
		if err != nil {
			return nil, err
		}
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
			Matrix:                   chScript.Head.StrSelector == nil,
		}
		proc, err = planner.Plan(internalScript, proc)
		if err != nil {
			return nil, err
		}
	}

	if proc.IsMatrix() {
		proc, err = gridPostProcessors(script, proc)
	}
	return shared.RequestProcessorChain{proc}, err
}

// gridPostProcessors evaluates a range aggregation on the request's
// evaluation grid.
func gridPostProcessors(script *log_parser.LogQLScript,
	proc shared.RequestProcessor) (shared.RequestProcessor, error) {
	duration, err := shared.GetDuration(script)
	if err != nil {
		return nil, err
	}
	offset, err := shared.GetOffset(script)
	if err != nil {
		return nil, err
	}
	return &GridPlanner{Main: proc, Duration: duration, Offset: offset}, nil
}

func PlanLabels(scripts []*log_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	for _, script := range scripts {
		if script.Head.StrSelector == nil {
			return nil, fmt.Errorf("unsupported query")
		}
	}
	return clickhouse_planner.PlanLabels(scripts)
}

func PlanFingerprints(script *log_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return clickhouse_planner.PlanFingerprints(script)
}

func PlanDetectLabels(script *log_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	return clickhouse_planner.PlanDetectLabels(script)
}

func PlanPatterns(script *log_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
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
	case *log_parser.LogQLScript:
		return dfs(script.Head.TopK, script.Head.QuantileOverTime, script.Head.AggOperator, script.Head.LRAOrUnwrap,
			script.Head.StrSelector)
	case *log_parser.TopK:
		return dfs(script.QuantileOverTime, script.AggOperator, script.LRAOrUnwrap)
	case *log_parser.QuantileOverTime:
		return dfs(&script.StrSel)
	case *log_parser.AggOperator:
		return dfs(&script.LRAOrUnwrap)
	case *log_parser.LRAOrUnwrap:
		bp, err := dfs(&script.StrSel)
		if script.Fn == "absent_over_time" && bp < 0 && err == nil {
			return BreakpointLra, nil
		}
		return bp, err
	case *log_parser.StrSelector:
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
func cancelJsonAndLogFmt(script *log_parser.LogQLScript) {
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

// planBinaryExpr plans a binary of SQL operands as one SQL query on the grid.
func planBinaryExpr(script *log_parser.LogQLScript) (shared.RequestProcessorChain, error) {
	sqlPlanner, err := planScriptToSQL(script)
	if err != nil {
		return nil, err
	}
	proc, err := binaryGridPlanner(binaryLeaves(script), &shared.ClickhouseGetterPlanner{
		ClickhouseRequestPlanner: sqlPlanner,
		Matrix:                   true,
	})
	if err != nil {
		return nil, err
	}
	return shared.RequestProcessorChain{proc}, nil
}

// binaryLeaves returns the non-scalar operands of a binary expression,
// nested binaries flattened.
func binaryLeaves(script *log_parser.LogQLScript) []*log_parser.LogQLScript {
	script = unwrapParens(script)
	if !script.IsBinary() {
		return []*log_parser.LogQLScript{script}
	}
	var res []*log_parser.LogQLScript
	for _, atom := range allBinaryAtoms(script) {
		switch {
		case atom.Scalar != "":
		case atom.Paren != nil:
			res = append(res, binaryLeaves(atom.Paren)...)
		default:
			res = append(res, &log_parser.LogQLScript{Head: atom})
		}
	}
	return res
}

// binaryGridPlanner puts proc on the grid with a read range that covers the
// range and offset of every leaf.
func binaryGridPlanner(leaves []*log_parser.LogQLScript,
	proc shared.RequestProcessor) (shared.RequestProcessor, error) {
	var reach, offset time.Duration
	for i, leaf := range leaves {
		r, err := shared.GetDuration(leaf)
		if err != nil {
			return nil, err
		}
		o, err := shared.GetOffset(leaf)
		if err != nil {
			return nil, err
		}
		if i == 0 || o < offset {
			offset = o
		}
		reach = max(reach, r+o)
	}
	return &GridPlanner{Main: proc, Duration: reach - offset, Offset: offset}, nil
}

// planScriptToSQL converts any LogQLScript (binary or not) into a SQLRequestPlanner.
// For nested binary expressions it recursively builds a BinaryExprSQLPlanner tree.
func planScriptToSQL(script *log_parser.LogQLScript) (shared.SQLRequestPlanner, error) {
	script = unwrapParens(script)
	if !script.IsBinary() {
		return clickhouse_planner.Plan(script, true)
	}
	current, err := planAtomToSQL(script.Head)
	if err != nil {
		return nil, err
	}
	for _, binOp := range script.BinOps {
		if binOp.Right.Scalar != "" {
			current = &clickhouse_planner.BinaryExprSQLPlanner{
				Left:        current,
				Op:          binOp.Op,
				RightScalar: binOp.Right.Scalar,
			}
			continue
		}
		right, err := planAtomToSQL(binOp.Right)
		if err != nil {
			return nil, err
		}
		current = &clickhouse_planner.BinaryExprSQLPlanner{
			Left:  current,
			Op:    binOp.Op,
			Right: right,
		}
	}
	return current, nil
}

// planAtomToSQL converts a single AtomExpr into a SQLRequestPlanner.
func planAtomToSQL(atom log_parser.AtomExpr) (shared.SQLRequestPlanner, error) {
	if atom.Paren != nil {
		return planScriptToSQL(atom.Paren)
	}
	return clickhouse_planner.Plan(&log_parser.LogQLScript{Head: atom}, true)
}

func breakScript(breakpoint int, script *log_parser.LogQLScript,
	node any) (*log_parser.LogQLScript, *log_parser.LogQLScript, error) {
	dfs := func(node ...any) (*log_parser.LogQLScript, *log_parser.LogQLScript, error) {
		for _, n := range node {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				return breakScript(breakpoint, script, n)
			}
		}
		return script, nil, nil
	}
	switch _script := node.(type) {
	case *log_parser.LogQLScript:
		return dfs(_script.Head.TopK, _script.Head.AggOperator, _script.Head.StrSelector, _script.Head.LRAOrUnwrap,
			_script.Head.QuantileOverTime)
	case *log_parser.TopK:
		return nil, nil, &shared.NotSupportedError{Msg: "TopK is not supported for this query"}
	case *log_parser.AggOperator:
		return dfs(&_script.LRAOrUnwrap)
	case *log_parser.StrSelector:
		if breakpoint < 0 {
			return script, nil, nil
		}
		chScript := &log_parser.LogQLScript{
			Head: log_parser.AtomExpr{
				StrSelector: &log_parser.StrSelector{
					StrSelCmds: _script.StrSelCmds,
					Pipelines:  _script.Pipelines[:breakpoint],
				},
			},
		}
		_script.Pipelines = _script.Pipelines[breakpoint:]
		return chScript, script, nil
	case *log_parser.LRAOrUnwrap:
		if breakpoint != BreakpointLra {
			return dfs(&_script.StrSel)
		}
		chScript := &log_parser.LogQLScript{
			Head: log_parser.AtomExpr{
				StrSelector: &log_parser.StrSelector{
					StrSelCmds: _script.StrSel.StrSelCmds,
					Pipelines:  _script.StrSel.Pipelines,
				},
			},
		}
		_script.StrSel = log_parser.StrSelector{StrSelCmds: _script.StrSel.StrSelCmds}
		return chScript, script, nil
	case *log_parser.QuantileOverTime:
		return dfs(&_script.StrSel)
	}
	return nil, nil, nil
}
