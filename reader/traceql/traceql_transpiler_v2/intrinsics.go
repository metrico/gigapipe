package traceql_transpiler_v2

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/traceql/tempo"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// planIntrinsicComparison handles comparisons with intrinsic attributes.
func (p *TempoPlanner) planIntrinsicComparison(attr tempo.Attribute, op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	switch attr.Intrinsic {
	case tempo.IntrinsicDuration:
		return p.planDurationComparison(op, value)
	case tempo.IntrinsicName:
		return p.planNameComparison(op, value)
	case tempo.IntrinsicStatus:
		return p.planStatusComparison(op, value)
	case tempo.IntrinsicNestedSetParent:
		return p.planNestedSetParentComparison(op, value)
	case tempo.IntrinsicStatusMessage:
		return p.planStatusMessageComparison(op, value)
	case tempo.IntrinsicKind:
		return p.planKindComparison(op, value)
	case tempo.IntrinsicTraceID:
		return p.planTraceIDComparison(op, value)
	case tempo.IntrinsicSpanID:
		return p.planSpanIDComparison(op, value)
	case tempo.IntrinsicTraceDuration:
		return p.planTraceDurationComparison(op, value)
	case tempo.IntrinsicTraceRootService:
		return p.planTraceRootServiceComparison(op, value)
	case tempo.IntrinsicTraceRootSpan:
		return p.planTraceRootSpanComparison(op, value)
	default:
		return nil, fmt.Errorf("unsupported intrinsic: %v", attr.Intrinsic)
	}
}

// planDurationComparison handles duration comparisons.
func (p *TempoPlanner) planDurationComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	static, ok := value.(tempo.Static)
	if !ok {
		return nil, fmt.Errorf("duration comparison requires static value")
	}

	dur, ok := static.Duration()
	if !ok {
		return nil, fmt.Errorf("duration comparison requires duration value")
	}

	return &DurationComparisonPlanner{
		Op:        op,
		DurationNs: dur.Nanoseconds(),
	}, nil
}

// DurationComparisonPlanner handles duration comparisons.
type DurationComparisonPlanner struct {
	Op         tempo.Operator
	DurationNs int64
}

func (d *DurationComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	compFn := getComparisonFn(d.Op)
	if compFn == nil {
		return nil, fmt.Errorf("unsupported operator for duration: %v", d.Op)
	}
	return compFn(sql.NewRawObject("traces_idx.duration"), sql.NewIntVal(d.DurationNs)), nil
}

func (d *DurationComparisonPlanner) NeedsAttributeJoin() bool  { return false }
func (d *DurationComparisonPlanner) GetAttributeKeys() []string { return nil }

// planNameComparison handles name comparisons.
func (p *TempoPlanner) planNameComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	strVal, err := p.getStaticString(value)
	if err != nil {
		return nil, err
	}

	return &NameComparisonPlanner{
		Op:    op,
		Value: strVal,
	}, nil
}

// NameComparisonPlanner handles name comparisons.
type NameComparisonPlanner struct {
	Op    tempo.Operator
	Value string
}

func (n *NameComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	switch n.Op {
	case tempo.OpEqual:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal("name")),
			sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(n.Value)),
		), nil
	case tempo.OpNotEqual:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal("name")),
			sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(n.Value)),
		), nil
	case tempo.OpRegex:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal("name")),
			sql.Eq(&matchRe{sql.NewRawObject("val"), n.Value}, sql.NewIntVal(1)),
		), nil
	case tempo.OpNotRegex:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal("name")),
			sql.Eq(&matchRe{sql.NewRawObject("val"), n.Value}, sql.NewIntVal(0)),
		), nil
	default:
		return nil, fmt.Errorf("unsupported operator for name: %v", n.Op)
	}
}

func (n *NameComparisonPlanner) NeedsAttributeJoin() bool   { return true }
func (n *NameComparisonPlanner) GetAttributeKeys() []string { return []string{"name"} }

// planStatusComparison handles status comparisons.
func (p *TempoPlanner) planStatusComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	static, ok := value.(tempo.Static)
	if !ok {
		return nil, fmt.Errorf("status comparison requires static value")
	}

	var statusStr string
	if status, ok := static.Status(); ok {
		statusStr = statusToString(status)
	} else {
		// Try to get as string
		str := static.EncodeToString(false)
		statusStr = str
	}

	return &StatusComparisonPlanner{
		Op:    op,
		Value: statusStr,
	}, nil
}

// statusToString converts tempo.Status to string.
func statusToString(s tempo.Status) string {
	switch s {
	case tempo.StatusOk:
		return "ok"
	case tempo.StatusError:
		return "error"
	case tempo.StatusUnset:
		return "unset"
	default:
		return "unset"
	}
}

// StatusComparisonPlanner handles status comparisons.
type StatusComparisonPlanner struct {
	Op    tempo.Operator
	Value string
}

func (s *StatusComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	switch s.Op {
	case tempo.OpEqual:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal("status")),
			sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(s.Value)),
		), nil
	case tempo.OpNotEqual:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal("status")),
			sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(s.Value)),
		), nil
	default:
		return nil, fmt.Errorf("unsupported operator for status: %v", s.Op)
	}
}

func (s *StatusComparisonPlanner) NeedsAttributeJoin() bool   { return true }
func (s *StatusComparisonPlanner) GetAttributeKeys() []string { return []string{"status"} }

// planNestedSetParentComparison handles nestedSetParent comparisons.
func (p *TempoPlanner) planNestedSetParentComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	static, ok := value.(tempo.Static)
	if !ok {
		return nil, fmt.Errorf("nestedSetParent comparison requires static value")
	}

	intVal, ok := static.Int()
	if !ok {
		return nil, fmt.Errorf("nestedSetParent comparison requires integer value")
	}

	return &NestedSetParentPlanner{
		Op:    op,
		Value: intVal,
	}, nil
}

// NestedSetParentPlanner handles nestedSetParent comparisons.
type NestedSetParentPlanner struct {
	Op    tempo.Operator
	Value int
}

func (n *NestedSetParentPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	// nestedSetParent is used to filter root vs non-root spans
	// nestedSetParent < 0 means root spans (no parent)
	// nestedSetParent >= 0 means non-root spans (has parent)
	//
	// Since parent_id is only in tempo_traces table (not in the index table),
	// we store the filter in context and return TRUE here.
	// The actual filter is applied in TracesDataPlanner.

	var filterType string
	switch n.Op {
	case tempo.OpLess:
		if n.Value <= 0 {
			filterType = "root"
		}
	case tempo.OpLessEqual:
		if n.Value < 0 {
			filterType = "root"
		}
	case tempo.OpGreaterEqual:
		if n.Value >= 0 {
			filterType = "non-root"
		}
	case tempo.OpGreater:
		if n.Value >= -1 {
			filterType = "non-root"
		}
	case tempo.OpEqual:
		if n.Value < 0 {
			filterType = "root"
		} else {
			filterType = "non-root"
		}
	case tempo.OpNotEqual:
		if n.Value < 0 {
			filterType = "non-root"
		}
	}

	if filterType == "" {
		return nil, fmt.Errorf("unsupported nestedSetParent operation: %v %d", n.Op, n.Value)
	}

	// Store the filter in context for later application
	ctx.NestedSetParentFilter = filterType

	// Return TRUE - all spans pass at index stage, filter applied at data stage
	return sql.Eq(sql.NewIntVal(1), sql.NewIntVal(1)), nil
}

func (n *NestedSetParentPlanner) NeedsAttributeJoin() bool  { return false }
func (n *NestedSetParentPlanner) GetAttributeKeys() []string { return nil }

// planStatusMessageComparison handles statusMessage comparisons.
func (p *TempoPlanner) planStatusMessageComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	strVal, err := p.getStaticString(value)
	if err != nil {
		return nil, err
	}

	return &AttributeStringComparisonPlanner{
		Key:   "statusMessage",
		Op:    op,
		Value: strVal,
	}, nil
}

// planKindComparison handles kind comparisons.
func (p *TempoPlanner) planKindComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	static, ok := value.(tempo.Static)
	if !ok {
		return nil, fmt.Errorf("kind comparison requires static value")
	}

	var kindStr string
	if kind, ok := static.Kind(); ok {
		kindStr = kindToString(kind)
	} else {
		kindStr = static.EncodeToString(false)
	}

	return &AttributeStringComparisonPlanner{
		Key:   "kind",
		Op:    op,
		Value: kindStr,
	}, nil
}

// kindToString converts tempo.Kind to string.
func kindToString(k tempo.Kind) string {
	switch k {
	case tempo.KindUnspecified:
		return "unspecified"
	case tempo.KindInternal:
		return "internal"
	case tempo.KindServer:
		return "server"
	case tempo.KindClient:
		return "client"
	case tempo.KindProducer:
		return "producer"
	case tempo.KindConsumer:
		return "consumer"
	default:
		return "unspecified"
	}
}

// planTraceIDComparison handles trace:id comparisons.
func (p *TempoPlanner) planTraceIDComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	strVal, err := p.getStaticString(value)
	if err != nil {
		return nil, err
	}

	return &TraceIDComparisonPlanner{
		Op:    op,
		Value: strVal,
	}, nil
}

// TraceIDComparisonPlanner handles trace:id comparisons.
type TraceIDComparisonPlanner struct {
	Op    tempo.Operator
	Value string
}

func (t *TraceIDComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	switch t.Op {
	case tempo.OpEqual:
		return sql.Eq(sql.NewRawObject("lower(hex(trace_id))"), sql.NewStringVal(t.Value)), nil
	case tempo.OpNotEqual:
		return sql.Neq(sql.NewRawObject("lower(hex(trace_id))"), sql.NewStringVal(t.Value)), nil
	default:
		return nil, fmt.Errorf("unsupported operator for trace:id: %v", t.Op)
	}
}

func (t *TraceIDComparisonPlanner) NeedsAttributeJoin() bool  { return false }
func (t *TraceIDComparisonPlanner) GetAttributeKeys() []string { return nil }

// planSpanIDComparison handles span:id comparisons.
func (p *TempoPlanner) planSpanIDComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	strVal, err := p.getStaticString(value)
	if err != nil {
		return nil, err
	}

	return &SpanIDComparisonPlanner{
		Op:    op,
		Value: strVal,
	}, nil
}

// SpanIDComparisonPlanner handles span:id comparisons.
type SpanIDComparisonPlanner struct {
	Op    tempo.Operator
	Value string
}

func (s *SpanIDComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	switch s.Op {
	case tempo.OpEqual:
		return sql.Eq(sql.NewRawObject("lower(hex(span_id))"), sql.NewStringVal(s.Value)), nil
	case tempo.OpNotEqual:
		return sql.Neq(sql.NewRawObject("lower(hex(span_id))"), sql.NewStringVal(s.Value)), nil
	default:
		return nil, fmt.Errorf("unsupported operator for span:id: %v", s.Op)
	}
}

func (s *SpanIDComparisonPlanner) NeedsAttributeJoin() bool  { return false }
func (s *SpanIDComparisonPlanner) GetAttributeKeys() []string { return nil }

// planTraceDurationComparison handles traceDuration comparisons.
func (p *TempoPlanner) planTraceDurationComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	static, ok := value.(tempo.Static)
	if !ok {
		return nil, fmt.Errorf("traceDuration comparison requires static value")
	}

	dur, ok := static.Duration()
	if !ok {
		return nil, fmt.Errorf("traceDuration comparison requires duration value")
	}

	return &TraceDurationComparisonPlanner{
		Op:         op,
		DurationNs: dur.Nanoseconds(),
	}, nil
}

// TraceDurationComparisonPlanner handles traceDuration comparisons.
type TraceDurationComparisonPlanner struct {
	Op         tempo.Operator
	DurationNs int64
}

func (t *TraceDurationComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	// Build subquery to find trace_ids with matching traceDuration
	// traceDuration = max(timestamp_ns + duration_ns) - min(timestamp_ns)
	table := ctx.TracesTable
	if ctx.IsCluster {
		table = ctx.TracesDistTable
	}

	// Build HAVING condition
	traceDurExpr := sql.NewRawObject("max(timestamp_ns + duration_ns) - min(timestamp_ns)")
	compFn := getComparisonFn(t.Op)
	if compFn == nil {
		return nil, fmt.Errorf("unsupported operator for traceDuration: %v", t.Op)
	}
	havingCond := compFn(traceDurExpr, sql.NewIntVal(t.DurationNs))

	// Build subquery
	subquery := sql.NewSelect().
		Select(sql.NewRawObject("trace_id")).
		From(sql.NewRawObject(table)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
		).
		GroupBy(sql.NewRawObject("trace_id")).
		AndHaving(havingCond)

	return sql.NewIn(sql.NewRawObject("traces_idx.trace_id"), subquery), nil
}

func (t *TraceDurationComparisonPlanner) NeedsAttributeJoin() bool  { return false }
func (t *TraceDurationComparisonPlanner) GetAttributeKeys() []string { return nil }

// planTraceRootServiceComparison handles rootServiceName comparisons.
func (p *TempoPlanner) planTraceRootServiceComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	strVal, err := p.getStaticString(value)
	if err != nil {
		return nil, err
	}

	return &TraceRootServiceComparisonPlanner{
		Op:    op,
		Value: strVal,
	}, nil
}

// TraceRootServiceComparisonPlanner handles rootServiceName comparisons.
type TraceRootServiceComparisonPlanner struct {
	Op    tempo.Operator
	Value string
}

func (t *TraceRootServiceComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	// Build subquery to find trace_ids where root span's service_name matches
	// Root span is identified by parent_id = ''
	table := ctx.TracesTable
	if ctx.IsCluster {
		table = ctx.TracesDistTable
	}

	// Build service_name condition
	var serviceNameCond sql.SQLCondition
	switch t.Op {
	case tempo.OpEqual:
		serviceNameCond = sql.Eq(sql.NewRawObject("service_name"), sql.NewStringVal(t.Value))
	case tempo.OpNotEqual:
		serviceNameCond = sql.Neq(sql.NewRawObject("service_name"), sql.NewStringVal(t.Value))
	case tempo.OpRegex:
		serviceNameCond = sql.Eq(&matchRe{sql.NewRawObject("service_name"), t.Value}, sql.NewIntVal(1))
	case tempo.OpNotRegex:
		serviceNameCond = sql.Eq(&matchRe{sql.NewRawObject("service_name"), t.Value}, sql.NewIntVal(0))
	default:
		return nil, fmt.Errorf("unsupported operator for rootServiceName: %v", t.Op)
	}

	// Build subquery - find trace_ids where root span has matching service_name
	subquery := sql.NewSelect().
		Select(sql.NewRawObject("trace_id")).
		From(sql.NewRawObject(table)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Eq(sql.NewRawObject("parent_id"), sql.NewStringVal("")), // Root span
			serviceNameCond,
		)

	return sql.NewIn(sql.NewRawObject("traces_idx.trace_id"), subquery), nil
}

func (t *TraceRootServiceComparisonPlanner) NeedsAttributeJoin() bool  { return false }
func (t *TraceRootServiceComparisonPlanner) GetAttributeKeys() []string { return nil }

// planTraceRootSpanComparison handles rootName comparisons.
func (p *TempoPlanner) planTraceRootSpanComparison(op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	strVal, err := p.getStaticString(value)
	if err != nil {
		return nil, err
	}

	return &TraceRootSpanComparisonPlanner{
		Op:    op,
		Value: strVal,
	}, nil
}

// TraceRootSpanComparisonPlanner handles rootName comparisons.
type TraceRootSpanComparisonPlanner struct {
	Op    tempo.Operator
	Value string
}

func (t *TraceRootSpanComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	// Build subquery to find trace_ids where root span's name matches
	// Root span is identified by parent_id = ''
	table := ctx.TracesTable
	if ctx.IsCluster {
		table = ctx.TracesDistTable
	}

	// Build name condition
	var nameCond sql.SQLCondition
	switch t.Op {
	case tempo.OpEqual:
		nameCond = sql.Eq(sql.NewRawObject("name"), sql.NewStringVal(t.Value))
	case tempo.OpNotEqual:
		nameCond = sql.Neq(sql.NewRawObject("name"), sql.NewStringVal(t.Value))
	case tempo.OpRegex:
		nameCond = sql.Eq(&matchRe{sql.NewRawObject("name"), t.Value}, sql.NewIntVal(1))
	case tempo.OpNotRegex:
		nameCond = sql.Eq(&matchRe{sql.NewRawObject("name"), t.Value}, sql.NewIntVal(0))
	default:
		return nil, fmt.Errorf("unsupported operator for rootName: %v", t.Op)
	}

	// Build subquery - find trace_ids where root span has matching name
	subquery := sql.NewSelect().
		Select(sql.NewRawObject("trace_id")).
		From(sql.NewRawObject(table)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Eq(sql.NewRawObject("parent_id"), sql.NewStringVal("")), // Root span
			nameCond,
		)

	return sql.NewIn(sql.NewRawObject("traces_idx.trace_id"), subquery), nil
}

func (t *TraceRootSpanComparisonPlanner) NeedsAttributeJoin() bool  { return false }
func (t *TraceRootSpanComparisonPlanner) GetAttributeKeys() []string { return nil }

// Helper functions

// getStaticString extracts string value from a FieldExpression.
func (p *TempoPlanner) getStaticString(value tempo.FieldExpression) (string, error) {
	static, ok := value.(tempo.Static)
	if !ok {
		return "", fmt.Errorf("expected static value, got %T", value)
	}

	return static.EncodeToString(false), nil
}

// getComparisonFn returns the SQL comparison function for a Tempo operator.
func getComparisonFn(op tempo.Operator) func(sql.SQLObject, sql.SQLObject) *sql.LogicalOp {
	switch op {
	case tempo.OpEqual:
		return sql.Eq
	case tempo.OpNotEqual:
		return sql.Neq
	case tempo.OpGreater:
		return sql.Gt
	case tempo.OpGreaterEqual:
		return sql.Ge
	case tempo.OpLess:
		return sql.Lt
	case tempo.OpLessEqual:
		return sql.Le
	default:
		return nil
	}
}

// matchRe helper for regex matching.
type matchRe struct {
	field sql.SQLObject
	re    string
}

func (m matchRe) String(ctx *sql.Ctx, options ...int) (string, error) {
	field, err := m.field.String(ctx, options...)
	if err != nil {
		return "", err
	}
	strRe, err := sql.NewStringVal(m.re).String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("match(%s,%s)", field, strRe), nil
}
