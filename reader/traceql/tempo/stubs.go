// Copyright (c) Grafana Labs
// SPDX-License-Identifier: AGPL-3.0-only
// Forked from github.com/grafana/tempo/pkg/traceql

package tempo

import "fmt"

// firstStageElement represents metrics aggregation operations.
// This is a stub interface - actual metrics implementation is in traceql_metrics_engine.
type firstStageElement interface {
	Element
	extractConditions(request *FetchSpansRequest)
}

// secondStageElement represents second-stage metrics operations (topk, bottomk).
// This is a stub interface - actual metrics implementation is in traceql_metrics_engine.
type secondStageElement interface {
	Element
}

// Sampler interface for trace/span sampling.
// This is a stub - sampling is handled differently in gigapipe.
type Sampler interface {
	Sample(s Span) bool
	FinalScalingFactor() float64
	Measured()
}

// branchOptimizer is a stub for branch optimization (used in engine).
type branchOptimizer struct{}

// MetricsAggregate is a stub for metrics aggregation.
// Actual implementation is in traceql_metrics_engine.
type MetricsAggregate struct {
	op             MetricsAggregateOp
	by             []Attribute
	attr           Attribute
	floatizeAttr   Attribute
	exemplarAttr   Attribute
	targetExemplar Attribute
	floats         []float64 // quantile values for quantile_over_time
}

func (m MetricsAggregate) validate() error {
	return nil
}

func (m MetricsAggregate) extractConditions(request *FetchSpansRequest) {
	// Stub - extraction done in SQL transpiler
}

// Floats returns the float values (e.g., quantile values for quantile_over_time).
func (m MetricsAggregate) Floats() []float64 {
	return m.floats
}

// By returns the grouping attributes.
func (m MetricsAggregate) By() []Attribute {
	return m.by
}

// Op returns the metrics aggregate operation type.
func (m MetricsAggregate) Op() MetricsAggregateOp {
	return m.op
}

// TopKBottomK is a stub for topk/bottomk operations.
type TopKBottomK struct {
	byExpr  FieldExpression
	orderBy TopKOperator
	k       int
}

type TopKOperator int

const (
	OpTopK TopKOperator = iota
	OpBottomK
)

func (t TopKBottomK) String() string {
	if t.orderBy == OpTopK {
		return fmt.Sprintf("topk(%d)", t.k)
	}
	return fmt.Sprintf("bottomk(%d)", t.k)
}

func (t TopKBottomK) validate() error {
	return nil
}

// K returns the number of results to return.
func (t TopKBottomK) K() int {
	return t.k
}

// OrderBy returns the ordering operator (OpTopK or OpBottomK).
func (t TopKBottomK) OrderBy() TopKOperator {
	return t.orderBy
}

// Stub evaluate methods for PipelineElement implementations.
// These are not used - gigapipe uses SQL transpiler instead of in-memory evaluation.

func (o SpansetOperation) evaluate(input []*Spanset) ([]*Spanset, error) {
	return nil, fmt.Errorf("evaluate not implemented - use SQL transpiler")
}

func (a Aggregate) evaluate(input []*Spanset) ([]*Spanset, error) {
	return nil, fmt.Errorf("evaluate not implemented - use SQL transpiler")
}

func (o SelectOperation) evaluate(input []*Spanset) ([]*Spanset, error) {
	return nil, fmt.Errorf("evaluate not implemented - use SQL transpiler")
}

func (o CoalesceOperation) evaluate(input []*Spanset) ([]*Spanset, error) {
	return nil, fmt.Errorf("evaluate not implemented - use SQL transpiler")
}

func (o ScalarFilter) evaluate(input []*Spanset) ([]*Spanset, error) {
	return nil, fmt.Errorf("evaluate not implemented - use SQL transpiler")
}

func (o GroupOperation) evaluate(input []*Spanset) ([]*Spanset, error) {
	return nil, fmt.Errorf("evaluate not implemented - use SQL transpiler")
}

// Stub execute methods for FieldExpression implementations.
// These are not used - gigapipe uses SQL transpiler instead of in-memory evaluation.

func (o *BinaryOperation) execute(span Span) (Static, error) {
	return Static{}, fmt.Errorf("execute not implemented - use SQL transpiler")
}

func (o UnaryOperation) execute(span Span) (Static, error) {
	return Static{}, fmt.Errorf("execute not implemented - use SQL transpiler")
}

func (s Static) execute(span Span) (Static, error) {
	return s, nil // Static just returns itself
}

func (a Attribute) execute(span Span) (Static, error) {
	return Static{}, fmt.Errorf("execute not implemented - use SQL transpiler")
}

// Stub extractConditions methods for FieldExpression implementations.

func (o *BinaryOperation) extractConditions(request *FetchSpansRequest) {}
func (o UnaryOperation) extractConditions(request *FetchSpansRequest)   {}
func (s Static) extractConditions(request *FetchSpansRequest)           {}
func (a Attribute) extractConditions(request *FetchSpansRequest)        {}

// Stub extractConditions for other PipelineElement types.

func (f SpansetFilter) extractConditions(request *FetchSpansRequest) {
	if f.Expression != nil {
		f.Expression.extractConditions(request)
	}
}

func (o SelectOperation) extractConditions(request *FetchSpansRequest) {}

// Attrs returns the list of attributes to select.
func (o SelectOperation) Attrs() []Attribute {
	return o.attrs
}

// newBranchPredictor is a stub for branch prediction optimization.
func newBranchPredictor(_, _ int) branchOptimizer {
	return branchOptimizer{}
}

// Constructors for MetricsAggregate - used by parser.
func newMetricsAggregate(agg MetricsAggregateOp, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{op: agg, by: by}
}

func newMetricsAggregateWithAttr(agg MetricsAggregateOp, attr Attribute, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{op: agg, attr: attr, by: by}
}

func newMetricsAggregateQuantileOverTime(attr Attribute, qs []float64, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{op: metricsAggregateQuantileOverTime, attr: attr, by: by, floats: qs}
}

func newMetricsAggregateWithAttrAndFn(agg MetricsAggregateOp, attr Attribute, fn Attribute, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{op: agg, attr: attr, floatizeAttr: fn, by: by}
}


// MetricsCompare is a stub for compare() operation.
type MetricsCompare struct {
	f     *SpansetFilter
	topN  int
	start int
	end   int
}

func (m *MetricsCompare) String() string {
	return "compare(...)"
}

func (m *MetricsCompare) validate() error {
	return nil
}

func (m *MetricsCompare) extractConditions(request *FetchSpansRequest) {
	// Stub - extraction done in SQL transpiler
}

// Filter returns the spanset filter for compare().
func (m *MetricsCompare) Filter() *SpansetFilter {
	return m.f
}

// TopN returns the number of top results to return.
func (m *MetricsCompare) TopN() int {
	return m.topN
}

// Start returns the start time offset in seconds.
func (m *MetricsCompare) Start() int {
	return m.start
}

// End returns the end time offset in seconds.
func (m *MetricsCompare) End() int {
	return m.end
}

// newMetricsCompare creates a new compare() operation.
func newMetricsCompare(f *SpansetFilter, topN, start, end int) *MetricsCompare {
	return &MetricsCompare{f: f, topN: topN, start: start, end: end}
}

// newTopKBottomK creates a new topk/bottomk operation.
func newTopKBottomK(orderBy TopKOperator, k int) *TopKBottomK {
	return &TopKBottomK{k: k, orderBy: orderBy}
}

// newAverageOverTimeMetricsAggregator creates avg_over_time aggregator.
// This is a stub - actual implementation in metrics engine.
func newAverageOverTimeMetricsAggregator(attr Attribute, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{op: metricsAggregateAvgOverTime, attr: attr, by: by}
}

