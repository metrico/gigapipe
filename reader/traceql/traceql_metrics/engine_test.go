package traceql_metrics

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v4/reader/traceql/tempo"
)

func TestExtractQuantileValue(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{
			name:     "quantile 0.9",
			query:    "{} | quantile_over_time(duration, 0.9)",
			expected: 0.9,
		},
		{
			name:     "quantile 0.5",
			query:    "{} | quantile_over_time(duration, 0.5)",
			expected: 0.5,
		},
		{
			name:     "quantile 0.99",
			query:    "{} | quantile_over_time(duration, 0.99)",
			expected: 0.99,
		},
		{
			name:     "quantile 0.95 with selector",
			query:    "{status=error} | quantile_over_time(duration, 0.95)",
			expected: 0.95,
		},
		{
			name:     "quantile with complex selector",
			query:    "{nestedSetParent<0 && true} | quantile_over_time(duration, 0.9)",
			expected: 0.9,
		},
		{
			name:     "rate query returns default",
			query:    "{} | rate()",
			expected: 0.5, // default when not quantile_over_time
		},
		{
			name:     "count_over_time returns default",
			query:    "{} | count_over_time()",
			expected: 0.5, // default when not quantile_over_time
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			result := extractQuantileValue(ast)
			if result != tt.expected {
				t.Errorf("extractQuantileValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExtractMetricsInfo(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		expectedOp MetricsOp
		wantErr    bool
	}{
		{
			name:       "rate",
			query:      "{} | rate()",
			expectedOp: OpRate,
		},
		{
			name:       "count_over_time",
			query:      "{} | count_over_time()",
			expectedOp: OpCountOverTime,
		},
		{
			name:       "quantile_over_time",
			query:      "{} | quantile_over_time(duration, 0.9)",
			expectedOp: OpQuantileOverTime,
		},
		{
			name:       "histogram_over_time",
			query:      "{} | histogram_over_time(duration)",
			expectedOp: OpHistogramOverTime,
		},
		{
			name:       "min_over_time",
			query:      "{} | min_over_time(duration)",
			expectedOp: OpMinOverTime,
		},
		{
			name:       "max_over_time",
			query:      "{} | max_over_time(duration)",
			expectedOp: OpMaxOverTime,
		},
		{
			name:       "avg_over_time",
			query:      "{} | avg_over_time(duration)",
			expectedOp: OpAvgOverTime,
		},
		{
			name:       "sum_over_time",
			query:      "{} | sum_over_time(duration)",
			expectedOp: OpSumOverTime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			op, _, err := extractMetricsInfo(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractMetricsInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if op != tt.expectedOp {
				t.Errorf("extractMetricsInfo() op = %v, want %v", op, tt.expectedOp)
			}
		})
	}
}

func TestMetricsAggregateGetters(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedFloats []float64
	}{
		{
			name:           "quantile 0.9",
			query:          "{} | quantile_over_time(duration, 0.9)",
			expectedFloats: []float64{0.9},
		},
		{
			name:           "quantile 0.5",
			query:          "{} | quantile_over_time(duration, 0.5)",
			expectedFloats: []float64{0.5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			if ast.MetricsPipeline == nil {
				t.Fatal("MetricsPipeline is nil")
			}

			ma, ok := ast.MetricsPipeline.(*tempo.MetricsAggregate)
			if !ok {
				t.Fatalf("MetricsPipeline is not *tempo.MetricsAggregate, got %T", ast.MetricsPipeline)
			}

			floats := ma.Floats()
			if len(floats) != len(tt.expectedFloats) {
				t.Errorf("Floats() length = %d, want %d", len(floats), len(tt.expectedFloats))
				return
			}

			for i, f := range floats {
				if f != tt.expectedFloats[i] {
					t.Errorf("Floats()[%d] = %v, want %v", i, f, tt.expectedFloats[i])
				}
			}
		})
	}
}

func TestExtractByAttributes(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		expectedBy []string
	}{
		{
			name:       "quantile with by single attribute",
			query:      "{} | quantile_over_time(duration, 0.9) by(resource.service.name)",
			expectedBy: []string{"resource.service.name"},
		},
		{
			name:       "rate with by",
			query:      "{} | rate() by(resource.service.name)",
			expectedBy: []string{"resource.service.name"},
		},
		{
			name:       "quantile without by",
			query:      "{} | quantile_over_time(duration, 0.9)",
			expectedBy: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			_, by, err := extractMetricsInfo(ast)
			if err != nil {
				t.Fatalf("extractMetricsInfo() error = %v", err)
			}

			if len(by) != len(tt.expectedBy) {
				t.Errorf("extractMetricsInfo() by length = %d, want %d", len(by), len(tt.expectedBy))
				return
			}

			for i, b := range by {
				if b != tt.expectedBy[i] {
					t.Errorf("extractMetricsInfo() by[%d] = %v, want %v", i, b, tt.expectedBy[i])
				}
			}
		})
	}
}

func TestCalculateWindows(t *testing.T) {
	tests := []struct {
		name          string
		startOffset   time.Duration
		endOffset     time.Duration
		step          time.Duration
		expectedCount int
	}{
		{
			name:          "1 hour with 1 minute step",
			startOffset:   -time.Hour,
			endOffset:     0,
			step:          time.Minute,
			expectedCount: 60,
		},
		{
			name:          "1 hour with 5 minute step",
			startOffset:   -time.Hour,
			endOffset:     0,
			step:          5 * time.Minute,
			expectedCount: 12,
		},
		{
			name:          "empty range",
			startOffset:   0,
			endOffset:     0,
			step:          time.Minute,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			start := now.Add(tt.startOffset)
			end := now.Add(tt.endOffset)

			windows := calculateWindows(start, end, tt.step)
			if len(windows) != tt.expectedCount {
				t.Errorf("calculateWindows() returned %d windows, want %d", len(windows), tt.expectedCount)
			}
		})
	}
}

func TestBuildSelectorConditions(t *testing.T) {
	tests := []struct {
		name              string
		query             string
		wantConditions    bool
		wantNeedsJoin     bool
		wantConditionPart string // substring that should be in conditions
	}{
		{
			name:           "empty selector",
			query:          "{} | rate()",
			wantConditions: false,
			wantNeedsJoin:  false,
		},
		{
			name:           "true selector",
			query:          "{true} | rate()",
			wantConditions: false, // 1=1 is trivial, might not be added
			wantNeedsJoin:  false,
		},
		{
			name:              "status=error",
			query:             "{status=error} | rate()",
			wantConditions:    true,
			wantNeedsJoin:     true,
			wantConditionPart: "status",
		},
		{
			name:              "duration filter",
			query:             "{duration>1s} | rate()",
			wantConditions:    true,
			wantNeedsJoin:     false, // duration is in traces table
			wantConditionPart: "duration_ns",
		},
		{
			name:              "nestedSetParent<0",
			query:             "{nestedSetParent<0} | rate()",
			wantConditions:    true,
			wantNeedsJoin:     false,
			wantConditionPart: "parent_id",
		},
		{
			name:              "span attribute",
			query:             `{span.http.method="GET"} | rate()`,
			wantConditions:    true,
			wantNeedsJoin:     true,
			wantConditionPart: "http.method",
		},
		{
			name:              "name filter",
			query:             `{name="my-span"} | rate()`,
			wantConditions:    true,
			wantNeedsJoin:     false,
			wantConditionPart: "name",
		},
		{
			name:              "complex AND condition",
			query:             "{status=error && duration>1s} | rate()",
			wantConditions:    true,
			wantNeedsJoin:     true,
			wantConditionPart: "AND",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			conds, err := BuildSelectorConditions(ast.Pipeline)
			if err != nil {
				t.Fatalf("BuildSelectorConditions() error = %v", err)
			}

			hasConditions := len(conds.Conditions) > 0
			if hasConditions != tt.wantConditions {
				t.Errorf("BuildSelectorConditions() hasConditions = %v, want %v", hasConditions, tt.wantConditions)
			}

			if conds.NeedsJoin != tt.wantNeedsJoin {
				t.Errorf("BuildSelectorConditions() NeedsJoin = %v, want %v", conds.NeedsJoin, tt.wantNeedsJoin)
			}

			if tt.wantConditionPart != "" && hasConditions {
				combined := ""
				for _, c := range conds.Conditions {
					combined += c
				}
				if !containsSubstring(combined, tt.wantConditionPart) {
					t.Errorf("BuildSelectorConditions() conditions = %v, should contain %q", conds.Conditions, tt.wantConditionPart)
				}
			}
		})
	}
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestExtractCompareInfo(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		expectedOp MetricsOp
		wantErr    bool
	}{
		{
			name:       "compare with filter",
			query:      "{} | compare({status=error})",
			expectedOp: OpCompare,
		},
		{
			name:       "compare with topN",
			query:      "{} | compare({status=error}, 5)",
			expectedOp: OpCompare,
		},
		{
			name:       "compare with time offsets",
			query:      "{} | compare({status=error}, 10, 3600, 0)",
			expectedOp: OpCompare,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			op, _, err := extractMetricsInfo(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractMetricsInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if op != tt.expectedOp {
				t.Errorf("extractMetricsInfo() op = %v, want %v", op, tt.expectedOp)
			}
		})
	}
}

func TestExtractHistogramByAttributes(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		expectedBy []string
	}{
		{
			name:       "histogram with by single attribute",
			query:      "{} | histogram_over_time(duration) by(resource.service.name)",
			expectedBy: []string{"resource.service.name"},
		},
		{
			name:       "histogram without by",
			query:      "{} | histogram_over_time(duration)",
			expectedBy: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			_, by, err := extractMetricsInfo(ast)
			if err != nil {
				t.Fatalf("extractMetricsInfo() error = %v", err)
			}

			if len(by) != len(tt.expectedBy) {
				t.Errorf("extractMetricsInfo() by length = %d, want %d", len(by), len(tt.expectedBy))
				return
			}

			for i, b := range by {
				if b != tt.expectedBy[i] {
					t.Errorf("extractMetricsInfo() by[%d] = %v, want %v", i, b, tt.expectedBy[i])
				}
			}
		})
	}
}

func TestTopKBottomKParsing(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		hasSecondStg bool
		isTopK       bool
		k            int
	}{
		{
			name:         "rate with topk",
			query:        "{} | rate() by(resource.service.name) | topk(5)",
			hasSecondStg: true,
			isTopK:       true,
			k:            5,
		},
		{
			name:         "count with bottomk",
			query:        "{} | count_over_time() by(resource.service.name) | bottomk(3)",
			hasSecondStg: true,
			isTopK:       false,
			k:            3,
		},
		{
			name:         "rate without second stage",
			query:        "{} | rate() by(resource.service.name)",
			hasSecondStg: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			hasSecondStage := ast.MetricsSecondStage != nil
			if hasSecondStage != tt.hasSecondStg {
				t.Errorf("hasSecondStage = %v, want %v", hasSecondStage, tt.hasSecondStg)
				return
			}

			if !tt.hasSecondStg {
				return
			}

			tkbk, ok := ast.MetricsSecondStage.(*tempo.TopKBottomK)
			if !ok {
				t.Fatal("MetricsSecondStage is not *tempo.TopKBottomK")
			}

			isTopK := tkbk.OrderBy() == tempo.OpTopK
			if isTopK != tt.isTopK {
				t.Errorf("isTopK = %v, want %v", isTopK, tt.isTopK)
			}

			if tkbk.K() != tt.k {
				t.Errorf("K() = %d, want %d", tkbk.K(), tt.k)
			}
		})
	}
}
