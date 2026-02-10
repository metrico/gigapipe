package traceql_transpiler_v2

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/traceql/tempo"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// TestPlannerBasicQueries tests basic TraceQL queries.
func TestPlannerBasicQueries(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "empty selector",
			query:   "{}",
			wantErr: false,
		},
		{
			name:    "true literal",
			query:   "{true}",
			wantErr: false,
		},
		{
			name:    "false literal",
			query:   "{false}",
			wantErr: false,
		},
		{
			name:    "status equals error",
			query:   "{status=error}",
			wantErr: false,
		},
		{
			name:    "status equals ok",
			query:   "{status=ok}",
			wantErr: false,
		},
		{
			name:    "duration greater than",
			query:   "{duration>1s}",
			wantErr: false,
		},
		{
			name:    "duration less than",
			query:   "{duration<500ms}",
			wantErr: false,
		},
		{
			name:    "name equals",
			query:   `{name="GET /api"}`,
			wantErr: false,
		},
		{
			name:    "span attribute string",
			query:   `{span.http.method="GET"}`,
			wantErr: false,
		},
		{
			name:    "resource attribute string",
			query:   `{resource.service.name="my-service"}`,
			wantErr: false,
		},
		{
			name:    "unscoped attribute string",
			query:   `{.http.status_code="200"}`,
			wantErr: false,
		},
		{
			name:    "attribute numeric comparison",
			query:   `{.http.status_code>=400}`,
			wantErr: false,
		},
		{
			name:    "nestedSetParent root",
			query:   `{nestedSetParent<0}`,
			wantErr: false,
		},
		{
			name:    "nestedSetParent non-root",
			query:   `{nestedSetParent>=0}`,
			wantErr: false,
		},
		{
			name:    "AND condition",
			query:   `{status=error && duration>1s}`,
			wantErr: false,
		},
		{
			name:    "OR condition",
			query:   `{status=error || status=ok}`,
			wantErr: false,
		},
		{
			name:    "complex condition",
			query:   `{nestedSetParent<0 && status=error}`,
			wantErr: false,
		},
		{
			name:    "true AND condition",
			query:   `{nestedSetParent<0 && true}`,
			wantErr: false,
		},
		{
			name:    "with select projection",
			query:   `{} | select(resource.service.name)`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("tempo.Parse() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			planner, err := PlanSQL(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("PlanSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if planner == nil && !tt.wantErr {
				t.Error("PlanSQL() returned nil planner")
			}
		})
	}
}

// TestPlannerSQLGeneration tests that SQL is generated correctly.
func TestPlannerSQLGeneration(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "empty selector",
			query: "{}",
		},
		{
			name:  "status equals error",
			query: "{status=error}",
		},
		{
			name:  "duration greater than",
			query: "{duration>1s}",
		},
	}

	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                20,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner, err := PlanSQL(ast)
			if err != nil {
				t.Fatalf("PlanSQL() error = %v", err)
			}

			sqlSelect, err := planner.Process(ctx)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			sqlStr, err := sqlSelect.String(&sql.Ctx{
				Params: map[string]sql.SQLObject{},
				Result: map[string]sql.SQLObject{},
			})
			if err != nil {
				t.Fatalf("String() error = %v", err)
			}

			if sqlStr == "" {
				t.Error("Generated SQL is empty")
			}

			t.Logf("Query: %s\nGenerated SQL:\n%s", tt.query, sqlStr)
		})
	}
}

// TestAttributeConditionsInWHERE verifies that attribute conditions (key/val)
// are placed in WHERE clause, not HAVING. This is critical because key/val
// are not aggregated columns and cannot be used in HAVING.
// Regression test for: "Column traces_idx.key is not under aggregate function and not in GROUP BY keys"
func TestAttributeConditionsInWHERE(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		mustContain    []string // strings that must be in the SQL
		mustNotContain []string // strings that must NOT be in the SQL
	}{
		{
			name:  "status=error should be in WHERE not HAVING",
			query: "{status=error}",
			mustContain: []string{
				"WHERE",
				"key",
				"status",
				"val",
				"error",
			},
			mustNotContain: []string{
				"HAVING",
			},
		},
		{
			name:  "complex condition with true and status",
			query: "{nestedSetParent<0 && true && status=error}",
			mustContain: []string{
				"WHERE",
			},
			mustNotContain: []string{
				"HAVING",
			},
		},
		{
			name:  "span attribute should be in WHERE",
			query: `{span.http.method="GET"}`,
			mustContain: []string{
				"WHERE",
				"key",
				"http.method",
			},
			mustNotContain: []string{
				"HAVING",
			},
		},
		{
			name:  "multiple attribute conditions",
			query: `{status=error && .http.status_code>=400}`,
			mustContain: []string{
				"WHERE",
			},
			mustNotContain: []string{
				"HAVING",
			},
		},
	}

	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                20,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner, err := PlanSQL(ast)
			if err != nil {
				t.Fatalf("PlanSQL() error = %v", err)
			}

			sqlSelect, err := planner.Process(ctx)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			sqlStr, err := sqlSelect.String(&sql.Ctx{
				Params: map[string]sql.SQLObject{},
				Result: map[string]sql.SQLObject{},
			})
			if err != nil {
				t.Fatalf("String() error = %v", err)
			}

			// Check must contain
			for _, substr := range tt.mustContain {
				if !containsString(sqlStr, substr) {
					t.Errorf("SQL should contain %q but doesn't.\nSQL: %s", substr, sqlStr)
				}
			}

			// Check must NOT contain
			for _, substr := range tt.mustNotContain {
				if containsString(sqlStr, substr) {
					t.Errorf("SQL should NOT contain %q but does.\nSQL: %s", substr, sqlStr)
				}
			}
		})
	}
}

// containsString checks if s contains substr (case-insensitive for SQL keywords)
func containsString(s, substr string) bool {
	// Simple case-sensitive check
	if len(substr) == 0 {
		return true
	}
	return len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestIntrinsicConditions tests that intrinsic conditions are handled correctly.
func TestIntrinsicConditions(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                20,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	}

	tests := []struct {
		name        string
		query       string
		mustContain []string
	}{
		{
			name:  "duration uses traces_idx.duration",
			query: "{duration>1s}",
			mustContain: []string{
				"traces_idx.duration",
				"1000000000", // 1s in nanoseconds
			},
		},
		{
			name:  "duration less than",
			query: "{duration<500ms}",
			mustContain: []string{
				"traces_idx.duration",
				"500000000", // 500ms in nanoseconds
			},
		},
		{
			name:  "name uses key/val",
			query: `{name="test-span"}`,
			mustContain: []string{
				"key",
				"name",
				"val",
				"test-span",
			},
		},
		{
			name:  "status uses key/val",
			query: "{status=error}",
			mustContain: []string{
				"key",
				"status",
				"val",
				"error",
			},
		},
		{
			name:  "nestedSetParent sets context filter",
			query: "{nestedSetParent<0}",
			mustContain: []string{
				"1", // TRUE condition at index level
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner, err := PlanSQL(ast)
			if err != nil {
				t.Fatalf("PlanSQL() error = %v", err)
			}

			sqlSelect, err := planner.Process(ctx)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			sqlStr, err := sqlSelect.String(&sql.Ctx{
				Params: map[string]sql.SQLObject{},
				Result: map[string]sql.SQLObject{},
			})
			if err != nil {
				t.Fatalf("String() error = %v", err)
			}

			for _, substr := range tt.mustContain {
				if !containsString(sqlStr, substr) {
					t.Errorf("SQL should contain %q but doesn't.\nSQL: %s", substr, sqlStr)
				}
			}
		})
	}
}

// TestAttributeScopes tests that attribute scopes are handled correctly.
func TestAttributeScopes(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                20,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	}

	tests := []struct {
		name        string
		query       string
		mustContain []string
	}{
		{
			name:  "span scoped attribute",
			query: `{span.http.method="GET"}`,
			mustContain: []string{
				"key",
				"http.method",
				"val",
				"GET",
			},
		},
		{
			name:  "resource scoped attribute",
			query: `{resource.service.name="my-service"}`,
			mustContain: []string{
				"key",
				"service.name",
				"val",
				"my-service",
			},
		},
		{
			name:  "unscoped attribute",
			query: `{.custom_attr="value"}`,
			mustContain: []string{
				"key",
				"custom_attr",
				"val",
				"value",
			},
		},
		{
			name:  "numeric attribute comparison",
			query: `{.http.status_code>=400}`,
			mustContain: []string{
				"key",
				"http.status_code",
				"toFloat64OrZero",
				"400",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner, err := PlanSQL(ast)
			if err != nil {
				t.Fatalf("PlanSQL() error = %v", err)
			}

			sqlSelect, err := planner.Process(ctx)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			sqlStr, err := sqlSelect.String(&sql.Ctx{
				Params: map[string]sql.SQLObject{},
				Result: map[string]sql.SQLObject{},
			})
			if err != nil {
				t.Fatalf("String() error = %v", err)
			}

			for _, substr := range tt.mustContain {
				if !containsString(sqlStr, substr) {
					t.Errorf("SQL should contain %q but doesn't.\nSQL: %s", substr, sqlStr)
				}
			}
		})
	}
}

// TestBooleanOperators tests AND/OR operators generate correct SQL.
func TestBooleanOperators(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                20,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	}

	tests := []struct {
		name        string
		query       string
		mustContain []string
	}{
		{
			name:  "AND creates and condition",
			query: "{status=error && duration>1s}",
			mustContain: []string{
				"and",
				"status",
				"error",
				"traces_idx.duration",
			},
		},
		{
			name:  "OR creates or condition",
			query: "{status=error || status=ok}",
			mustContain: []string{
				"or",
				"status",
				"error",
				"ok",
			},
		},
		{
			name:  "complex nested condition",
			query: "{status=error && (duration>1s || duration<100ms)}",
			mustContain: []string{
				"and",
				"or",
				"status",
				"error",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner, err := PlanSQL(ast)
			if err != nil {
				t.Fatalf("PlanSQL() error = %v", err)
			}

			sqlSelect, err := planner.Process(ctx)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			sqlStr, err := sqlSelect.String(&sql.Ctx{
				Params: map[string]sql.SQLObject{},
				Result: map[string]sql.SQLObject{},
			})
			if err != nil {
				t.Fatalf("String() error = %v", err)
			}

			for _, substr := range tt.mustContain {
				if !containsString(sqlStr, substr) {
					t.Errorf("SQL should contain %q but doesn't.\nSQL: %s", substr, sqlStr)
				}
			}
		})
	}
}

// TestTagsValuesPlanning tests the tags and values planners.
func TestTagsValuesPlanning(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                100,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
		TracesKVTable:        "tempo_traces_kv",
		TracesKVDistTable:    "tempo_traces_kv_dist",
	}

	t.Run("all tags without filter", func(t *testing.T) {
		planner, err := PlanTagsV2(nil)
		if err != nil {
			t.Fatalf("PlanTagsV2() error = %v", err)
		}

		// Access internal planner to get SQL
		processor := planner.(*TagsRequestProcessor)
		sqlSelect, err := processor.sqlPlanner.Process(ctx)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		sqlStr, err := sqlSelect.String(&sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		})
		if err != nil {
			t.Fatalf("String() error = %v", err)
		}

		if !containsString(sqlStr, "DISTINCT") {
			t.Error("Tags query should use DISTINCT")
		}
		if !containsString(sqlStr, "key") {
			t.Error("Tags query should select key")
		}
	})

	t.Run("all values without filter", func(t *testing.T) {
		planner, err := PlanValuesV2(nil, "service.name")
		if err != nil {
			t.Fatalf("PlanValuesV2() error = %v", err)
		}

		processor := planner.(*TagsRequestProcessor)
		sqlSelect, err := processor.sqlPlanner.Process(ctx)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		sqlStr, err := sqlSelect.String(&sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		})
		if err != nil {
			t.Fatalf("String() error = %v", err)
		}

		if !containsString(sqlStr, "DISTINCT") {
			t.Error("Values query should use DISTINCT")
		}
		if !containsString(sqlStr, "val") {
			t.Error("Values query should select val")
		}
		if !containsString(sqlStr, "service.name") {
			t.Error("Values query should filter by key")
		}
	})

	t.Run("tags with filter", func(t *testing.T) {
		ast, err := tempo.Parse("{status=error}")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		planner, err := PlanTagsV2(ast)
		if err != nil {
			t.Fatalf("PlanTagsV2() error = %v", err)
		}

		processor := planner.(*TagsRequestProcessor)
		sqlSelect, err := processor.sqlPlanner.Process(ctx)
		if err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		sqlStr, err := sqlSelect.String(&sql.Ctx{
			Params: map[string]sql.SQLObject{},
			Result: map[string]sql.SQLObject{},
		})
		if err != nil {
			t.Fatalf("String() error = %v", err)
		}

		if !containsString(sqlStr, "WITH") {
			t.Error("Filtered tags query should use CTE")
		}
	})
}

// TestNestedSetParentFilter tests that nestedSetParent filter is applied correctly.
func TestNestedSetParentFilter(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedFilter string
	}{
		{
			name:           "root spans nestedSetParent<0",
			query:          "{nestedSetParent<0}",
			expectedFilter: "root",
		},
		{
			name:           "non-root spans nestedSetParent>=0",
			query:          "{nestedSetParent>=0}",
			expectedFilter: "non-root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &shared.PlannerContext{
				From:                 time.Now().Add(-1 * time.Hour),
				To:                   time.Now(),
				Limit:                20,
				TracesAttrsTable:     "tempo_traces_attrs_gin",
				TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
				TracesTable:          "tempo_traces",
				TracesDistTable:      "tempo_traces_dist",
			}

			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner, err := PlanSQL(ast)
			if err != nil {
				t.Fatalf("PlanSQL() error = %v", err)
			}

			_, err = planner.Process(ctx)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}

			if ctx.NestedSetParentFilter != tt.expectedFilter {
				t.Errorf("NestedSetParentFilter = %q, want %q", ctx.NestedSetParentFilter, tt.expectedFilter)
			}
		})
	}
}

// TestClusterMode tests that cluster mode uses distributed tables.
func TestClusterMode(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                20,
		IsCluster:            true,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	}

	ast, err := tempo.Parse("{status=error}")
	if err != nil {
		t.Fatalf("tempo.Parse() error = %v", err)
	}

	planner, err := PlanSQL(ast)
	if err != nil {
		t.Fatalf("PlanSQL() error = %v", err)
	}

	sqlSelect, err := planner.Process(ctx)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	sqlStr, err := sqlSelect.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		t.Fatalf("String() error = %v", err)
	}

	if !containsString(sqlStr, "_dist") {
		t.Error("Cluster mode should use distributed tables (*_dist)")
	}
}

// TestSelectOperatorExtraction tests that select() attributes are extracted correctly.
func TestSelectOperatorExtraction(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedAttrs []string
	}{
		{
			name:          "no select",
			query:         "{status=error}",
			expectedAttrs: nil,
		},
		{
			name:          "single select attribute",
			query:         "{} | select(resource.service.name)",
			expectedAttrs: []string{"resource.service.name"},
		},
		{
			name:          "multiple select attributes",
			query:         "{} | select(resource.service.name, span.http.method)",
			expectedAttrs: []string{"resource.service.name", "span.http.method"},
		},
		{
			name:          "select with filter",
			query:         "{status=error} | select(span.http.url)",
			expectedAttrs: []string{"span.http.url"},
		},
		{
			name:          "unscoped attribute in select",
			query:         "{} | select(.custom_attr)",
			expectedAttrs: []string{".custom_attr"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner := &TempoPlanner{root: ast}
			attrs := planner.extractSelectAttrs()

			if len(attrs) != len(tt.expectedAttrs) {
				t.Errorf("extractSelectAttrs() returned %d attrs, want %d", len(attrs), len(tt.expectedAttrs))
				return
			}

			for i, expected := range tt.expectedAttrs {
				if attrs[i] != expected {
					t.Errorf("attr[%d] = %q, want %q", i, attrs[i], expected)
				}
			}
		})
	}
}

// TestWithHintsExtraction tests that with() hints are extracted correctly.
func TestWithHintsExtraction(t *testing.T) {
	tests := []struct {
		name               string
		query              string
		expectedMostRecent bool
	}{
		{
			name:               "no hints",
			query:              "{}",
			expectedMostRecent: false,
		},
		{
			name:               "most_recent=true",
			query:              "{} with(most_recent=true)",
			expectedMostRecent: true,
		},
		{
			name:               "most_recent=false",
			query:              "{} with(most_recent=false)",
			expectedMostRecent: false,
		},
		{
			name:               "with filter and hint",
			query:              "{status=error} with(most_recent=true)",
			expectedMostRecent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := tempo.Parse(tt.query)
			if err != nil {
				t.Fatalf("tempo.Parse() error = %v", err)
			}

			planner := &TempoPlanner{root: ast}
			mostRecent := planner.extractMostRecentHint()

			if mostRecent != tt.expectedMostRecent {
				t.Errorf("extractMostRecentHint() = %v, want %v", mostRecent, tt.expectedMostRecent)
			}
		})
	}
}

// TestSelectDoesNotAffectFiltering tests that select() doesn't change the SQL filter logic.
func TestSelectDoesNotAffectFiltering(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Now().Add(-1 * time.Hour),
		To:                   time.Now(),
		Limit:                20,
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
	}

	// Query without select
	astWithout, _ := tempo.Parse("{status=error}")
	plannerWithout, _ := PlanSQL(astWithout)
	sqlWithout, _ := plannerWithout.Process(ctx)
	strWithout, _ := sqlWithout.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})

	// Reset context
	ctx.NestedSetParentFilter = ""

	// Query with select
	astWith, _ := tempo.Parse("{status=error} | select(resource.service.name)")
	plannerWith, _ := PlanSQL(astWith)
	sqlWith, _ := plannerWith.Process(ctx)
	strWith, _ := sqlWith.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})

	// Both should have the same WHERE clause for status=error
	if !containsString(strWithout, "status") || !containsString(strWith, "status") {
		t.Error("Both queries should filter by status")
	}
	if !containsString(strWithout, "error") || !containsString(strWith, "error") {
		t.Error("Both queries should filter by error value")
	}
}
