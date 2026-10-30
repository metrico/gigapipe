package traceql_transpiler

import (
	"slices"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

func parse(t *testing.T, query string) *traceql_ir.Query {
	t.Helper()
	if query == "" {
		return nil
	}
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// Queries that select every span read every key or value in range, without a complexity estimate.
func TestAutocompleteWithoutSelectorReadsEveryKeyAndValue(t *testing.T) {
	for _, query := range []string{``, `{}`, `({})`} {
		tags, err := PlanTagsV2(parse(t, query))
		if err != nil {
			t.Fatal(err)
		}
		p, ok := tags.(*autocompleteRequestProcessor)
		if !ok {
			t.Fatalf("tags v2 of %q is %T", query, tags)
		}
		if _, ok := p.plan.Data.(*clickhouse_transpiler.AllTagsRequestPlanner); !ok || len(p.plan.Ids) != 0 {
			t.Errorf("tags v2 of %q reads %#v", query, p.plan)
		}

		values, err := PlanValuesV2(parse(t, query), "k")
		if err != nil {
			t.Fatal(err)
		}
		p, ok = values.(*autocompleteRequestProcessor)
		if !ok {
			t.Fatalf("values v2 of %q is %T", query, values)
		}
		if v, ok := p.plan.Data.(*clickhouse_transpiler.AllValuesRequestPlanner); !ok || v.Key != "k" || len(p.plan.Ids) != 0 {
			t.Errorf("values v2 of %q reads %#v, want every value of k", query, p.plan)
		}
	}
}

// Filtered queries read their result spans in two statements, by complexity
// portions when costly.
func TestAutocompleteWithSelectorEstimatesComplexity(t *testing.T) {
	for _, query := range []string{`{.a = 1}`, `{.a = 1} || {.b = 2}`, `{} >> {.b = 2}`} {
		for kind, plan := range map[string]func(*traceql_ir.Query) (shared.GenericTraceRequestProcessor[string], error){
			"tags": PlanTagsV2,
			"values": func(q *traceql_ir.Query) (shared.GenericTraceRequestProcessor[string], error) {
				return PlanValuesV2(q, "k")
			},
		} {
			res, err := plan(parse(t, query))
			if err != nil {
				t.Fatalf("%s %s: %v", kind, query, err)
			}
			e, ok := res.(*TraceQLComplexityEvaluator[string, *clickhouse_transpiler.SearchPlan])
			if !ok {
				t.Fatalf("%s %s is %T", kind, query, res)
			}
			if len(e.main.Ids) == 0 || e.main.Data == nil {
				t.Errorf("%s %s: plan %#v, want a search and a data statement", kind, query, e.main)
			}
			if _, ok := e.complexRequestProcessor.(*complexAutocompleteRequestProcessor); !ok {
				t.Errorf("%s %s: complex processor is %T", kind, query, e.complexRequestProcessor)
			}
			if _, ok := e.simpleRequestProcessor.(*autocompleteRequestProcessor); !ok {
				t.Errorf("%s %s: simple processor is %T", kind, query, e.simpleRequestProcessor)
			}
		}
	}
}

func TestMergeSuggestions(t *testing.T) {
	parts := [][]string{{"b", "d"}, {"a", "b"}, nil, {"c"}}
	if got, want := mergeSuggestions(parts, 0), []string{"a", "b", "c", "d"}; !slices.Equal(got, want) {
		t.Errorf("unlimited merge = %v, want %v", got, want)
	}
	if got, want := mergeSuggestions(parts, 3), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("merge up to 3 = %v, want %v", got, want)
	}
}
