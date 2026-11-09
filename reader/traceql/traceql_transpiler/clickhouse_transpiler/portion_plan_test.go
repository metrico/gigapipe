package clickhouse_transpiler

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// An autocomplete's operand reads keep the current portion's hash.
func TestPortionFilterOnEveryOperandRead(t *testing.T) {
	const portionFilter = `(cityHash64(trace_id) % 3) == (1)`
	requestedBounds := []string{">86390", "<=86410"}
	for _, tc := range []struct {
		query    string
		operands int
	}{
		{`{.a} >> {.b}`, 2},
		{`{.a} && {.b}`, 2},
		{`{.a} !>> {.b}`, 2},
		{`{.a} | count() > 2`, 1},
		{`{.a = 1} >> {.b = 2}`, 2},
		{`{.a = 1} && {.b = 2}`, 2},
		{`{.a = 1} !>> {.b = 2}`, 2},
		{`{.a = 1} | count() > 2`, 1},
	} {
		t.Run(tc.query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.RandomFilter = shared.RandomFilter{Max: 3, I: 1}
			q, err := traceql_ir.Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := PlanTagsV2(q)
			if err != nil {
				t.Fatal(err)
			}
			req, err := plan.Ids[0].Statement.Process(ctx)
			if err != nil {
				t.Fatal(err)
			}
			operands := 0
			for _, w := range req.GetWith() {
				s := renderWith(t, w)
				bounds := spanStartBounds(s)
				// selected_values reads only the traces the operands chose.
				if len(bounds) == 0 || w.GetAlias() == "selected_values" {
					continue
				}
				operands++
				if !slices.Equal(bounds, requestedBounds) {
					t.Errorf("%s: bounds %v, want %v", w.GetAlias(), bounds, requestedBounds)
				}
				if !strings.Contains(s, portionFilter) {
					t.Errorf("%s: no portion filter:\n%s", w.GetAlias(), s)
				}
			}
			if operands != tc.operands {
				t.Errorf("got %d operand reads, want %d", operands, tc.operands)
			}
		})
	}
}

func TestResultSortsEqualStartsByTraceID(t *testing.T) {
	req, err := planSearch(t, `{.a = 1}`).Data.Process(testPlannerContext(3))
	if err != nil {
		t.Fatal(err)
	}
	s, err := req.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s, "ORDER BY start_time_unix_nano desc , trace_id desc") {
		t.Errorf("got %s", s)
	}
}

// Every search shape returns the rank the reader merges portions by.
func TestFinalSelectHasTheRank(t *testing.T) {
	shapes := []string{
		`{}`, `{} | select(.x)`, `{.a = 1}`, `{.a}`, `{.a} >> {.b}`, `{.a} && {.b}`, `{.a} !>> {.b}`,
		`{.a} | count() > 2`, `{.a} | by(.x)`, `{.a} || {.b}`, `{.a} | select(.x)`,
	}
	for _, query := range shapes {
		t.Run(query, func(t *testing.T) {
			req, err := planSearch(t, query).Ids[0].Statement.Process(testPlannerContext(3))
			if err != nil {
				t.Fatal(err)
			}
			cols := req.GetSelect()
			last, err := cols[len(cols)-1].String(sql.DefaultCtx())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasSuffix(last, "as trace_newest") {
				t.Errorf("last column %q, want trace_newest", last)
			}
		})
	}
}

func estimateOf(t *testing.T, query string) shared.SQLRequestPlanner {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	eval, err := PlanEval(q)
	if err != nil {
		t.Fatal(err)
	}
	if gate, ok := eval.(*StructuralQueryPlanner); ok {
		eval = gate.Main
	}
	return eval.(*EvalFinalizerPlanner).Main
}

// estimateStatement is the whole statement that estimates query.
func estimateStatement(t *testing.T, query string) shared.SQLRequestPlanner {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	eval, err := PlanEval(q)
	if err != nil {
		t.Fatal(err)
	}
	return eval
}

func TestStructuralQueriesEstimateEverySpanInRange(t *testing.T) {
	queries := []string{`{.a} && ({.b} >> {.c})`, `({.a} >> {.b}) | count() > 1`, `{} ~ {.a}`, `{} >> {.a}`}
	for _, op := range []string{">", "<", "~", "!>", "!<", "!~", "&>", "&<", "&~", ">>", "<<", "!>>", "!<<", "&>>", "&<<"} {
		queries = append(queries, `{.a = 1} `+op+` {.b = 1}`)
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			perShard, ok := estimateOf(t, query).(*shardLocalPlanner)
			if !ok {
				t.Fatalf("estimated by %T", estimateOf(t, query))
			}
			estimate, ok := perShard.Single.(*AllSpansEvaluatorPlanner)
			if !ok {
				t.Fatalf("estimated on one node by %T", perShard.Single)
			}
			res := renderPlan(t, estimate, testPlannerContext(3))
			if !strings.Contains(res, "count()") || !strings.Contains(res, "tempo_traces") {
				t.Errorf("no count over the traces table:\n%s", res)
			}
			if bounds := spanStartBounds(res); !slices.Equal(bounds, []string{">86390", "<=86410"}) {
				t.Errorf("bounds %v:\n%s", bounds, res)
			}
		})
	}
}

// One row per shard reaches the evaluator: each shard counts its own spans in range.
func TestStructuralEstimateCountsOnEachShard(t *testing.T) {
	view := shardViewThen(`(.*)`)
	for _, query := range append([]string{`{.a} && ({.b} >> {.c})`, `{} >> {.a}`}, structuralQueries...) {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, estimateStatement(t, query), ctx)
			if strings.Count(res, "cluster('test_cluster', view(") != 1 {
				t.Fatalf("estimate is not one statement on each shard:\n%s", res)
			}
			m := view.FindStringSubmatch(res)
			if m == nil {
				t.Fatalf("estimate does not read a shard view:\n%s", res)
			}
			shard, outer := m[2], m[1]+m[3]
			if !strings.Contains(shard, "count()") || !strings.Contains(shard, "tempo_traces") || strings.Contains(shard, "_dist") {
				t.Errorf("shard does not count its local traces table:\n%s", shard)
			}
			if bounds := spanStartBounds(shard); !slices.Equal(bounds, []string{">86390", "<=86410"}) {
				t.Errorf("bounds %v:\n%s", bounds, shard)
			}
			if strings.Contains(outer, "max(") || strings.Contains(outer, "sum(") {
				t.Errorf("coordinator aggregates the shards' rows:\n%s", outer)
			}
			if !strings.Contains(outer, "prefer_localhost_replica=0") {
				t.Errorf("local shard runs in-process:\n%s", outer)
			}
		})
	}
}

func TestSpanOnlyEstimatesStayOffTheShardView(t *testing.T) {
	for _, query := range []string{`{}`, `{.a = 1}`, `{.a} | count() > 2`, `{.a = 1} && {.b = 2}`, `{.a = 1} || {.b = 2}`} {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			ctx.SkipUnavailableShards = true
			res := renderPlan(t, estimateStatement(t, query), ctx)
			for _, bad := range []string{"cluster(", "SETTINGS"} {
				if strings.Contains(res, bad) {
					t.Errorf("estimate holds %q:\n%s", bad, res)
				}
			}
		})
	}
}

func TestSpanOnlyQueriesKeepTheirEstimates(t *testing.T) {
	for _, query := range []string{`{.a = 1} && {.b = 2}`, `{.a = 1} || {.b = 2}`} {
		t.Run(query, func(t *testing.T) {
			estimate, ok := estimateOf(t, query).(*ComplexEvalOrPlanner)
			if !ok {
				t.Fatalf("estimated by %T", estimateOf(t, query))
			}
			for _, operand := range estimate.Operands {
				if _, all := operand.(*AllSpansEvaluatorPlanner); all {
					t.Errorf("operand estimates every span")
				}
			}
		})
	}
	for _, query := range []string{`{.a = 1}`, `{.a} | count() > 2`} {
		t.Run(query, func(t *testing.T) {
			if _, all := estimateOf(t, query).(*AllSpansEvaluatorPlanner); all {
				t.Errorf("estimates every span")
			}
		})
	}
}

// autocompletePortionQueries cover every structural operator and the reads a
// filter can add: root, nested set and span-only operands.
var autocompletePortionQueries = []string{
	`{.a = 1}`, `{.a = 1 || duration > 1ms}`, `{.a} && {.b}`, `{.a} || {.b}`, `{.a} | count() > 2`,
	`{.a} >> {.b}`, `{.a = 1} > {.b = 2}`, `{.a} ~ {.b}`, `{.a} &>> {.b}`, `{.a} !>> {.b}`,
	`{.a} << {.b}`, `{.a} < {.b}`, `{.a} &<< {.b}`, `{.a} !<< {.b}`, `{.a} !~ {.b}`, `{} >> {.b}`, `{duration > 1ms} >> {.b}`, `{.a} > {}`,
	`{nestedSetParent = -1 && .a = 1} >> {.b}`, `{nestedSetLeft > 0 && .a = 1} &>> {.b}`,
	`{.b = 1} | by(.a) | select(name)`, `{.a} | avg(.x) > 1`, `{.a} >> {.b} | max(duration) > 1ms`,
	`({nestedSetParent < 0 && .a = 1} &>> {.b = 1}) || ({nestedSetParent < 0 && .a = 1}) | select(name, nestedSetParent)`,
}

// tableReads returns, for each read of a traces table in s, the text of the
// SELECT that reads it, nested SELECTs left out.
func tableReads(s string) map[string][]string {
	re := regexp.MustCompile(`(?i)FROM (tempo_traces\w*)`)
	res := map[string][]string{}
	for _, m := range re.FindAllStringSubmatchIndex(s, -1) {
		var b strings.Builder
		depth := 0
	scan:
		for i := m[1]; i < len(s); i++ {
			switch {
			case s[i] == '(' && strings.HasPrefix(strings.ToUpper(strings.TrimLeft(s[i+1:], " (")), "SELECT"):
				for d := 0; i < len(s); i++ {
					if s[i] == '(' {
						d++
					} else if s[i] == ')' {
						if d--; d == 0 {
							break
						}
					}
				}
			case s[i] == '(':
				depth++
				b.WriteByte(s[i])
			case s[i] == ')':
				if depth--; depth < 0 {
					break scan
				}
				b.WriteByte(s[i])
			case depth == 0 && strings.HasPrefix(strings.ToUpper(s[i:]), "UNION ALL"):
				break scan
			default:
				b.WriteByte(s[i])
			}
		}
		table := s[m[2]:m[3]]
		res[table] = append(res[table], b.String())
	}
	return res
}

// Every table read of a portioned autocomplete, ids and data statements, keeps
// the portion's trace_id ranges, single node and on a cluster.
func TestAutocompletePortionNarrowsEveryRead(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, query := range autocompletePortionQueries {
			for kind, plan := range autocompletePlans(t, query) {
				t.Run(fmt.Sprintf("%s %s cluster=%v", kind, query, cluster), func(t *testing.T) {
					ctx := testPlannerContext(3)
					ctx.IsCluster = cluster
					ctx.RandomFilter = shared.RandomFilter{Max: 3, I: 1, TraceIDRanges: true}
					reads := 0
					for name, p := range map[string]shared.SQLRequestPlanner{"ids": plan.Ids[0].Statement, "data": plan.Data} {
						req, err := p.Process(ctx)
						if err != nil {
							t.Fatal(err)
						}
						s, err := req.String(sql.DefaultCtx())
						if err != nil {
							t.Fatal(err)
						}
						for table, sels := range tableReads(s) {
							for _, sel := range sels {
								reads++
								if !strings.Contains(sel, "trace_id >= unhex(") || !strings.Contains(sel, "trace_id < unhex(") {
									t.Errorf("%s statement: read of %s without the portion:\n%s", name, table, sel)
								}
							}
						}
						if strings.Contains(s, "cityHash64(trace_id)") {
							t.Errorf("%s statement: hash portion:\n%s", name, s)
						}
					}
					if reads < 2 {
						t.Errorf("%d table reads, want an operand read and the data read", reads)
					}
				})
			}
		}
	}
}

// Under hash portions only operand reads carry the portion: every read of the
// ids or traces an earlier stage chose stays unrestricted.
func TestHashPortionStaysOnOperandReads(t *testing.T) {
	const hash = "cityHash64(trace_id)"
	chosen := regexp.MustCompile(`(?i)_id\)? (GLOBAL )?IN`)
	for _, cluster := range []bool{false, true} {
		for _, query := range autocompletePortionQueries {
			t.Run(fmt.Sprintf("%s cluster=%v", query, cluster), func(t *testing.T) {
				ctx := testPlannerContext(3)
				ctx.IsCluster = cluster
				ctx.RandomFilter = shared.RandomFilter{Max: 3, I: 1}
				plan := planSearch(t, query)
				ids := renderPlan(t, plan.Ids[0].Statement, ctx)
				if !strings.Contains(ids, hash) {
					t.Errorf("no operand read holds the portion:\n%s", ids)
				}
				for _, s := range []string{ids, renderPlan(t, plan.Data, ctx)} {
					for table, sels := range tableReads(s) {
						for _, sel := range sels {
							if chosen.MatchString(sel) && strings.Contains(sel, hash) {
								t.Errorf("read of %s holds the portion:\n%s", table, sel)
							}
						}
					}
				}
			})
		}
	}
}
