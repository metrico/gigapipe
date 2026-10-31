package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"
)

// `X || Y`, where Y repeats an operand of the structural X, plans X alone,
// keeping every row of that operand.
func TestOrOfARepeatedOperandPlansTheStructuralOperatorOnce(t *testing.T) {
	for _, c := range []struct{ query, want string }{
		{`({.a = 1} &>> {.b = 2}) || ({.a = 1})`, `&>>({.a},{.b})+lhs`},
		{`({.a = 1} &>> {.b = 2}) || {.a = 1}`, `&>>({.a},{.b})+lhs`},
		{`{.a = 1} || ({.a = 1} &>> {.b = 2})`, `&>>({.a},{.b})+lhs`},
		{`({.a = 1} >> {.b = 2}) || {.b = 2}`, `>>({.a},{.b})+rhs`},
		{`({.a = 1} ~ {.b = 2}) || {.a = 1}`, `~({.a},{.b})+lhs`},
		{`({.a = 1} !> {.b = 2}) || {.b = 2}`, `!>({.a},{.b})+rhs`},
		{`({.a = 1} >> {.b = 2}) || {.a = 2}`, `or(>>({.a},{.b}),{.a})`},
		{`({.a = 1} || {.b = 2}) || {.a = 1}`, `or(or({.a},{.b}),{.a})`},
		{`({.a = 1} >> {.b = 2}) && {.a = 1}`, `and(>>({.a},{.b}),{.a})`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); got != c.want {
				t.Errorf("plan = %s, want %s", got, c.want)
			}
		})
	}
}

// cteRefs counts the reads of each CTE of a rendered statement.
func cteRefs(sql string) map[string]int {
	res := map[string]int{}
	for _, m := range regexp.MustCompile(`(?:WITH RECURSIVE |WITH |,)\s*([A-Za-z_0-9]+) as \(`).FindAllStringSubmatch(sql, -1) {
		res[m[1]] = len(regexp.MustCompile(`\b(?:FROM|JOIN) `+m[1]+`\b`).FindAllString(sql, -1))
	}
	return res
}

// The Drilldown reads its root pivot from one CTE, read once.
func TestDrilldownBuildsTheRootPivotOnce(t *testing.T) {
	const query = `({nestedSetParent < 0 && resource.service.name = "a"} &>> {kind = server}) || ` +
		`({nestedSetParent < 0 && resource.service.name = "a"}) | select(name)`
	for _, cluster := range []bool{false, true} {
		ctx := testPlannerContext(3)
		ctx.IsCluster = cluster
		res := renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx)
		pivots := 0
		for name, refs := range cteRefs(res) {
			if !strings.Contains(name, "_pivot") {
				continue
			}
			if strings.HasSuffix(name, "_pivot_attrs") {
				pivots++
			}
			if refs != 1 {
				t.Errorf("cluster=%v: %s is read %d times, want 1", cluster, name, refs)
			}
		}
		if pivots != 1 {
			t.Errorf("cluster=%v: %d attribute pivots, want 1:\n%s", cluster, pivots, res)
		}
	}
}

// `>>` and `~` read each operand once into their sides, which feed the
// candidate trace set and the nodes.
func TestStructuralOperandsFeedTheirSidesOnce(t *testing.T) {
	for _, query := range []string{`{.a = 1} >> {.b = 2}`, `{.a = 1} ~ {.b = 2}`} {
		for _, cluster := range []bool{false, true} {
			ctx := testPlannerContext(3)
			ctx.IsCluster = cluster
			refs := cteRefs(renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx))
			want := map[string]int{"_lhs": 1, "_rhs": 1, "_sides": 2}
			for suffix, n := range want {
				found := 0
				for name, got := range refs {
					if strings.HasSuffix(name, suffix) {
						found++
						if got != n {
							t.Errorf("%s cluster=%v: %s is read %d times, want %d", query, cluster, name, got, n)
						}
					}
				}
				if found != 1 {
					t.Errorf("%s cluster=%v: %d CTEs end in %s, want 1", query, cluster, found, suffix)
				}
			}
		}
	}
}
