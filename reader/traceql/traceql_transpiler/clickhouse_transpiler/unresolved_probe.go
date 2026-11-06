package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// UnresolvedProbe counts and lists the traces with unresolved rows in one jump.
type UnresolvedProbe struct {
	Jump   string
	Select sql.ISelect
}

// UnresolvedProbes plans a query's unresolved-trace probes.
type UnresolvedProbes struct {
	result shared.SQLRequestPlanner
	sample sql.SQLCondition
}

// PlanUnresolvedProbes plans the probes over the query's result chain.
func PlanUnresolvedProbes(query *traceql_ir.Query) (*UnresolvedProbes, error) {
	p := &planner{query: query}
	result, err := p.planQuery()
	if err != nil {
		return nil, err
	}
	res := &UnresolvedProbes{result: result}
	// Only metrics and compare() requests read the sample hint.
	if query.Metrics != nil {
		res.sample = sampleFilter(query)
	}
	return res, nil
}

// Process plans one statement per recursive CTE, in plan order; each runs on
// its own.
func (u *UnresolvedProbes) Process(ctx *shared.PlannerContext) ([]UnresolvedProbe, error) {
	if !ctx.VersionInfo.HasCapability(dbversion.CapStructuralQueries) {
		return nil, errStructuralVersion
	}
	req, err := u.result.Process(sampled(ctx, u.sample))
	if err != nil {
		return nil, err
	}
	var res []UnresolvedProbe
	for _, w := range req.GetWith() {
		if !w.IsRecursive() {
			continue
		}
		res = append(res, UnresolvedProbe{
			Jump: w.GetAlias(),
			Select: sql.NewSelect().With(req.GetWith()...).
				Select(sql.NewSimpleCol("uniqExact(trace_id)", "n"),
					sql.NewSimpleCol("groupUniqArray(20)(lower(hex(trace_id)))", "ids")).
				From(sql.NewWithRef(w)).
				AndWhere(rawCond(finalJump), rawCond(unresolvedRow(ancestryRounds(ctx)))).
				SetSetting("enable_analyzer", "1"),
		})
	}
	return res, nil
}
