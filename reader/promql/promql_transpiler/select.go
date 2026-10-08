package promql_transpiler

import (
	"time"

	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler/planner"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// Route is the source a selector's read is served from.
type Route string

const (
	RouteRaw        Route = "raw"
	RouteMetrics15s Route = "metrics_15s"
	RouteSubstitute Route = "substitute"
)

// SelectRequest is one selector read as the engine issues it.
type SelectRequest struct {
	Hints *storage.SelectHints
	// Matchers may carry the selector's grid matcher.
	Matchers    []*labels.Matcher
	Substitutes map[string]*promql_parser.Substitute
	VersionInfo dbversion.VersionInfo
}

// supportedFunctions maps a range or instant function to whether metrics_15s
// can serve it. Functions missing from the map are treated as supported.
var supportedFunctions = map[string]bool{
	// Over time
	"avg_over_time":      true,
	"min_over_time":      true,
	"max_over_time":      true,
	"sum_over_time":      true,
	"count_over_time":    true,
	"quantile_over_time": false,
	"stddev_over_time":   false,
	"stdvar_over_time":   false,
	"last_over_time":     true,
	"present_over_time":  true,
	"absent_over_time":   true,
	//instant
	"":    true,
	"abs": true, "absent": true, "ceil": true, "exp": true, "floor": true,
	"ln": true, "log2": true, "log10": true, "round": true, "scalar": true,
	"sgn": true, "sort": true, "sqrt": true, "timestamp": true, "atan": true,
	"cos": true, "cosh": true, "sin": true, "sinh": true, "tan": true,
	"tanh": true, "deg": true, "rad": true,
	//agg
	"sum":   true,
	"min":   true,
	"max":   true,
	"group": true,
	"avg":   true,
}

// TranspileSelect routes one selector read and plans its SQL. It settles
// req.Hints in place, and takes the database fields of the planner context
// from base.
//
// A tagged selector that is not a substitute steps on its grid. It reads raw
// samples when its grid is off the 15s lattice or its function needs sample
// timestamps. An untagged selector routes on the hints alone.
func TranspileSelect(base shared.PlannerContext, req SelectRequest) (*TranspileResponse, error) {
	hints := req.Hints
	var grid *planner.Grid
	matchers := req.Matchers
	if g, rest, ok := planner.GridFromMatchers(matchers); ok {
		grid = &g
		matchers = rest
	}
	isSupported, ok := supportedFunctions[hints.Func]
	sub := substituteFor(matchers, req.Substitutes)
	if grid != nil && sub == nil {
		hints.Step = grid.StepMs
	}
	if grid == nil || sub != nil || hints.Range > 0 {
		AdjustHintsForRate(hints)
	}

	if !config.Cloki.Setting.ClokiReader.Compat_4_0_19 {
		hints.Start = hints.Start / planner.LatticeMs * planner.LatticeMs
	}

	useRawData := !req.VersionInfo.Metrics15sAvailable((hints.Start-hints.Range)*1000000) ||
		hints.Start%planner.LatticeMs != 0 ||
		hints.Step < planner.LatticeMs ||
		(hints.Range > 0 && hints.Range < planner.LatticeMs) ||
		!(isSupported || !ok)
	if grid != nil {
		useRawData = useRawData || !grid.OnLattice(planner.SelectorWindowMs(hints.Range)) ||
			planner.NeedsSampleTimestamps(hints.Func) ||
			(planner.GridEdgeMs(*grid, hints.Range) == planner.LatticeMs && !distinctKeyed(hints))
	}

	start := hints.Start - hints.Range

	ctx := base
	ctx.From = time.Unix(0, start*1000000)
	ctx.To = time.Unix(0, hints.End*1000000)
	ctx.Step = time.Millisecond * time.Duration(hints.Step)
	ctx.Type = 2
	ctx.VersionInfo = req.VersionInfo

	if sub != nil {
		q, err := sub.Request.Process(&ctx)
		if err != nil {
			return nil, err
		}
		return &TranspileResponse{Query: q, Route: RouteSubstitute}, nil
	}

	if useRawData {
		return TranspileLabelMatchers(hints, &ctx, grid, matchers...)
	}
	return TranspileLabelMatchersDownsample(hints, &ctx, grid, matchers...)
}

// distinctKeyed reports whether a metrics_15s read of hints keeps the epoch
// keys of functions that need distinct samples.
func distinctKeyed(hints *storage.SelectHints) bool {
	return hints.Range > 0 && planner.NeedsDistinctSamples(hints.Func)
}

// substituteFor returns the substitute the selector's metric name stands for.
func substituteFor(matchers []*labels.Matcher, subs map[string]*promql_parser.Substitute) *promql_parser.Substitute {
	for _, m := range matchers {
		if m.Name == "__name__" {
			if sub, ok := subs[m.Value]; ok {
				return sub
			}
		}
	}
	return nil
}

// AdjustHintsForRate settles the step the rest of the request runs on.
//
// Two separate jobs. A query with no step of its own (an instant query), or a
// range function the engine reports no range for (its argument is a subquery
// rather than a matrix selector), has nothing to size a bucket against; 15s is
// the metrics_15s grid, the finest step that table can answer at.
//
// Otherwise the only adjustment is the one the planners need: a function that
// measures a change across samples cannot answer from a single bucket, so its
// step is capped to what planner.BucketResolution says that takes. Capping here
// rather than only inside the planner is what keeps it visible to useRawData
// in TranspileSelect -- a step the cap drops under the 15s grid is one
// metrics_15s cannot serve at all, and routes to raw samples instead of to a
// bucket finer than the table's own resolution. The planners apply the same
// function to the same numbers and so reach the same width; it is idempotent,
// so calling it at both layers is not a conflict.
func AdjustHintsForRate(hints *storage.SelectHints) {
	if hints.Step != 0 && !planner.NeedsDistinctSamples(hints.Func) {
		return
	}
	if hints.Step == 0 || hints.Range == 0 {
		hints.Step = max(hints.Range/2, 15000)
		return
	}
	hints.Step = planner.BucketResolution(
		time.Duration(hints.Step)*time.Millisecond,
		time.Duration(hints.Range)*time.Millisecond).Milliseconds()
}
