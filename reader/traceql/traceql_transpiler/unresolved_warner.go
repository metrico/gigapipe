package traceql_transpiler

import (
	"context"
	gosql "database/sql"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

const (
	// unresolvedMaxIDs is the most trace ids one warning lists.
	unresolvedMaxIDs = 20
	// unresolvedProbeTimeout is one probe's time limit across its reads and jumps.
	unresolvedProbeTimeout = 30 * time.Second
)

// processWarner is the one warner of the process.
var processWarner = &unresolvedWarner{
	now:      time.Now,
	readonly: readReadonly,
	probe:    probeUnresolved,
	warn:     func(s string) { logger.Warn(s) },
	debug:    logger.Debug,
	timeout:  unresolvedProbeTimeout,
}

// WarnUnresolved logs the unresolved traces of a structural query's chain once
// reqCtx ends. Call it after the request's read succeeds.
func WarnUnresolved(reqCtx context.Context, query *traceql_ir.Query, planCtx *shared.PlannerContext) {
	processWarner.after(reqCtx, query, planCtx)
}

// unresolvedFound is one probe result row.
type unresolvedFound struct {
	Jump string
	N    uint64
	IDs  []string
}

// unresolvedWarner logs the unresolved traces its probe finds.
type unresolvedWarner struct {
	now      func() time.Time
	readonly func(context.Context, model.ISqlxDB) (uint64, error)
	probe    func(context.Context, *traceql_ir.Query, *shared.PlannerContext) ([]unresolvedFound, error)
	warn     func(string)
	debug    func(...any)
	timeout  time.Duration

	mu   sync.Mutex
	last time.Time
}

// after runs the probe of a structural query once reqCtx ends, over planCtx as
// it is at this call.
func (w *unresolvedWarner) after(reqCtx context.Context, query *traceql_ir.Query, planCtx *shared.PlannerContext) {
	if query == nil || !query.HasStructural() {
		return
	}
	snapshot := *planCtx
	context.AfterFunc(reqCtx, func() { w.run(reqCtx, query, &snapshot) })
}

// claim takes the probe slot of the current minute.
func (w *unresolvedWarner) claim() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	if !w.last.IsZero() && now.Sub(w.last) < time.Minute {
		return false
	}
	w.last = now
	return true
}

// run probes query, at most once a minute, and logs what it finds, even when
// the probe fails midway. The probe outlives reqCtx up to its own timeout.
func (w *unresolvedWarner) run(reqCtx context.Context, query *traceql_ir.Query, planCtx *shared.PlannerContext) {
	defer func() {
		if r := recover(); r != nil {
			w.debug("TraceQL unresolved-trace probe: panic: ", r, " stack: ", string(debug.Stack()))
		}
	}()
	if !w.claim() {
		return
	}
	ctx, cancel := w.limited(context.WithoutCancel(reqCtx), planCtx.CHDb)
	defer cancel()
	if ctx.Err() != nil {
		return
	}
	found, err := w.probe(ctx, query, planCtx)
	if err != nil {
		if pastLimit(ctx) {
			err = fmt.Errorf("%w: %w", dsn.LimitReached(w.timeout), err)
		}
		w.debug("TraceQL unresolved-trace probe: ", err)
	}
	n, ids := mergeUnresolved(found)
	if n == 0 {
		return
	}
	r := planCtx.AncestryRounds
	w.warn(fmt.Sprintf("TraceQL structural query: %d trace(s) have unresolved ancestry "+
		"(deeper than 2^%d spans at %s=%d, or parent links form a cycle); "+
		"their unresolved spans are treated as unreachable; trace_ids=[%s]",
		n, r, traceqlconfig.AncestryRoundsEnv, r, strings.Join(ids, ", ")))
}

// limited is ctx under one probe's time limit, which includes reading readonly.
// The read has no deadline. The probe has one only when readonly reads 0.
func (w *unresolvedWarner) limited(ctx context.Context, db model.ISqlxDB) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(w.timeout)
	timed, cancel := dsn.WithLimit(ctx, w.timeout)
	readonly, err := w.readonly(timed, db)
	if err != nil {
		w.debug("TraceQL unresolved-trace probe: reading readonly: ", err)
		return timed, cancel
	}
	if readonly != 0 {
		return timed, cancel
	}
	bounded, stop := context.WithDeadline(timed, deadline)
	return bounded, func() {
		stop()
		cancel()
	}
}

// pastLimit reports whether ctx has ended or its deadline has passed. A read
// under the deadline can fail just before ctx ends.
func pastLimit(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

// readReadonly is the readonly setting of db's user.
func readReadonly(ctx context.Context, db model.ISqlxDB) (uint64, error) {
	rows, err := db.QueryCtx(ctx, "SELECT getSetting('readonly')")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, gosql.ErrNoRows
	}
	var readonly uint64
	err = rows.Scan(&readonly)
	return readonly, err
}

// mergeUnresolved is found's trace count and first distinct ids. The count is the distinct ids
// when every row lists all it counted, else an estimate: the largest jump's sum, at least those ids.
func mergeUnresolved(found []unresolvedFound) (uint64, []string) {
	perJump := map[string]uint64{}
	listedAll := true
	var ids []string
	for _, f := range found {
		perJump[f.Jump] += f.N
		ids = append(ids, f.IDs...)
		listedAll = listedAll && uint64(len(f.IDs)) == f.N
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	n := uint64(len(ids))
	if !listedAll {
		for _, c := range perJump {
			n = max(n, c)
		}
	}
	return n, ids[:min(len(ids), unresolvedMaxIDs)]
}

// unresolvedPortion is planCtx for one portion of the probe, on ctx.
func unresolvedPortion(ctx context.Context, planCtx *shared.PlannerContext, i int64) *shared.PlannerContext {
	res := *planCtx
	res.Ctx = ctx
	res.RandomFilter = shared.RandomFilter{}
	if planCtx.Portions > 1 {
		res.RandomFilter = shared.RandomFilter{Max: int(planCtx.Portions), I: int(i)}
	}
	return &res
}

// unresolvedReads is the contexts the probe reads planCtx in, on ctx: the slices
// a walk evaluated, or else its portions.
func unresolvedReads(ctx context.Context, planCtx *shared.PlannerContext) []*shared.PlannerContext {
	if planCtx.TracesSlices != nil {
		res := make([]*shared.PlannerContext, len(planCtx.TracesSlices))
		for i, slice := range planCtx.TracesSlices {
			read := *planCtx
			read.Ctx = ctx
			read.From, read.To, read.TracesCandidates = slice.From, slice.To, slice.Candidates
			res[i] = &read
		}
		return res
	}
	res := make([]*shared.PlannerContext, max(planCtx.Portions, 1))
	for i := range res {
		res[i] = unresolvedPortion(ctx, planCtx, int64(i))
	}
	return res
}

// probeUnresolved runs each probe statement over each of the request's reads,
// in order, and returns what it found up to the first error.
func probeUnresolved(ctx context.Context, query *traceql_ir.Query, planCtx *shared.PlannerContext) ([]unresolvedFound, error) {
	plan, err := clickhouse_transpiler.PlanUnresolvedProbes(query)
	if err != nil {
		return nil, err
	}
	var found []unresolvedFound
	for _, read := range unresolvedReads(ctx, planCtx) {
		probes, err := plan.Process(read)
		if err != nil {
			return found, err
		}
		for _, probe := range probes {
			stmt, err := probe.Select.String(sql.DefaultCtx())
			if err != nil {
				return found, err
			}
			found, err = readUnresolved(ctx, planCtx.CHDb, probe.Jump, stmt, found)
			if err != nil {
				return found, err
			}
		}
	}
	return found, nil
}

// readUnresolved appends every row of stmt to found.
func readUnresolved(ctx context.Context, db model.ISqlxDB, jump, stmt string, found []unresolvedFound) ([]unresolvedFound, error) {
	rows, err := db.QueryCtx(ctx, stmt)
	if err != nil {
		return found, err
	}
	defer rows.Close()
	for rows.Next() {
		row := unresolvedFound{Jump: jump}
		if err := rows.Scan(&row.N, &row.IDs); err != nil {
			return found, err
		}
		found = append(found, row)
	}
	return found, rows.Err()
}
