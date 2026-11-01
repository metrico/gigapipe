package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// stopTimeout bounds the kill and the lease release once the run stops.
const stopTimeout = 30 * time.Second

// migration is one run holding the lease.
type migration struct {
	exec Executor
	// control runs the kill, outside any limits or gating on exec.
	control Executor
	records migrationRecords
	// tables names the tables the run reads and writes.
	tables migrationTables
	lease  *migrationLease
	opts   MetricMigrationOptions
	// ratioKnown is whether the server lists max_bytes_ratio_before_external_sort.
	ratioKnown bool
}

func (m *migration) failed(step string, err error) error {
	return &MigrationFailedError{Database: m.opts.Database, Step: step, Err: err}
}

// run migrates the planned units newest first, copies series and metadata, then advances
// the watermark to the top.
func (m *migration) run(ctx context.Context) (MetricMigrationReport, error) {
	report := MetricMigrationReport{Database: m.opts.Database}
	if err := m.probeLimits(ctx); err != nil {
		return report, m.failed(opSetting, err)
	}
	recs, err := m.records.all(ctx)
	if err != nil {
		return report, m.failed("plan", err)
	}
	now := m.opts.now()
	floor := planFloor(now, m.opts.SamplesDays, recs)
	rollup, err := m.rollupHistory(ctx, now, planEdge(now, m.opts.RawDays, floor))
	if err != nil {
		return report, m.failed("plan", err)
	}
	plan := planMigration(now, m.opts.SamplesDays, m.opts.RawDays, m.opts.Margin, m.opts.Lookback, recs, rollup)
	if _, ok := recordedTime(recs, floorRecord); !ok {
		if err = m.records.put(ctx, floorRecord, strconv.FormatInt(plan.floor.UnixMilli(), 10)); err != nil {
			return report, m.failed("plan", err)
		}
	}
	for i, u := range plan.units {
		start := m.opts.now()
		rows, err := m.unit(ctx, u)
		if err != nil {
			m.kill(ctx, unitQueryID(m.opts.Database, u), unitLabel(u))
			return report, m.failed(fmt.Sprintf("unit %d/%d %s", i+1, len(plan.units), unitLabel(u)), err)
		}
		report.Units++
		if u.kind == unit15s {
			report.Rollup15sSamples += rows
		} else {
			report.RawSamples += rows
		}
		if u.recovery {
			report.Recovered++
		}
		m.opts.Logger.Info(fmt.Sprintf("metric migration: %d/%d %s: %d rows in %s",
			i+1, len(plan.units), unitLabel(u), rows, m.opts.now().Sub(start).Round(time.Millisecond)))
		if i+1 < len(plan.units) {
			if err = m.opts.sleep(ctx, m.opts.Pause); err != nil {
				return report, m.failed("pause", err)
			}
		}
	}
	if report.Series, err = m.series(ctx, recs[seriesRecord] == unitDone, plan.watermark, rollup != nil); err != nil {
		m.kill(ctx, runQueryID(m.opts.Database, opSeries), opSeries)
		return report, m.failed(opSeries, err)
	}
	if report.Metadata, err = m.insert(ctx, opMetadata, m.tables.metadataOut, m.tables.metadataSQL()); err != nil {
		m.kill(ctx, runQueryID(m.opts.Database, opMetadata), opMetadata)
		return report, m.failed(opMetadata, err)
	}
	if err = m.records.put(ctx, watermarkRecord, strconv.FormatInt(plan.top.UnixMilli(), 10)); err != nil {
		return report, m.failed("watermark", err)
	}
	return report, nil
}

// rollupHistory reads the hours below edge holding metrics_15s metric rows within its
// lifetime as of now, or nil when metrics_15s does not exist.
func (m *migration) rollupHistory(ctx context.Context, now, edge time.Time) (*rollupHistory, error) {
	var exists uint8
	if err := m.exec.Scalar(ctx, m.statement(opExists, m.tables.rollup, existsSQL(m.tables.rollup)), &exists); err != nil {
		return nil, err
	}
	if exists != 1 {
		return nil, nil
	}
	h := &rollupHistory{}
	if m.opts.RollupDays > 0 {
		h.lifetime = hourBefore(now, m.opts.RollupDays)
	}
	err := m.exec.Rows(ctx, m.statement(op15sHours, m.tables.rollup, m.tables.rollup15sHoursSQL(h.lifetime, edge)),
		func(scan func(dest ...any) error) error {
			var hour int64
			if err := scan(&hour); err != nil {
				return err
			}
			h.ends = append(h.ends, time.UnixMilli(hour*time.Hour.Milliseconds()).UTC())
			return nil
		})
	return h, err
}

// kill stops the statements still running under queryID once ctx is cancelled: on a cluster,
// first on this node, then on every node.
func (m *migration) kill(ctx context.Context, queryID, label string) {
	if ctx.Err() == nil {
		return
	}
	kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer cancel()
	kills := []string{killSQL(queryID)}
	if m.tables.cluster != "" {
		kills = append(kills, m.tables.killClusterSQL(queryID))
	}
	for _, sql := range kills {
		// The kill carries no limits and is not gated.
		if _, err := m.control.Exec(kctx, Statement{Op: opKill, SQL: sql}); err != nil {
			m.opts.Logger.Error(fmt.Sprintf("metric migration: kill %s: %v", label, err))
		}
	}
}

// unitLabel names a unit in progress lines and errors.
func unitLabel(u migrationUnit) string {
	return fmt.Sprintf("%s (%s, %s]", u.kind, u.from.UTC().Format(time.RFC3339), u.to.UTC().Format(time.RFC3339))
}

// statement builds every unit and run statement; lease and run-state records build their own.
func (m *migration) statement(op, table, sql string) Statement {
	return Statement{Op: op, Table: table, SQL: sql, Settings: m.limitSettings()}
}

// runStatement is a series or metadata statement, run under the run's query id for op.
func (m *migration) runStatement(op, table, sql string) Statement {
	s := m.statement(op, table, sql)
	s.QueryID = runQueryID(m.opts.Database, op)
	return s
}

// unitStatement is a statement of unit u, run under the unit's query id.
func (m *migration) unitStatement(u migrationUnit, op, table, sql string) Statement {
	s := m.statement(op, table, sql)
	s.QueryID = unitQueryID(m.opts.Database, u)
	return s
}

// unit records u started, fills its hour and records it done. It returns the samples it
// filled the hour with.
func (m *migration) unit(ctx context.Context, u migrationUnit) (uint64, error) {
	held, err := m.lease.held(ctx)
	if err != nil {
		return 0, err
	}
	if !held {
		return 0, errLeaseLost
	}
	if err = m.records.put(ctx, u.name(), unitStarted); err != nil {
		return 0, err
	}
	fill := m.rawUnit
	if u.kind == unit15s {
		fill = m.unit15s
	}
	rows, err := fill(ctx, u)
	if err != nil {
		return 0, err
	}
	// A 15s unit under a raw record also records its 15s name done, before the raw one.
	if u.rawRecord {
		if err = m.records.put(ctx, migrationUnit{kind: unit15s, to: u.to}.name(), unitDone); err != nil {
			return 0, err
		}
	}
	if err = m.records.put(ctx, u.name(), unitDone); err != nil {
		return 0, err
	}
	return rows, nil
}

// rawUnit copies the unit's samples into raw, rebuilds each tier's buckets of the hour from raw
// and checks them. It returns the raw samples the copy added; when it added none and u is not a
// recovery unit, the rebuild and check are skipped.
func (m *migration) rawUnit(ctx context.Context, u migrationUnit) (uint64, error) {
	before, _, err := m.countRaw(ctx, u)
	if err != nil {
		return 0, err
	}
	split := m.opts.Split
	if err = m.copy(ctx, u, &split); err != nil {
		return 0, err
	}
	after, expected, err := m.countRaw(ctx, u)
	if err != nil {
		return 0, err
	}
	rows := uint64(0)
	if after > before {
		rows = after - before
	}
	if rows > 0 || u.recovery {
		if err = m.tiers(ctx, u, &split); err != nil {
			return 0, err
		}
		if err = m.check(ctx, u, "raw", expected); err != nil {
			return 0, err
		}
	}
	return rows, nil
}

// unit15s rebuilds each tier's buckets of the hour from the unit's metrics_15s source samples
// and checks them. With no source samples it skips both, clearing the tiers of a unit under a
// started raw record. It returns the source samples.
func (m *migration) unit15s(ctx context.Context, u migrationUnit) (uint64, error) {
	var rows, expected uint64
	if err := m.exec.Scalar(ctx, m.unitStatement(u, opCount, m.tables.rollup, m.tables.count15sSQL(u)), &rows, &expected); err != nil {
		return 0, err
	}
	if rows == 0 {
		if u.rawRecord {
			return 0, m.clearTiers(ctx, u)
		}
		return 0, nil
	}
	split := m.opts.Split
	if err := m.tiers(ctx, u, &split); err != nil {
		return 0, err
	}
	return rows, m.check(ctx, u, legacyRollup, expected)
}

// tiers rebuilds each tier's buckets of the unit's hour, carrying the split from tier to tier.
func (m *migration) tiers(ctx context.Context, u migrationUnit, split *int) error {
	for _, tier := range metricTiers {
		if err := m.tier(ctx, u, tier, split); err != nil {
			return err
		}
	}
	return nil
}

// check fails unless each tier counts at least expected samples in the unit's hour, the
// non-stale samples of source.
func (m *migration) check(ctx context.Context, u migrationUnit, source string, expected uint64) error {
	for _, tier := range metricTiers {
		var sum uint64
		if err := m.exec.Scalar(ctx, m.unitStatement(u, opCheck, m.tables.tier(tier), m.tables.checkTierSQL(tier, u)), &sum); err != nil {
			return err
		}
		if sum < expected {
			return fmt.Errorf("%s holds %d samples, %s holds %d; the hour stays started and the next run rebuilds it",
				tier.Table, sum, source, expected)
		}
	}
	return nil
}

// countRaw counts the raw samples of the unit's hour, then its non-stale ones.
func (m *migration) countRaw(ctx context.Context, u migrationUnit) (n, live uint64, err error) {
	err = m.exec.Scalar(ctx, m.unitStatement(u, opCount, m.tables.raw, m.tables.countRawSQL(u)), &n, &live)
	return n, live, err
}

// series copies every series, joined with metrics_15s when it exists, and records it done;
// once done with a watermark recorded, it copies the series seen since watermark − lookback.
func (m *migration) series(ctx context.Context, done bool, watermark time.Time, rollup bool) (uint64, error) {
	if done && !watermark.IsZero() {
		since := watermark.Add(-m.opts.Lookback)
		return m.insert(ctx, opSeries, m.tables.seriesOut, m.tables.seriesSQL(false, since))
	}
	n, err := m.insert(ctx, opSeries, m.tables.seriesOut, m.tables.seriesSQL(rollup, time.Time{}))
	if err != nil {
		return 0, err
	}
	return n, m.records.put(ctx, seriesRecord, unitDone)
}

// insert runs the series or metadata insert op into table and returns the rows it added: the
// rows written on a single node, the change in the table's FINAL count on a cluster.
func (m *migration) insert(ctx context.Context, op, table, sql string) (uint64, error) {
	if m.tables.cluster == "" {
		return m.exec.Exec(ctx, m.runStatement(op, table, sql))
	}
	before, err := m.countFinal(ctx, op, table)
	if err != nil {
		return 0, err
	}
	if _, err = m.exec.Exec(ctx, m.runStatement(op, table, sql)); err != nil {
		return 0, err
	}
	after, err := m.countFinal(ctx, op, table)
	if err != nil || after < before {
		return 0, err
	}
	return after - before, nil
}

// countFinal counts table's rows after merging, under the query id of op.
func (m *migration) countFinal(ctx context.Context, op, table string) (uint64, error) {
	var n uint64
	s := m.statement(opCount, table, fmt.Sprintf("SELECT count() FROM %s FINAL", table))
	s.QueryID = runQueryID(m.opts.Database, op)
	err := m.exec.Scalar(ctx, s, &n)
	return n, err
}
