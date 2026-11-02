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
	lease   *migrationLease
	opts    MetricMigrationOptions
	// ratioKnown is whether the server lists max_bytes_ratio_before_external_sort.
	ratioKnown bool
	// floor is the run's floor, set once planned.
	floor time.Time
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
	rollup, err := m.rollupHistory(ctx, now, floor)
	if err != nil {
		return report, m.failed("plan", err)
	}
	plan := planMigration(now, m.opts.SamplesDays, m.opts.Margin, m.opts.Lookback, recs, rollup)
	m.floor = plan.floor
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
			return report, m.failed(unitLabel(u), err)
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
	if report.Metadata, err = m.exec.Exec(ctx, m.runStatement(opMetadata, metadataTable, metadataSQL())); err != nil {
		m.kill(ctx, runQueryID(m.opts.Database, opMetadata), opMetadata)
		return report, m.failed(opMetadata, err)
	}
	if err = m.records.put(ctx, watermarkRecord, strconv.FormatInt(plan.top.UnixMilli(), 10)); err != nil {
		return report, m.failed("watermark", err)
	}
	return report, nil
}

// rollupHistory reads the hours below floor holding metrics_15s metric rows within its
// lifetime as of now, or nil when metrics_15s does not exist.
func (m *migration) rollupHistory(ctx context.Context, now, floor time.Time) (*rollupHistory, error) {
	var exists uint8
	if err := m.exec.Scalar(ctx, m.statement(opExists, legacyRollup, existsSQL(legacyRollup)), &exists); err != nil {
		return nil, err
	}
	if exists != 1 {
		return nil, nil
	}
	h := &rollupHistory{}
	if m.opts.RollupDays > 0 {
		h.lifetime = hourBefore(now, m.opts.RollupDays)
	}
	err := m.exec.Rows(ctx, m.statement(op15sHours, legacyRollup, rollup15sHoursSQL(h.lifetime, floor)),
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

// kill stops the statements still running under queryID once ctx is cancelled.
func (m *migration) kill(ctx context.Context, queryID, label string) {
	if ctx.Err() == nil {
		return
	}
	kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer cancel()
	// The kill carries no limits and is not gated.
	if _, err := m.control.Exec(kctx, Statement{Op: opKill, SQL: killSQL(queryID)}); err != nil {
		m.opts.Logger.Error(fmt.Sprintf("metric migration: kill %s: %v", label, err))
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
	if err = m.records.put(ctx, u.name(), unitDone); err != nil {
		return 0, err
	}
	return rows, nil
}

// rawUnit copies the unit's samples into raw and rebuilds each tier's buckets of the hour from
// raw. It returns the raw samples the copy added; when it added none and u is not a recovery
// unit, the rebuild is skipped.
func (m *migration) rawUnit(ctx context.Context, u migrationUnit) (uint64, error) {
	before, err := m.countRaw(ctx, u)
	if err != nil {
		return 0, err
	}
	split := m.opts.Split
	if err = m.copy(ctx, u, &split); err != nil {
		return 0, err
	}
	after, err := m.countRaw(ctx, u)
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
	}
	return rows, nil
}

// unit15s rebuilds each tier's buckets of the hour from the unit's metrics_15s source samples,
// skipping the rebuild when there are none. It returns the source samples.
func (m *migration) unit15s(ctx context.Context, u migrationUnit) (uint64, error) {
	var rows uint64
	if err := m.exec.Scalar(ctx, m.unitStatement(u, opCount, legacyRollup, count15sSQL(u, m.floor)), &rows); err != nil {
		return 0, err
	}
	if rows == 0 {
		return 0, nil
	}
	split := m.opts.Split
	return rows, m.tiers(ctx, u, &split)
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

func (m *migration) countRaw(ctx context.Context, u migrationUnit) (uint64, error) {
	var n uint64
	err := m.exec.Scalar(ctx, m.unitStatement(u, opCount, rawSamples, countRawSQL(u)), &n)
	return n, err
}

// series copies every series, joined with metrics_15s when it exists, and records it done;
// once done with a watermark recorded, it copies the series seen since watermark − lookback.
func (m *migration) series(ctx context.Context, done bool, watermark time.Time, rollup bool) (uint64, error) {
	if done && !watermark.IsZero() {
		since := watermark.Add(-m.opts.Lookback)
		return m.exec.Exec(ctx, m.runStatement(opSeries, seriesTable, seriesSQL(false, since)))
	}
	n, err := m.exec.Exec(ctx, m.runStatement(opSeries, seriesTable, seriesSQL(rollup, time.Time{})))
	if err != nil {
		return 0, err
	}
	return n, m.records.put(ctx, seriesRecord, unitDone)
}
