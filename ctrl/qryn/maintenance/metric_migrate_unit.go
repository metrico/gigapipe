package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// migration is one run holding the lease.
type migration struct {
	exec    Executor
	records migrationRecords
	lease   *migrationLease
	opts    MetricMigrationOptions
	// ratioKnown is whether the server lists max_bytes_ratio_before_external_sort.
	ratioKnown bool
}

func (m *migration) failed(step string, err error) error {
	return &MigrationFailedError{Database: m.opts.Database, Step: step, Err: err}
}

// run migrates the planned units newest first, advances the watermark to the top, then
// copies series and metadata.
func (m *migration) run(ctx context.Context) (MetricMigrationReport, error) {
	report := MetricMigrationReport{Database: m.opts.Database}
	if err := m.probeLimits(ctx); err != nil {
		return report, m.failed(opSetting, err)
	}
	recs, err := m.records.all(ctx)
	if err != nil {
		return report, m.failed("plan", err)
	}
	plan := planMigration(m.opts.now(), m.opts.SamplesDays, m.opts.Margin, m.opts.Lookback, recs)
	if _, ok := recordedTime(recs, floorRecord); !ok {
		if err = m.records.put(ctx, floorRecord, strconv.FormatInt(plan.floor.UnixMilli(), 10)); err != nil {
			return report, m.failed("plan", err)
		}
	}
	for i, u := range plan.units {
		start := m.opts.now()
		rows, err := m.unit(ctx, u)
		if err != nil {
			return report, m.failed(unitLabel(u), err)
		}
		report.Units++
		report.RawSamples += rows
		m.opts.Logger.Info(fmt.Sprintf("metric migration: %d/%d %s: %d rows in %s",
			i+1, len(plan.units), unitLabel(u), rows, m.opts.now().Sub(start).Round(time.Millisecond)))
	}
	if err = m.records.put(ctx, watermarkRecord, strconv.FormatInt(plan.top.UnixMilli(), 10)); err != nil {
		return report, m.failed("watermark", err)
	}
	if report.Series, err = m.series(ctx, recs[seriesRecord] == unitDone, plan.watermark); err != nil {
		return report, m.failed(opSeries, err)
	}
	if report.Metadata, err = m.exec.Exec(ctx, m.statement(opMetadata, metadataTable, metadataSQL())); err != nil {
		return report, m.failed(opMetadata, err)
	}
	return report, nil
}

// unitLabel names a unit in progress lines and errors.
func unitLabel(u migrationUnit) string {
	return fmt.Sprintf("%s (%s, %s]", u.kind, u.from.UTC().Format(time.RFC3339), u.to.UTC().Format(time.RFC3339))
}

// statement builds every unit and run statement; lease and run-state records build their own.
func (m *migration) statement(op, table, sql string) Statement {
	return Statement{Op: op, Table: table, SQL: sql, Settings: m.limitSettings()}
}

// unitStatement is a statement of unit u, run under the unit's query id.
func (m *migration) unitStatement(u migrationUnit, op, table, sql string) Statement {
	s := m.statement(op, table, sql)
	s.QueryID = unitQueryID(m.opts.Database, u)
	return s
}

// unit records u started, copies its samples_v3 rows into raw, rebuilds each tier's buckets of
// the hour from raw and records it done. It returns the raw samples the copy added.
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
	for _, tier := range metricTiers {
		if err = m.tier(ctx, u, tier, &split); err != nil {
			return 0, err
		}
	}
	if err = m.records.put(ctx, u.name(), unitDone); err != nil {
		return 0, err
	}
	if after < before {
		return 0, nil
	}
	return after - before, nil
}

func (m *migration) countRaw(ctx context.Context, u migrationUnit) (uint64, error) {
	var n uint64
	err := m.exec.Scalar(ctx, m.unitStatement(u, opCount, rawSamples, countRawSQL(u)), &n)
	return n, err
}

// series copies every series on the first run, joined with metrics_15s when it exists, and
// records it done; later runs copy the series seen since watermark − lookback.
func (m *migration) series(ctx context.Context, done bool, watermark time.Time) (uint64, error) {
	if done {
		since := watermark.Add(-m.opts.Lookback)
		return m.exec.Exec(ctx, m.statement(opSeries, seriesTable, seriesSQL(false, since)))
	}
	var rollup uint8
	if err := m.exec.Scalar(ctx, m.statement(opExists, legacyRollup, existsSQL(legacyRollup)), &rollup); err != nil {
		return 0, err
	}
	n, err := m.exec.Exec(ctx, m.statement(opSeries, seriesTable, seriesSQL(rollup == 1, time.Time{})))
	if err != nil {
		return 0, err
	}
	return n, m.records.put(ctx, seriesRecord, unitDone)
}
