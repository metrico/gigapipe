package maintenance

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// opSetting reads whether the server lists a setting.
const opSetting = "setting"

const externalSortRatio = "max_bytes_ratio_before_external_sort"

// settingKnownSQL counts the server's settings named name.
func settingKnownSQL(name string) string {
	return fmt.Sprintf("SELECT count() FROM system.settings WHERE name = '%s'", name)
}

// probeLimits reads which optional limit settings the server knows.
func (m *migration) probeLimits(ctx context.Context) error {
	var n uint64
	if err := m.exec.Scalar(ctx, m.statement(opSetting, "system.settings", settingKnownSQL(externalSortRatio)), &n); err != nil {
		return err
	}
	m.ratioKnown = n > 0
	return nil
}

// limitSettings are the per-query limits every unit and run statement carries.
func (m *migration) limitSettings() map[string]any {
	o := m.opts
	s := map[string]any{
		"max_threads":                               o.MaxThreads,
		"max_insert_threads":                        1,
		"max_memory_usage":                          o.MaxMemory,
		"max_execution_time":                        wholeSeconds(o.MaxExecutionTime),
		"max_bytes_before_external_sort":            o.ExternalSort,
		"max_bytes_before_external_group_by":        o.ExternalGroupBy,
		"max_temporary_data_on_disk_size_for_query": o.MaxSpill,
	}
	if m.ratioKnown {
		s[externalSortRatio] = 0
	}
	return s
}

// wholeSeconds is d in seconds, rounded up and at least 1.
func wholeSeconds(d time.Duration) int {
	return max(1, int(math.Ceil(d.Seconds())))
}

// limitSettingOf names the setting behind each limit error a unit retries smaller.
var limitSettingOf = map[int32]string{
	241: "max_memory_usage",
	159: "max_execution_time",
	307: "max_temporary_data_on_disk_size_for_query",
}

// limitError reports the code and setting of a ClickHouse limit error.
func limitError(err error) (int32, string, bool) {
	var ex *clickhouse.Exception
	if !errors.As(err, &ex) {
		return 0, "", false
	}
	setting, ok := limitSettingOf[ex.Code]
	return ex.Code, setting, ok
}

// partError is the error of a split part; only these retry smaller.
type partError struct{ err error }

func (e *partError) Error() string { return e.err.Error() }
func (e *partError) Unwrap() error { return e.err }

// splitting runs body split into *n parts. On a limit error in a part it doubles *n, up to
// MaxSplit, and runs body again; *n keeps the split the body last ran at.
func (m *migration) splitting(u migrationUnit, n *int, body func(parts int) error) error {
	for {
		err := body(*n)
		var pe *partError
		if !errors.As(err, &pe) {
			return err
		}
		code, setting, ok := limitError(pe.err)
		if !ok {
			return pe.err
		}
		if *n >= m.opts.MaxSplit {
			m.warn(fmt.Sprintf("metric migration: %s hit %s (code %d) at the maximum split %d; giving up", unitLabel(u), setting, code, *n))
			return pe.err
		}
		*n = min(2**n, m.opts.MaxSplit)
		m.warn(fmt.Sprintf("metric migration: %s hit %s (code %d); retrying split %d", unitLabel(u), setting, code, *n))
	}
}

// parts runs the step's statement for each part of a split into parts.
func (m *migration) parts(ctx context.Context, u migrationUnit, op, table string, parts int, sql func(part int) string) error {
	for p := range max(parts, 1) {
		s := m.unitStatement(u, op, table, sql(p))
		s.Part, s.Parts = p, parts
		if _, err := m.exec.Exec(ctx, s); err != nil {
			return &partError{err}
		}
	}
	return nil
}

// copy copies the unit's samples_v3 rows into raw, split into *n parts.
func (m *migration) copy(ctx context.Context, u migrationUnit, n *int) error {
	return m.splitting(u, n, func(parts int) error {
		return m.parts(ctx, u, opCopy, rawSamples, parts, func(p int) string { return copyRawSQL(u, parts, p) })
	})
}

// tier deletes the tier's buckets of the hour whole, then rebuilds them split into *n parts.
// Each retry deletes the hour again.
func (m *migration) tier(ctx context.Context, u migrationUnit, tier metricTier, n *int) error {
	return m.splitting(u, n, func(parts int) error {
		del := m.unitStatement(u, opDelete, tier.Table, deleteTierSQL(tier, u))
		del.Settings["mutations_sync"] = 2
		if _, err := m.exec.Exec(ctx, del); err != nil {
			return err
		}
		return m.parts(ctx, u, opRebuild, tier.Table, parts, func(p int) string { return rebuildTierSQL(tier, u, parts, p) })
	})
}

// warn logs msg at warning level when the logger has one.
func (m *migration) warn(msg string) {
	if w, ok := m.opts.Logger.(interface{ Warn(args ...any) }); ok {
		w.Warn(msg)
		return
	}
	m.opts.Logger.Info(msg)
}
