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

// settingKnownOnClusterSQL counts, per node of cluster, the settings named name.
func settingKnownOnClusterSQL(cluster, name string) string {
	return fmt.Sprintf("SELECT hostName() AS host, countIf(name = '%s') AS n "+
		"FROM clusterAllReplicas(%s, system.settings) GROUP BY host", name, quoteString(cluster))
}

// probeLimits reads which optional limit settings the server knows: on a cluster, every node.
func (m *migration) probeLimits(ctx context.Context) error {
	if c := m.tables.cluster; c != "" {
		hosts, known := 0, true
		err := m.exec.Rows(ctx, m.statement(opSetting, "system.settings", settingKnownOnClusterSQL(c, externalSortRatio)),
			func(scan func(dest ...any) error) error {
				var host string
				var n uint64
				if err := scan(&host, &n); err != nil {
					return err
				}
				hosts++
				known = known && n > 0
				return nil
			})
		m.ratioKnown = err == nil && hosts > 0 && known
		return err
	}
	var n uint64
	if err := m.exec.Scalar(ctx, m.statement(opSetting, "system.settings", settingKnownSQL(externalSortRatio)), &n); err != nil {
		return err
	}
	m.ratioKnown = n > 0
	return nil
}

// limitSettings are the per-query limits every unit and run statement carries, with insert deduplication off.
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
		"insert_deduplicate":                        0,
	}
	if m.ratioKnown {
		s[externalSortRatio] = 0
	}
	if m.tables.cluster != "" {
		s["insert_distributed_sync"] = 1
		s["skip_unavailable_shards"] = 0
		// Writes and reads of a unit go to one replica of each shard: the local one, else the first with the fewest errors.
		s["load_balancing"] = "in_order"
		s["use_hedged_requests"] = 0
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
	396: "max_temporary_data_on_disk_size_for_query",
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

// copy copies the raw unit's samples into raw, split into *n parts.
func (m *migration) copy(ctx context.Context, u migrationUnit, n *int) error {
	return m.splitting(u, n, func(parts int) error {
		return m.parts(ctx, u, opCopy, m.tables.raw, parts, func(p int) string { return m.tables.copyRawSQL(u, parts, p) })
	})
}

// tier deletes the tier's buckets of the hour whole, then rebuilds them split into *n parts.
// The first attempt deletes only when the tier holds buckets of the hour; each retry deletes
// the hour again.
func (m *migration) tier(ctx context.Context, u migrationUnit, tier metricTier, n *int) error {
	buckets, err := m.tierBuckets(ctx, u, tier)
	if err != nil {
		return err
	}
	retry := false
	return m.splitting(u, n, func(parts int) error {
		if buckets > 0 || retry {
			if err := m.deleteTier(ctx, u, tier); err != nil {
				return err
			}
		}
		retry = true
		return m.parts(ctx, u, opRebuild, m.tables.tier(tier), parts, func(p int) string { return m.rebuildSQL(tier, u, parts, p) })
	})
}

// clearTiers deletes each tier's buckets of the unit's hour where it holds any.
func (m *migration) clearTiers(ctx context.Context, u migrationUnit) error {
	for _, tier := range metricTiers {
		buckets, err := m.tierBuckets(ctx, u, tier)
		if err != nil {
			return err
		}
		if buckets == 0 {
			continue
		}
		if err := m.deleteTier(ctx, u, tier); err != nil {
			return err
		}
	}
	return nil
}

// tierBuckets counts the tier's buckets of the unit's hour.
func (m *migration) tierBuckets(ctx context.Context, u migrationUnit, tier metricTier) (uint64, error) {
	var buckets uint64
	err := m.exec.Scalar(ctx, m.unitStatement(u, opCount, m.tables.tier(tier), m.tables.countTierSQL(tier, u)), &buckets)
	return buckets, err
}

// deleteTier deletes the tier's buckets of the unit's hour, waiting for the mutation.
func (m *migration) deleteTier(ctx context.Context, u migrationUnit, tier metricTier) error {
	del := m.unitStatement(u, opDelete, tier.Table, m.tables.deleteTierSQL(tier, u))
	del.Settings["mutations_sync"] = 2
	if m.tables.cluster != "" {
		del.Settings["distributed_ddl_task_timeout"] = wholeSeconds(m.opts.MaxExecutionTime)
	}
	_, err := m.exec.Exec(ctx, del)
	return err
}

// rebuildSQL rebuilds the tier from raw for a raw unit and from metrics_15s for a 15s unit.
func (m *migration) rebuildSQL(tier metricTier, u migrationUnit, parts, part int) string {
	if u.kind == unit15s {
		return m.tables.rebuild15sTierSQL(tier, u, parts, part)
	}
	return m.tables.rebuildTierSQL(tier, u, parts, part)
}

// warn logs msg at warning level when the logger has one.
func (m *migration) warn(msg string) {
	if w, ok := m.opts.Logger.(interface{ Warn(args ...any) }); ok {
		w.Warn(msg)
		return
	}
	m.opts.Logger.Info(msg)
}
