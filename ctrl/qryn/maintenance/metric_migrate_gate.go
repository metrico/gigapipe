package maintenance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/metrico/qryn/v5/ctrl/logger"
)

const (
	opGate      = "gate"
	opGateTable = "gate table"
)

const (
	// gatePoll is how often a waiting gate reads the signals again.
	gatePoll = 10 * time.Second
	// progressEvery is the interval of back-off and running-unit lines.
	progressEvery = 60 * time.Second
)

// gateSQL reads each back-off signal as (signal, value, limit, table, mutation): limit is the
// setting a fraction applies to, table the signal's busiest table (mutations: the oldest mutation's
// table) where one is known, mutation the oldest unfinished mutation's id.
func gateSQL() string {
	return `SELECT 'parts' AS signal,
  (SELECT toFloat64(maxIf(value, metric = 'MaxPartCountForPartition')) FROM system.asynchronous_metrics) AS value,
  (SELECT toFloat64OrZero(any(value)) FROM system.merge_tree_settings WHERE name = 'parts_to_delay_insert') AS lim,
  '' AS tbl,
  '' AS mid
UNION ALL
SELECT 'merges', toFloat64(count()), toFloat64(0), toString(ifNull(topK(1)(concat(database, '.', table))[1], '')), ''
  FROM system.merges
UNION ALL
SELECT 'mutations', toFloat64(count()), toFloat64(0),
  argMin(concat(database, '.', table), create_time), argMin(mutation_id, create_time)
  FROM system.mutations WHERE NOT is_done AND latest_fail_reason = ''
UNION ALL
SELECT 'delayed', toFloat64(sum(value)), toFloat64(0), '', ''
  FROM system.metrics WHERE metric = 'DelayedInserts'
UNION ALL
SELECT 'memory',
  (SELECT toFloat64(maxIf(value, metric = 'MemoryResident')) FROM system.asynchronous_metrics),
  (WITH
    (SELECT toFloat64OrZero(any(value)) FROM system.server_settings WHERE name = 'max_server_memory_usage') AS fixed,
    (SELECT toFloat64OrZero(any(value)) FROM system.server_settings WHERE name = 'max_server_memory_usage_to_ram_ratio') AS ratio,
    (SELECT toFloat64(maxIf(value, metric = 'OSMemoryTotal')) FROM system.asynchronous_metrics) AS total,
    (SELECT toFloat64(maxIf(value, metric = 'CGroupMemoryTotal')) FROM system.asynchronous_metrics) AS cgroup,
    if(cgroup > 0 AND cgroup < total, cgroup, total) AS ram
  SELECT if(fixed > 0, fixed, ratio * ram)),
  '',
  ''`
}

// gateClusterSQL reads gateSQL's signals on every replica of cluster, one row per signal and
// node for parts, merges and memory, each against that node's own limit. A parts row's table
// column holds its node's host name.
func gateClusterSQL(cluster string) string {
	all := func(table string) string {
		return fmt.Sprintf("clusterAllReplicas(%s, system.%s)", quoteString(cluster), table)
	}
	return `SELECT 'parts' AS signal, toFloat64(m.parts) AS value, toFloat64(s.lim) AS lim, m.host AS tbl, '' AS mid
  FROM (SELECT hostName() AS host, maxIf(value, metric = 'MaxPartCountForPartition') AS parts
    FROM ` + all("asynchronous_metrics") + ` GROUP BY host) AS m
  LEFT JOIN (SELECT hostName() AS host, toFloat64OrZero(anyIf(value, name = 'parts_to_delay_insert')) AS lim
    FROM ` + all("merge_tree_settings") + ` GROUP BY host) AS s USING host
UNION ALL
SELECT 'merges', toFloat64(count()), toFloat64(0), toString(ifNull(topK(1)(concat(database, '.', table))[1], '')), ''
  FROM ` + all("merges") + ` GROUP BY hostName()
UNION ALL
SELECT 'mutations', toFloat64(count()), toFloat64(0),
  argMin(concat(database, '.', table), create_time), argMin(mutation_id, create_time)
  FROM ` + all("mutations") + ` WHERE NOT is_done AND latest_fail_reason = ''
UNION ALL
SELECT 'delayed', toFloat64(max(delayed)), toFloat64(0), '', ''
  FROM (SELECT hostName() AS host, sum(value) AS delayed FROM ` + all("metrics") + `
    WHERE metric = 'DelayedInserts' GROUP BY host)
UNION ALL
SELECT 'memory', toFloat64(m.resident),
  if(s.fixed > 0, s.fixed, s.ratio * if(m.cgroup > 0 AND m.cgroup < m.total, m.cgroup, m.total)), '', ''
  FROM (SELECT hostName() AS host, maxIf(value, metric = 'MemoryResident') AS resident,
      maxIf(value, metric = 'OSMemoryTotal') AS total, maxIf(value, metric = 'CGroupMemoryTotal') AS cgroup
    FROM ` + all("asynchronous_metrics") + ` GROUP BY host) AS m
  LEFT JOIN (SELECT hostName() AS host,
      toFloat64OrZero(anyIf(value, name = 'max_server_memory_usage')) AS fixed,
      toFloat64OrZero(anyIf(value, name = 'max_server_memory_usage_to_ram_ratio')) AS ratio
    FROM ` + all("server_settings") + ` GROUP BY host) AS s USING host`
}

// gatePartsTableSQL reads the table holding the visible partition with the most active parts
// and that partition's part count; an empty table and 0 when no part is visible.
func gatePartsTableSQL() string {
	return `SELECT argMax(tbl, n), toFloat64(max(n)) FROM (SELECT concat(database, '.', table) AS tbl, count() AS n
  FROM system.parts WHERE active GROUP BY database, table, partition_id)`
}

// gatePartsTableClusterSQL reads gatePartsTableSQL's table and part count over the partitions
// of host, read through cluster.
func gatePartsTableClusterSQL(cluster, host string) string {
	return fmt.Sprintf(`SELECT argMax(tbl, n), toFloat64(max(n)) FROM (SELECT hostName() AS host, concat(database, '.', table) AS tbl, count() AS n
  FROM clusterAllReplicas(%s, system.parts) WHERE active GROUP BY host, database, table, partition_id)
  WHERE host = %s`, quoteString(cluster), quoteString(host))
}

// gateSignals is the order in which over-threshold signals are reported.
var gateSignals = []string{"parts", "memory", "delayed", "merges", "mutations"}

// rank is the signal's position in gateSignals.
func rank(signal string) int {
	for i, s := range gateSignals {
		if s == signal {
			return i
		}
	}
	return len(gateSignals)
}

// String formats the signal's value against its threshold, memory in bytes.
func (p *pressure) String() string {
	if p.signal == "memory" {
		return fmt.Sprintf("%s: %s at threshold %s", p.signal, formatBytes(p.value), formatBytes(p.threshold))
	}
	return fmt.Sprintf("%s: %g at threshold %g", p.signal, p.value, p.threshold)
}

// formatBytes formats n bytes in the largest binary unit it reaches.
func formatBytes(n float64) string {
	for i, unit := range []string{"GiB", "MiB", "KiB"} {
		if size := float64(int64(1) << (30 - 10*i)); n >= size {
			return fmt.Sprintf("%.1f %s", n/size, unit)
		}
	}
	return fmt.Sprintf("%.0f B", n)
}

// pressure is a signal at or over its threshold.
type pressure struct {
	signal           string
	value, threshold float64
	table, mutation  string
	// host is the node a cluster's parts signal was read on.
	host string
}

// gatedExecutor waits at the back-off gate before each statement and logs a running unit's
// rows read every progressEvery.
type gatedExecutor struct {
	exec  Executor
	opts  MetricMigrationOptions
	mu    sync.Mutex
	unit  string
	start time.Time
	shown time.Time
}

func newGatedExecutor(exec Executor, opts MetricMigrationOptions) Executor {
	return &gatedExecutor{exec: exec, opts: opts}
}

func (g *gatedExecutor) Exec(ctx context.Context, s Statement) (uint64, error) {
	if err := g.wait(ctx); err != nil {
		return 0, err
	}
	return g.exec.Exec(ctx, g.watch(s))
}

func (g *gatedExecutor) Scalar(ctx context.Context, s Statement, dest ...any) error {
	if err := g.wait(ctx); err != nil {
		return err
	}
	return g.exec.Scalar(ctx, g.watch(s), dest...)
}

func (g *gatedExecutor) Rows(ctx context.Context, s Statement, row func(scan func(dest ...any) error) error) error {
	if err := g.wait(ctx); err != nil {
		return err
	}
	return g.exec.Rows(ctx, g.watch(s), row)
}

func (g *gatedExecutor) log() logger.ILogger { return g.opts.Logger }

// wait polls the signals every gatePoll until none is over its threshold.
func (g *gatedExecutor) wait(ctx context.Context) error {
	var since, shown time.Time
	for {
		p, err := g.pressure(ctx)
		if err != nil {
			return err
		}
		now := g.opts.now()
		switch {
		case p == nil && since.IsZero():
			return nil
		case p == nil:
			g.log().Info(fmt.Sprintf("metric migration: back-off resumed after %s", now.Sub(since).Round(time.Second)))
			return nil
		case since.IsZero():
			since, shown = now, now
			holder, err := g.holder(ctx, p)
			if err != nil {
				return err
			}
			g.log().Info(fmt.Sprintf("metric migration: back-off on %s%s", p, holder))
		case now.Sub(shown) >= progressEvery:
			shown = now
			holder, err := g.holder(ctx, p)
			if err != nil {
				return err
			}
			g.log().Info(fmt.Sprintf("metric migration: back-off on %s%s, waited %s",
				p, holder, now.Sub(since).Round(time.Second)))
		}
		if err = g.opts.sleep(ctx, gatePoll); err != nil {
			return err
		}
	}
}

// holder is the back-off line's segment naming p's table and, for mutations, its mutation id;
// empty for a signal with no table.
func (g *gatedExecutor) holder(ctx context.Context, p *pressure) (string, error) {
	table, err := g.table(ctx, p)
	switch {
	case err != nil || table == "":
		return "", err
	case p.mutation != "":
		return fmt.Sprintf(", table %s, mutation %s", table, p.mutation), nil
	}
	return ", table " + table, nil
}

// table is the busiest table of p's signal, read from system.parts for the parts signal.
// A parts table holding fewer parts than p's value is reported as not visible.
func (g *gatedExecutor) table(ctx context.Context, p *pressure) (string, error) {
	if p.signal != "parts" {
		return p.table, nil
	}
	var table string
	sql := gatePartsTableSQL()
	if g.opts.Cluster != "" {
		sql = gatePartsTableClusterSQL(g.opts.Cluster, p.host)
	}
	var count float64
	if err := g.exec.Scalar(ctx, Statement{Op: opGateTable, SQL: sql}, &table, &count); err != nil {
		return "", err
	}
	if count < p.value {
		return "not visible to this user", nil
	}
	return table, nil
}

// pressure returns the over-threshold signal first in gateSignals, nil when none is; among
// nodes over on the same signal, the one with the largest value.
func (g *gatedExecutor) pressure(ctx context.Context) (*pressure, error) {
	var found *pressure
	sql := gateSQL()
	if g.opts.Cluster != "" {
		sql = gateClusterSQL(g.opts.Cluster)
	}
	err := g.exec.Rows(ctx, Statement{Op: opGate, SQL: sql}, func(scan func(dest ...any) error) error {
		var p pressure
		var limit float64
		var where string
		if err := scan(&p.signal, &p.value, &limit, &where, &p.mutation); err != nil {
			return err
		}
		// The parts row names its host; the other rows name their table.
		if p.signal == "parts" {
			p.host = where
		} else {
			p.table = where
		}
		if g.over(&p, limit) && (found == nil || rank(p.signal) < rank(found.signal) ||
			(p.signal == found.signal && p.value > found.value)) {
			found = &p
		}
		return nil
	})
	return found, err
}

// over sets p's threshold and reports whether p's value holds the gate.
func (g *gatedExecutor) over(p *pressure, limit float64) bool {
	switch p.signal {
	case "parts":
		p.threshold = g.opts.BackoffParts * limit
		return limit > 0 && p.value >= p.threshold
	case "memory":
		p.threshold = g.opts.BackoffMemory * limit
		return limit > 0 && p.value >= p.threshold
	case "merges":
		p.threshold = float64(g.opts.BackoffMerges)
		return p.value > p.threshold
	case "mutations", "delayed":
		return p.value > 0
	}
	return false
}

// watch chains a progress callback logging the rows read of a unit running past progressEvery.
// A unit is the statements sharing a query id; a statement without one is its own unit.
func (g *gatedExecutor) watch(s Statement) Statement {
	label := s.QueryID + " " + s.Op + " " + s.Table
	key := s.QueryID
	if key == "" {
		key = label
	}
	now := g.opts.now()
	g.mu.Lock()
	if g.unit != key || s.QueryID == "" {
		g.unit, g.start, g.shown = key, now, now
	}
	g.mu.Unlock()
	next := s.Progress
	s.Progress = func(read, written uint64) {
		now := g.opts.now()
		g.mu.Lock()
		due := now.Sub(g.shown) >= progressEvery
		if due {
			g.shown = now
		}
		running := now.Sub(g.start)
		g.mu.Unlock()
		if due {
			g.log().Info(fmt.Sprintf("metric migration: %s running %s: %d rows read",
				label, running.Round(time.Second), read))
		}
		if next != nil {
			next(read, written)
		}
	}
	return s
}
