package maintenance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/helputils"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

const (
	importRecordType = "metric_import"
	floorRecord      = "floor"
	seriesRecord     = "series"
	watermarkRecord  = "watermark"
)

// ErrImportLeaseHeld reports that another instance holds the metric import's lease.
var ErrImportLeaseHeld = errors.New("the metric import lease is held by another instance")

// MetricImportOptions configures the metric import.
type MetricImportOptions struct {
	// Dist reads settings through the distributed table.
	Dist bool
	// Instance names this instance in the lease.
	Instance string
	// SamplesDays and RollupDays are the lifetimes of samples_v3 and metrics_15s.
	SamplesDays int
	RollupDays  int
	// LeaseTTL is the age at which a lease is taken over; Renew is how often it is refreshed;
	// Settle is the wait before an acquired lease is read back.
	LeaseTTL time.Duration
	Renew    time.Duration
	Settle   time.Duration
	// Poll is the tail's wait between empty reads; Quiet is how long the old table must
	// receive no metric row above the watermark before the import completes.
	Poll  time.Duration
	Quiet time.Duration
	// Retry is the wait between attempts of RunMetricImport.
	Retry  time.Duration
	Logger logger.ILogger
}

func (o MetricImportOptions) withDefaults() MetricImportOptions {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&o.LeaseTTL, 3*time.Minute)
	def(&o.Renew, time.Minute)
	def(&o.Settle, 5*time.Second)
	def(&o.Poll, time.Minute)
	def(&o.Quiet, time.Hour)
	def(&o.Retry, time.Minute)
	if o.Instance == "" {
		host, _ := os.Hostname()
		o.Instance = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if o.Logger == nil {
		o.Logger = logger.Logger
	}
	return o
}

// RunMetricImport repeats ImportMetrics until the import is complete or ctx ends.
func RunMetricImport(ctx context.Context, db clickhouse.Conn, opts MetricImportOptions) {
	opts = opts.withDefaults()
	for {
		err := ImportMetrics(ctx, db, opts)
		if err == nil {
			return
		}
		if !errors.Is(err, ErrImportLeaseHeld) {
			opts.Logger.Error("metric import: ", err.Error())
		}
		if sleepCtx(ctx, opts.Retry) != nil {
			return
		}
	}
}

// ImportMetrics copies the metric history of the shared tables into the metric stack, on the
// instance holding the lease. It returns nil once the import is recorded complete.
func ImportMetrics(ctx context.Context, db clickhouse.Conn, opts MetricImportOptions) error {
	opts = opts.withDefaults()
	done, err := getSetting(db, opts.Dist, "update", importRecordType)
	if err != nil || done != "" {
		return err
	}
	records := &settingsRecords{db: db, dist: opts.Dist}
	lease := &importLease{
		records:  records,
		instance: opts.Instance,
		ttl:      opts.LeaseTTL,
		settle:   opts.Settle,
		now:      time.Now,
		sleep:    sleepCtx,
	}
	held, err := lease.acquire(ctx)
	if err != nil {
		return err
	}
	if !held {
		return ErrImportLeaseHeld
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for sleepCtx(runCtx, opts.Renew) == nil {
			if ok, err := lease.renew(runCtx); err != nil || !ok {
				opts.Logger.Error("metric import: lease lost")
				cancel()
				return
			}
		}
	}()
	defer lease.release(context.WithoutCancel(ctx))
	job := &metricImport{db: db, records: records, tables: localImportTables, opts: opts}
	if err = job.run(runCtx); err != nil {
		return err
	}
	if err = putSetting(db, "update", importRecordType, strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
		return err
	}
	opts.Logger.Info("metric import: complete")
	return nil
}

type metricImport struct {
	db      clickhouse.Conn
	records importRecords
	tables  importTables
	opts    MetricImportOptions
}

func (j *metricImport) run(ctx context.Context) error {
	h0, err := j.h0(ctx)
	if err != nil {
		return err
	}
	rollup, err := tableExists(j.db, j.tables.rollup)
	if err != nil {
		return err
	}
	has, err := j.anyMetricRows(ctx, rollup)
	if err != nil || !has {
		return err
	}
	recs, err := j.records.all(ctx)
	if err != nil {
		return err
	}
	floor, err := j.floor(ctx, recs, h0)
	if err != nil {
		return err
	}
	if recs[seriesRecord] != unitDone {
		if err = j.unit(ctx, seriesRecord, j.series); err != nil {
			return err
		}
	}
	var oldest15s time.Time
	if rollup {
		ns, ok, err := j.minNs(ctx, fmt.Sprintf("SELECT minOrNull(timestamp_ns) FROM %s WHERE %s AND timestamp_ns < %d",
			j.tables.rollup, metricRows, floor.UnixNano()))
		if err != nil {
			return err
		}
		if ok {
			oldest15s = time.Unix(0, ns).UTC()
		}
	}
	plan := planImport(h0, floor, oldest15s, j.opts.RollupDays)
	for _, p := range pendingUnits(append(plan.chunks, plan.spans...), recs) {
		u := p.unit
		if p.redo {
			for _, q := range redoSQL(j.tables, u) {
				if err = j.db.Exec(ctx, q); err != nil {
					return err
				}
			}
		}
		query := chunkSQL(j.tables, u.from, u.to)
		if u.kind == "15s" {
			query = spanSQL(j.tables, u.from, u.to, floor)
		}
		j.opts.Logger.Info(fmt.Sprintf("metric import: %s (%s, %s]", u.kind, u.from.Format(time.RFC3339), u.to.Format(time.RFC3339)))
		if err = j.unit(ctx, u.name(), func(ctx context.Context) error { return j.db.Exec(ctx, query) }); err != nil {
			return err
		}
	}
	if err = j.tail(ctx, h0, recs[watermarkRecord]); err != nil {
		return err
	}
	return j.series(ctx)
}

// unit records name started, runs it and records it done.
func (j *metricImport) unit(ctx context.Context, name string, run func(context.Context) error) error {
	if err := j.records.put(ctx, name, unitStarted); err != nil {
		return err
	}
	if err := run(ctx); err != nil {
		return err
	}
	return j.records.put(ctx, name, unitDone)
}

func (j *metricImport) series(ctx context.Context) error {
	if err := j.db.Exec(ctx, seriesSQL(j.tables)); err != nil {
		return err
	}
	return j.db.Exec(ctx, metadataSQL(j.tables))
}

// h0 is the start of the hour before T0, the metric stack's migration time.
func (j *metricImport) h0(ctx context.Context) (time.Time, error) {
	settings := "settings"
	if j.opts.Dist {
		settings += distconfig.Suffix()
	}
	var t0 string
	err := j.db.QueryRow(ctx, fmt.Sprintf("SELECT argMax(value, inserted_at) FROM %s "+
		"WHERE type = 'update' AND name = 'metric_stack'", settings)).Scan(&t0)
	if err != nil {
		return time.Time{}, err
	}
	sec, err := strconv.ParseInt(t0, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("no metric stack migration time in settings: %q", t0)
	}
	return time.Unix(sec, 0).UTC().Truncate(time.Hour).Add(-time.Hour), nil
}

func (j *metricImport) anyMetricRows(ctx context.Context, rollup bool) (bool, error) {
	tables := []string{j.tables.timeSeries, j.tables.samples}
	if rollup {
		tables = append(tables, j.tables.rollup)
	}
	for _, t := range tables {
		var n uint64
		err := j.db.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM (SELECT 1 FROM %s WHERE %s LIMIT 1)",
			t, metricRows)).Scan(&n)
		if err != nil || n > 0 {
			return n > 0, err
		}
	}
	return false, nil
}

func (j *metricImport) minNs(ctx context.Context, query string) (int64, bool, error) {
	var ns *int64
	if err := j.db.QueryRow(ctx, query).Scan(&ns); err != nil || ns == nil {
		return 0, false, err
	}
	return *ns, true, nil
}

// floor reads the raw chunks' lower bound, recording it on the first run.
func (j *metricImport) floor(ctx context.Context, recs map[string]string, h0 time.Time) (time.Time, error) {
	if ms, err := strconv.ParseInt(recs[floorRecord], 10, 64); err == nil {
		return time.UnixMilli(ms).UTC(), nil
	}
	ns, ok, err := j.minNs(ctx, fmt.Sprintf("SELECT minOrNull(timestamp_ns) FROM %s WHERE %s AND timestamp_ns < %d",
		j.tables.samples, metricRows, (h0.UnixMilli()+1)*msNs))
	if err != nil {
		return time.Time{}, err
	}
	var oldest time.Time
	if ok {
		oldest = time.Unix(0, ns).UTC()
	}
	floor := importFloor(h0, oldest, j.opts.SamplesDays)
	return floor, j.records.put(ctx, floorRecord, strconv.FormatInt(floor.UnixMilli(), 10))
}

// tail copies the metric rows old writers add above the watermark, an hour of rows at a time,
// until none arrives for the quiet period. The watermark starts at the last ns flooring to h0.
func (j *metricImport) tail(ctx context.Context, h0 time.Time, recorded string) error {
	w, err := strconv.ParseInt(recorded, 10, 64)
	if err != nil {
		w = (h0.UnixMilli()+1)*msNs - 1
	}
	lastRow := time.Now()
	for {
		var lo, hi *int64
		err := j.db.QueryRow(ctx, fmt.Sprintf("SELECT minOrNull(timestamp_ns), maxOrNull(timestamp_ns) FROM %s "+
			"WHERE %s AND timestamp_ns > %d AND timestamp_ns <= %d",
			j.tables.samples, metricRows, w, time.Now().UnixNano())).Scan(&lo, &hi)
		if err != nil {
			return err
		}
		if lo == nil {
			if time.Since(lastRow) >= j.opts.Quiet {
				return nil
			}
			if err = sleepCtx(ctx, j.opts.Poll); err != nil {
				return err
			}
			continue
		}
		hour := int64(time.Hour)
		upper := min(*hi, *lo-*lo%hour+hour)
		j.opts.Logger.Info(fmt.Sprintf("metric import: tail above %d", w))
		if err = j.db.Exec(ctx, tailSQL(j.tables, w, upper)); err != nil {
			return err
		}
		w = upper
		if err = j.records.put(ctx, watermarkRecord, strconv.FormatInt(w, 10)); err != nil {
			return err
		}
		lastRow = time.Now()
	}
}

// settingsRecords keeps the import's records in settings under type 'metric_import'.
type settingsRecords struct {
	db   clickhouse.Conn
	dist bool
}

func (s *settingsRecords) all(ctx context.Context) (map[string]string, error) {
	settings := "settings"
	if s.dist {
		settings += distconfig.Suffix()
	}
	rows, err := s.db.Query(ctx, fmt.Sprintf("SELECT argMax(name, inserted_at), argMax(value, inserted_at) "+
		"FROM %s WHERE type = $1 GROUP BY fingerprint", settings), importRecordType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, value string
		if err = rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		out[name] = value
	}
	return out, rows.Err()
}

func (s *settingsRecords) put(ctx context.Context, name, value string) error {
	fp := helputils.FingerprintLabelsDJBHashPrometheus(
		fmt.Appendf(nil, `{"type":%s, "name":%s`, strconv.Quote(importRecordType), strconv.Quote(name)))
	return s.db.Exec(ctx, `INSERT INTO settings (fingerprint, type, name, value, inserted_at)
VALUES ($1, $2, $3, $4, now64(9))`, fp, importRecordType, name, value)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
