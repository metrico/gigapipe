package maintenance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/metrico/qryn/v5/ctrl/logger"
)

// MetricMigrationOptions configures one metric migration run.
type MetricMigrationOptions struct {
	// Database names the database in each unit's query id and in the report.
	Database string
	// Instance names this instance in the lease.
	Instance string
	// SamplesDays and RollupDays are the lifetimes of samples_v3 and metrics_15s.
	SamplesDays int
	RollupDays  int
	// Lookback is how far below the watermark a later run copies again; Margin keeps the
	// hours ending after now − Margin out of the run.
	Lookback time.Duration
	Margin   time.Duration
	// Split is the initial fingerprint-range split of a unit; MaxSplit is the largest.
	Split    int
	MaxSplit int
	// Per-statement limits.
	MaxThreads       int
	MaxMemory        uint64
	MaxExecutionTime time.Duration
	ExternalSort     uint64
	ExternalGroupBy  uint64
	MaxSpill         uint64
	// Pause is the wait between units.
	Pause time.Duration
	// Back-off thresholds: a fraction of parts_to_delay_insert, a merge count and a
	// fraction of the server memory limit.
	BackoffParts  float64
	BackoffMerges int
	BackoffMemory float64
	// LeaseTTL is the age at which a lease is taken over; Renew is how often it is refreshed;
	// Settle is the wait before an acquired lease is read back.
	LeaseTTL time.Duration
	Renew    time.Duration
	Settle   time.Duration
	Logger   logger.ILogger

	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

const (
	kib = uint64(1024)
	mib = 1024 * kib
	gib = 1024 * mib
)

func (o MetricMigrationOptions) withDefaults() MetricMigrationOptions {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	defInt := func(d *int, v int) {
		if *d == 0 {
			*d = v
		}
	}
	defBytes := func(d *uint64, v uint64) {
		if *d == 0 {
			*d = v
		}
	}
	defFrac := func(d *float64, v float64) {
		if *d == 0 {
			*d = v
		}
	}
	def(&o.Lookback, time.Hour)
	def(&o.Margin, 5*time.Minute)
	defInt(&o.Split, 1)
	defInt(&o.MaxSplit, 16)
	defInt(&o.MaxThreads, 2)
	defBytes(&o.MaxMemory, 2*gib)
	def(&o.MaxExecutionTime, 5*time.Minute)
	defBytes(&o.ExternalSort, 512*mib)
	defBytes(&o.ExternalGroupBy, 512*mib)
	defBytes(&o.MaxSpill, 20*gib)
	def(&o.Pause, time.Second)
	defFrac(&o.BackoffParts, 0.5)
	defInt(&o.BackoffMerges, 8)
	defFrac(&o.BackoffMemory, 0.8)
	def(&o.LeaseTTL, 3*time.Minute)
	def(&o.Renew, time.Minute)
	def(&o.Settle, 5*time.Second)
	if o.Instance == "" {
		host, _ := os.Hostname()
		o.Instance = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if o.Logger == nil {
		o.Logger = logger.Logger
	}
	if o.now == nil {
		o.now = time.Now
	}
	if o.sleep == nil {
		o.sleep = sleepCtx
	}
	return o
}

// MetricMigrationReport sums up one migration run.
type MetricMigrationReport struct {
	Database   string
	Units      int
	RawSamples uint64
	// Rollup15sSamples are the approximate samples read from metrics_15s into the tiers.
	Rollup15sSamples uint64
	Series           uint64
	Metadata         uint64
	// Recovered counts the units a run left started that this run completed.
	Recovered int
	Duration  time.Duration
}

// Summary is the run's summary line.
func (r MetricMigrationReport) Summary() string {
	return fmt.Sprintf("metric migration: %s: %d raw samples, %d approximate 15s samples, %d series, %d metadata, %d recovered units in %s",
		r.Database, r.RawSamples, r.Rollup15sSamples, r.Series, r.Metadata, r.Recovered, r.Duration.Round(time.Millisecond))
}

// NoNewData reports a run that migrated no raw or 15s samples and recovered no unit.
func (r MetricMigrationReport) NoNewData() bool {
	return r.RawSamples == 0 && r.Rollup15sSamples == 0 && r.Recovered == 0
}

// MigrationFailedError reports a run that stopped at Step, a unit or a run step.
type MigrationFailedError struct {
	Database string
	Step     string
	Err      error
}

func (e *MigrationFailedError) Error() string {
	return fmt.Sprintf("metric migration of %s failed at %s: %v", e.Database, e.Step, e.Err)
}

func (e *MigrationFailedError) Unwrap() error { return e.Err }

// MigrationRefusedError reports a run refused before copying anything.
type MigrationRefusedError struct {
	Reason string
}

func (e *MigrationRefusedError) Error() string {
	return "metric migration refused: " + e.Reason
}

// MigrationLeaseHeldError reports that another instance holds the migration's lease.
type MigrationLeaseHeldError struct {
	Holder string
	// Renewed is when the holder last wrote the lease; TakeoverIn is how long until it expires.
	Renewed    time.Time
	TakeoverIn time.Duration
}

func (e *MigrationLeaseHeldError) Error() string {
	return fmt.Sprintf("metric migration lease held by %s, renewed %s, taken over in %s",
		e.Holder, e.Renewed.UTC().Format(time.RFC3339), e.TakeoverIn.Round(time.Second))
}

// errLeaseLost reports that the lease passed to another instance during the run.
var errLeaseLost = errors.New("the metric migration lease passed to another instance")

// errLeaseRenew wraps an error renewing the lease.
var errLeaseRenew = errors.New("renew the metric migration lease")

// RunMetricMigration runs one migration of samples_v3, time_series and metrics_15s into the metric stack
// while holding the lease, and reports what it copied.
func RunMetricMigration(ctx context.Context, exec Executor, opts MetricMigrationOptions) (MetricMigrationReport, error) {
	opts = opts.withDefaults()
	start := opts.now()
	records := &settingsRecords{exec: exec}
	lease := &migrationLease{
		records:  records,
		instance: opts.Instance,
		ttl:      opts.LeaseTTL,
		settle:   opts.Settle,
		now:      opts.now,
		sleep:    opts.sleep,
	}
	failed := func(step string, err error) error {
		return &MigrationFailedError{Database: opts.Database, Step: step, Err: err}
	}
	held, err := lease.acquire(ctx)
	if err != nil {
		return MetricMigrationReport{}, failed("lease", err)
	}
	if !held {
		who, age, err := lease.holder(ctx)
		if err != nil {
			return MetricMigrationReport{}, failed("lease", err)
		}
		return MetricMigrationReport{}, &MigrationLeaseHeldError{
			Holder:     who,
			Renewed:    opts.now().Add(-age),
			TakeoverIn: max(opts.LeaseTTL-age, 0),
		}
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		for sleepCtx(runCtx, opts.Renew) == nil {
			ok, err := lease.renew(runCtx)
			if err != nil {
				cancel(fmt.Errorf("%w: %w", errLeaseRenew, err))
				return
			}
			if !ok {
				cancel(errLeaseLost)
				return
			}
		}
	}()
	m := &migration{exec: newGatedExecutor(exec, opts), control: exec, records: records, lease: lease, opts: opts}
	report, err := m.run(runCtx)
	// stop is the renewal's reason for cancelling the run, if any.
	var stop error
	if cause := context.Cause(runCtx); errors.Is(cause, errLeaseLost) || errors.Is(cause, errLeaseRenew) {
		stop = cause
	}
	cancel(nil)
	<-renewed
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer rcancel()
	releaseErr := lease.release(rctx)
	var f *MigrationFailedError
	switch {
	case errors.As(err, &f):
		if stop != nil {
			f.Err = stop
		}
		return report, f
	case stop != nil:
		return report, failed("lease", stop)
	case releaseErr != nil:
		return report, failed("lease", releaseErr)
	}
	report.Duration = opts.now().Sub(start)
	return report, nil
}
