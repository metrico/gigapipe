package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/ctrl/qryn/helputils"
)

// migrationRecords reads and writes the metric migration's records, keyed by name.
type migrationRecords interface {
	all(ctx context.Context) (map[string]string, error)
	put(ctx context.Context, name, value string) error
	// age is the server time elapsed since name was last written.
	age(ctx context.Context, name string) (time.Duration, error)
}

const leaseRecord = "lease"

// migrationLease is the record naming the one instance that runs the migration:
// '<instance>:<unix time>', taken over once its write is older than ttl by the server clock,
// empty when released.
type migrationLease struct {
	records  migrationRecords
	instance string
	ttl      time.Duration
	settle   time.Duration
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
}

func (l *migrationLease) holder(ctx context.Context) (string, time.Duration, error) {
	records, err := l.records.all(ctx)
	if err != nil {
		return "", 0, err
	}
	v := records[leaseRecord]
	i := strings.LastIndexByte(v, ':')
	if i < 0 {
		return "", 0, nil
	}
	age, err := l.records.age(ctx, leaseRecord)
	return v[:i], age, err
}

func (l *migrationLease) write(ctx context.Context) error {
	return l.records.put(ctx, leaseRecord, fmt.Sprintf("%s:%d", l.instance, l.now().Unix()))
}

// acquire takes the lease when it is free, expired or already held, and reports
// whether this instance still holds it after settle.
func (l *migrationLease) acquire(ctx context.Context) (bool, error) {
	who, age, err := l.holder(ctx)
	if err != nil {
		return false, err
	}
	if who != "" && who != l.instance && age < l.ttl {
		return false, nil
	}
	if err = l.write(ctx); err != nil {
		return false, err
	}
	if err = l.sleep(ctx, l.settle); err != nil {
		return false, err
	}
	who, _, err = l.holder(ctx)
	return who == l.instance, err
}

// held reports whether this instance holds the lease.
func (l *migrationLease) held(ctx context.Context) (bool, error) {
	who, _, err := l.holder(ctx)
	return who == l.instance, err
}

// renew refreshes the lease while this instance holds it and reports whether it does.
func (l *migrationLease) renew(ctx context.Context) (bool, error) {
	who, _, err := l.holder(ctx)
	if err != nil || who != l.instance {
		return false, err
	}
	return true, l.write(ctx)
}

// release frees the lease when this instance holds it.
func (l *migrationLease) release(ctx context.Context) error {
	who, _, err := l.holder(ctx)
	if err != nil || who != l.instance {
		return err
	}
	return l.records.put(ctx, leaseRecord, "")
}

const (
	migrationRecordType = "metric_migrate"
	floorRecord         = "floor"
	seriesRecord        = "series"
	watermarkRecord     = "watermark"
	unitStarted         = "started"
	unitDone            = "done"
)

// settingsRecords keeps the migration's records in settings under type 'metric_migrate'.
type settingsRecords struct {
	exec Executor
}

func (s *settingsRecords) all(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	err := s.exec.Rows(ctx, Statement{
		Op:    opRecords,
		Table: "settings",
		SQL: "SELECT argMax(name, inserted_at), argMax(value, inserted_at) " +
			"FROM settings WHERE type = $1 GROUP BY fingerprint",
		Args: []any{migrationRecordType},
	}, func(scan func(dest ...any) error) error {
		var name, value string
		if err := scan(&name, &value); err != nil {
			return err
		}
		out[name] = value
		return nil
	})
	return out, err
}

func (s *settingsRecords) put(ctx context.Context, name, value string) error {
	_, err := s.exec.Exec(ctx, Statement{
		Op:    opRecord,
		Table: "settings",
		SQL: "INSERT INTO settings (fingerprint, type, name, value, inserted_at) " +
			"VALUES ($1, $2, $3, $4, now64(9))",
		Args: []any{migrationRecordFingerprint(name), migrationRecordType, name, value},
	})
	return err
}

func (s *settingsRecords) age(ctx context.Context, name string) (time.Duration, error) {
	var ms int64
	err := s.exec.Scalar(ctx, Statement{
		Op:    opRecordAge,
		Table: "settings",
		SQL:   "SELECT dateDiff('millisecond', max(inserted_at), now64(9)) FROM settings WHERE fingerprint = $1",
		Args:  []any{migrationRecordFingerprint(name)},
	}, &ms)
	return time.Duration(ms) * time.Millisecond, err
}

func migrationRecordFingerprint(name string) uint32 {
	return helputils.FingerprintLabelsDJBHashPrometheus(
		fmt.Appendf(nil, `{"type":%s, "name":%s`, strconv.Quote(migrationRecordType), strconv.Quote(name)))
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
