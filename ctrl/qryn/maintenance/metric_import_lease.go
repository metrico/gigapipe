package maintenance

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// importRecords reads and writes the metric import's records, keyed by name.
type importRecords interface {
	all(ctx context.Context) (map[string]string, error)
	put(ctx context.Context, name, value string) error
	// age is the server time elapsed since name was last written.
	age(ctx context.Context, name string) (time.Duration, error)
}

const leaseRecord = "lease"

// importLease is the record naming the one instance that runs the import:
// '<instance>:<unix time>', taken over once its write is older than ttl by the server clock,
// empty when released.
type importLease struct {
	records  importRecords
	instance string
	ttl      time.Duration
	settle   time.Duration
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
}

func (l *importLease) holder(ctx context.Context) (string, time.Duration, error) {
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

func (l *importLease) write(ctx context.Context) error {
	return l.records.put(ctx, leaseRecord, fmt.Sprintf("%s:%d", l.instance, l.now().Unix()))
}

// acquire takes the lease when it is free, expired or already held, and reports
// whether this instance still holds it after settle.
func (l *importLease) acquire(ctx context.Context) (bool, error) {
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
func (l *importLease) held(ctx context.Context) (bool, error) {
	who, _, err := l.holder(ctx)
	return who == l.instance, err
}

// renew refreshes the lease while this instance holds it and reports whether it does.
func (l *importLease) renew(ctx context.Context) (bool, error) {
	who, _, err := l.holder(ctx)
	if err != nil || who != l.instance {
		return false, err
	}
	return true, l.write(ctx)
}

// release frees the lease when this instance holds it.
func (l *importLease) release(ctx context.Context) error {
	who, _, err := l.holder(ctx)
	if err != nil || who != l.instance {
		return err
	}
	return l.records.put(ctx, leaseRecord, "")
}
