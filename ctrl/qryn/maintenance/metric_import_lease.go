package maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// importRecords reads and writes the metric import's records, keyed by name.
type importRecords interface {
	all(ctx context.Context) (map[string]string, error)
	put(ctx context.Context, name, value string) error
}

const leaseRecord = "lease"

// importLease is the record naming the one instance that runs the import:
// '<instance>:<unix time>', taken over once older than ttl, empty when released.
type importLease struct {
	records  importRecords
	instance string
	ttl      time.Duration
	settle   time.Duration
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
}

func (l *importLease) holder(ctx context.Context) (string, time.Time, error) {
	records, err := l.records.all(ctx)
	if err != nil {
		return "", time.Time{}, err
	}
	v := records[leaseRecord]
	i := strings.LastIndexByte(v, ':')
	if i < 0 {
		return "", time.Time{}, nil
	}
	sec, err := strconv.ParseInt(v[i+1:], 10, 64)
	if err != nil {
		return "", time.Time{}, nil
	}
	return v[:i], time.Unix(sec, 0), nil
}

func (l *importLease) write(ctx context.Context) error {
	return l.records.put(ctx, leaseRecord, fmt.Sprintf("%s:%d", l.instance, l.now().Unix()))
}

// acquire takes the lease when it is free, expired or already held, and reports
// whether this instance still holds it after settle.
func (l *importLease) acquire(ctx context.Context) (bool, error) {
	who, at, err := l.holder(ctx)
	if err != nil {
		return false, err
	}
	if who != "" && who != l.instance && l.now().Sub(at) < l.ttl {
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
