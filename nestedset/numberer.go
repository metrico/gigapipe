package nestedset

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
)

// Store runs passes against ClickHouse and keeps the numberer's lease there.
type Store interface {
	Version(ctx context.Context) (string, error)
	// Pass runs one pass from cursor after the earlier passes' reads and
	// returns the cursor for the next and its read, zero when it took none.
	Pass(ctx context.Context, idle time.Duration, maxSpans int, cursor time.Time, reads Reads) (time.Time, RunningRead, error)
	// ReadLease returns the lease rows of the highest term and the connected
	// node's time.
	ReadLease(ctx context.Context) ([]LeaseRow, time.Time, error)
	// WriteLease inserts w, stamped by the connected node's clock, unless a
	// write with the same token was stored.
	WriteLease(ctx context.Context, w LeaseWrite) error
}

// ConnStore is a Store over a connection to a single node.
type ConnStore struct{ Conn clickhouse.Conn }

// Version returns the server version.
func (s ConnStore) Version(ctx context.Context) (string, error) {
	var v string
	err := s.Conn.QueryRow(ctx, "SELECT version()").Scan(&v)
	return v, err
}

// Pass runs one pass.
func (s ConnStore) Pass(ctx context.Context, idle time.Duration, maxSpans int, cursor time.Time, reads Reads) (time.Time, RunningRead, error) {
	return Pass(ctx, s.Conn, idle, maxSpans, cursor, reads)
}

// ReadLease reads the lease table.
func (s ConnStore) ReadLease(ctx context.Context) ([]LeaseRow, time.Time, error) {
	return s.leaseTable().read(ctx)
}

// WriteLease writes to the lease table.
func (s ConnStore) WriteLease(ctx context.Context, w LeaseWrite) error {
	return s.leaseTable().write(ctx, w)
}

func (s ConnStore) leaseTable() leaseTable { return leaseTable{conn: s.Conn, name: leaseTableName} }

// ClusterStore is a Store over a connection to one node of a cluster. Each
// pass numbers every shard's traces on that shard.
type ClusterStore struct {
	Conn              clickhouse.Conn
	Cluster, Database string
}

// Version returns the server version of the connected node.
func (s ClusterStore) Version(ctx context.Context) (string, error) {
	return ConnStore{Conn: s.Conn}.Version(ctx)
}

// Pass runs one pass on every shard.
func (s ClusterStore) Pass(ctx context.Context, idle time.Duration, maxSpans int, cursor time.Time, reads Reads) (time.Time, RunningRead, error) {
	return ClusterPass(ctx, s.Conn, s.Cluster, s.Database, idle, maxSpans, cursor, reads)
}

// ReadLease reads the lease through its distributed table.
func (s ClusterStore) ReadLease(ctx context.Context) ([]LeaseRow, time.Time, error) {
	return s.leaseTable().read(ctx)
}

// WriteLease writes to the lease through its distributed table, waiting for
// the shard to store the row.
func (s ClusterStore) WriteLease(ctx context.Context, w LeaseWrite) error {
	return s.leaseTable().write(ctx, w)
}

func (s ClusterStore) leaseTable() leaseTable {
	return leaseTable{
		conn:     s.Conn,
		name:     tables{db: s.Database}.prefix() + leaseTableName + "_dist",
		settings: clickhouse.Settings{"distributed_foreground_insert": 1},
	}
}

// minVersion is the oldest ClickHouse the pass runs on.
var minVersion = [2]int{24, 9}

// SupportedVersion reports whether a ClickHouse version string is at least minVersion.
func SupportedVersion(version string) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	return major > minVersion[0] || major == minVersion[0] && minor >= minVersion[1]
}

// Numberer takes part in the lease every interval and runs a pass while it
// holds it, until stopped.
type Numberer struct {
	cancel context.CancelFunc
	done   sync.WaitGroup
}

// Start checks the server version and starts the loop. Below 24.9, or when
// the version can't be read, it logs one error and returns nil.
func Start(ctx context.Context, store Store, cfg Config, log logger.ILogger) *Numberer {
	version, err := store.Version(ctx)
	if err != nil {
		log.Error("nested-set numberer not started: reading the ClickHouse version: ", err.Error())
		return nil
	}
	if !SupportedVersion(version) {
		log.Error(fmt.Sprintf("nested-set numberer not started: it needs ClickHouse %d.%d or newer, the server runs %s",
			minVersion[0], minVersion[1], version))
		return nil
	}
	e := &elector{store: store, me: holderID(), leaseLen: cfg.LeaseLen(), log: log, passOK: true}
	ctx, cancel := context.WithCancel(ctx)
	n := &Numberer{cancel: cancel}
	n.done.Add(1)
	go func() {
		defer n.done.Done()
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if !e.step(ctx) {
				continue
			}
			if err := e.pass(ctx, cfg); err != nil && ctx.Err() == nil {
				log.Error("nested-set numberer pass: ", err.Error())
			}
		}
	}()
	log.Info(fmt.Sprintf("nested-set numberer started as %s (idle %s, interval %s, lease %s, max spans %d)",
		e.me, cfg.Idle, cfg.Interval, cfg.LeaseLen(), cfg.MaxSpans))
	return n
}

// Stop cancels the running pass and waits for the loop to end.
func (n *Numberer) Stop() {
	n.cancel()
	n.done.Wait()
}

// elector is one instance's side of the lease.
type elector struct {
	store    Store
	me       string
	leaseLen time.Duration
	log      logger.ILogger
	passOK   bool      // the last pass succeeded or found nothing to do
	cursor   time.Time // where the next pass scans from (SQL clamps it to now - 1 day)
	reads    Reads     // the running reads of this instance's passes, in every term
	led      uint64    // the term this instance last led, 0 after stepping down
	renewals uint64
	outage   bool
}

// pass runs one pass from the cursor and advances the cursor when it succeeds.
func (e *elector) pass(ctx context.Context, cfg Config) error {
	next, read, err := e.store.Pass(ctx, cfg.Idle, cfg.MaxSpans, e.cursor, e.reads)
	e.reads = e.reads.add(read)
	e.passOK = err == nil
	if err == nil {
		e.cursor = next
	}
	return err
}

// step claims or renews the lease as decide says and reports whether this
// instance leads for the iteration.
func (e *elector) step(ctx context.Context) bool {
	rows, now, err := e.store.ReadLease(ctx)
	d := decide(rows, now, err, e.me, e.passOK)
	switch d.action {
	case claim:
		err = e.store.WriteLease(ctx, LeaseWrite{Term: d.term, Holder: e.me, Claim: true, LeaseLen: e.leaseLen,
			Cursor: inheritedCursor(rows), Token: claimToken(d.term)})
		if err == nil {
			rows, now, err = e.store.ReadLease(ctx)
		}
		if err == nil {
			term, holder, live := leader(rows, now)
			d = decision{notLeader, term}
			if live && holder == e.me {
				d.action = lead
				e.cursor = inheritedCursor(rows)
			}
		}
	case renew:
		e.renewals++
		err = e.store.WriteLease(ctx, LeaseWrite{Term: d.term, Holder: e.me, Claim: false, LeaseLen: e.leaseLen,
			Cursor: e.cursor, Token: fmt.Sprintf("renew-%d-%s-%d", d.term, e.me, e.renewals)})
	case notLeader:
		e.passOK = true
	}
	if err != nil {
		if !e.outage && ctx.Err() == nil {
			e.outage = true
			e.log.Error("nested-set numberer lease outage, not numbering: ", err.Error())
		}
		return false
	}
	if e.outage {
		e.outage = false
		e.log.Info("nested-set numberer lease outage over")
	}
	leading := d.action == renew || d.action == lead
	switch {
	case leading && d.term != e.led:
		e.led = d.term
		e.log.Info(fmt.Sprintf("nested-set numberer: %s won term %d and runs the passes", e.me, d.term))
	case !leading && e.led != 0 && d.term > e.led:
		e.led = 0
		e.log.Info(fmt.Sprintf("nested-set numberer: %s stepped down to term %d", e.me, d.term))
	}
	return leading
}
