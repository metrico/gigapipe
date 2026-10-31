package nestedset

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// LeaseRow is a row of the lease table: a claim of a term, or its holder's
// renewal. On a read, Cursor is the cursor of the latest row by term and
// expires_at that has one.
type LeaseRow struct {
	Term      uint64
	Holder    string
	Claim     bool
	ExpiresAt time.Time
	Cursor    time.Time
}

// inheritedCursor is the cursor rows read from the lease carry; zero when
// none has one.
func inheritedCursor(rows []LeaseRow) time.Time {
	for _, r := range rows {
		if cursorSet(r.Cursor) {
			return r.Cursor
		}
	}
	return time.Time{}
}

type action int

const (
	notLeader action = iota
	claim
	renew
	lead
)

// decision is what an instance does in one iteration, and the term it
// claims, renews, leads or sees.
type decision struct {
	action action
	term   uint64
}

// leader returns the highest term, its holder (latest renewer, greatest name on
// a tie; else earliest claimant, least name) and whether it is live at now.
func leader(rows []LeaseRow, now time.Time) (term uint64, holder string, live bool) {
	for _, r := range rows {
		term = max(term, r.Term)
	}
	var claimed, latest, renewed time.Time
	claimant, renewer := "", ""
	for _, r := range rows {
		if r.Term != term {
			continue
		}
		if r.ExpiresAt.After(latest) {
			latest = r.ExpiresAt
		}
		switch {
		case !r.Claim && (renewer == "" || r.ExpiresAt.After(renewed) || r.ExpiresAt.Equal(renewed) && r.Holder > renewer):
			renewed, renewer = r.ExpiresAt, r.Holder
		case r.Claim && (claimant == "" || r.ExpiresAt.Before(claimed) || r.ExpiresAt.Equal(claimed) && r.Holder < claimant):
			claimed, claimant = r.ExpiresAt, r.Holder
		}
	}
	holder = renewer
	if holder == "" {
		holder = claimant
	}
	return term, holder, latest.After(now)
}

// decide is me's step at server time now: claim an expired lease's next term,
// renew a held one after a good pass, else lead without renewing or not lead.
// A holder whose last pass failed lets one expired lease go unclaimed.
func decide(rows []LeaseRow, now time.Time, err error, me string, passOK bool) decision {
	if err != nil {
		return decision{notLeader, 0}
	}
	term, holder, live := leader(rows, now)
	switch {
	case !live && passOK && len(rows) == 0:
		return decision{claim, uint64(now.Unix())}
	case !live && passOK:
		return decision{claim, term + 1}
	case !live || holder != me:
		return decision{notLeader, term}
	case passOK:
		return decision{renew, term}
	default:
		return decision{lead, term}
	}
}

// holderID names this process: hostname, PID and a random suffix.
func holderID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
}

const leaseTableName = "tempo_traces_nested_set_lease"

// leaseTable reads and writes the lease table name over conn.
type leaseTable struct {
	conn     clickhouse.Conn
	name     string
	settings clickhouse.Settings
}

// read returns the rows of the highest term, collapsed per holder and kind
// to the earliest claim and the latest renewal, each with the inherited
// cursor, and the server time.
func (l leaseTable) read(ctx context.Context) ([]LeaseRow, time.Time, error) {
	rows, err := l.conn.Query(ctx, "SELECT term, holder, claim, if(claim, min(expires_at), max(expires_at)), now64(3),\n"+
		"(SELECT argMax(cursor, (term, expires_at)) FROM "+l.name+" WHERE cursor > 0)\n"+
		"FROM "+l.name+" GROUP BY term, holder, claim ORDER BY term DESC LIMIT 1 WITH TIES")
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("reading %s: %w", l.name, err)
	}
	defer rows.Close()
	var out []LeaseRow
	var now time.Time
	for rows.Next() {
		var r LeaseRow
		if err := rows.Scan(&r.Term, &r.Holder, &r.Claim, &r.ExpiresAt, &now, &r.Cursor); err != nil {
			return nil, time.Time{}, fmt.Errorf("reading %s: %w", l.name, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, time.Time{}, fmt.Errorf("reading %s: %w", l.name, err)
	}
	if len(out) == 0 {
		if err := l.conn.QueryRow(ctx, "SELECT now64(3)").Scan(&now); err != nil {
			return nil, time.Time{}, fmt.Errorf("reading %s: %w", l.name, err)
		}
	}
	return out, now, nil
}

// claimToken is the dedup token shared by every claim of term.
func claimToken(term uint64) string { return fmt.Sprintf("term-%d", term) }

// LeaseWrite is a claim of a term, or its holder's renewal, that expires
// LeaseLen after the connected node's time at insert and carries the cursor
// of the holder's next pass.
type LeaseWrite struct {
	Term     uint64
	Holder   string
	Claim    bool
	LeaseLen time.Duration
	Cursor   time.Time
	Token    string
}

// write inserts w under its insert-deduplication token.
func (l leaseTable) write(ctx context.Context, w LeaseWrite) error {
	settings := clickhouse.Settings{"insert_deduplicate": 1, "insert_deduplication_token": w.Token, "async_insert": 0}
	for k, v := range l.settings {
		settings[k] = v
	}
	q := fmt.Sprintf("INSERT INTO %s (term, holder, claim, expires_at, cursor)\nSELECT %d, %s, %t, now64(3) + toIntervalMillisecond(%d), %s",
		l.name, w.Term, quote(w.Holder), w.Claim, w.LeaseLen.Milliseconds(), timeExpr(w.Cursor))
	if err := l.conn.Exec(clickhouse.Context(ctx, clickhouse.WithSettings(settings)), q); err != nil {
		return fmt.Errorf("writing %s: %w", l.name, err)
	}
	return nil
}
