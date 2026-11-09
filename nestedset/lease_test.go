package nestedset

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func TestDecide(t *testing.T) {
	for _, c := range []struct {
		name   string
		rows   []LeaseRow
		now    time.Time
		err    error
		passOK bool
		want   decision
	}{
		{name: "empty table: claim the server's Unix second", now: at(0), passOK: true,
			want: decision{claim, uint64(t0.Unix())}},
		{name: "expired by server time: claim the next term",
			rows: []LeaseRow{{Term: 7, Holder: "b", Claim: true, ExpiresAt: at(1000)}, {Term: 7, Holder: "b", Claim: false, ExpiresAt: at(4000)}}, now: at(4000), passOK: true,
			want: decision{claim, 8}},
		{name: "still ahead of server time: another holder leads",
			rows: []LeaseRow{{Term: 7, Holder: "b", Claim: true, ExpiresAt: at(1000)}, {Term: 7, Holder: "b", Claim: false, ExpiresAt: at(4000)}}, now: at(3999), passOK: true,
			want: decision{notLeader, 7}},
		{name: "holder renews after a good pass",
			rows: []LeaseRow{{Term: 7, Holder: "me", Claim: true, ExpiresAt: at(1000)}}, now: at(500), passOK: true,
			want: decision{renew, 7}},
		{name: "no renewal after a failed pass, but the holder still leads",
			rows: []LeaseRow{{Term: 7, Holder: "me", Claim: true, ExpiresAt: at(1000)}}, now: at(500), passOK: false,
			want: decision{lead, 7}},
		{name: "racing claims: the earliest claim wins",
			rows: []LeaseRow{{Term: 3, Holder: "me", Claim: true, ExpiresAt: at(1001)}, {Term: 3, Holder: "a", Claim: true, ExpiresAt: at(1000)}}, now: at(500), passOK: true,
			want: decision{notLeader, 3}},
		{name: "a stale claim stored late loses in its term",
			rows: []LeaseRow{{Term: 3, Holder: "me", Claim: true, ExpiresAt: at(1000)}, {Term: 3, Holder: "late", Claim: true, ExpiresAt: at(9000)}}, now: at(5000), passOK: true,
			want: decision{renew, 3}},
		{name: "only the highest term counts",
			rows: []LeaseRow{{Term: 2, Holder: "me", Claim: true, ExpiresAt: at(1000)}, {Term: 3, Holder: "a", Claim: true, ExpiresAt: at(2000)}}, now: at(500), passOK: true,
			want: decision{notLeader, 3}},
		{name: "a term whose claim aged out is held by its renewer",
			rows: []LeaseRow{{Term: 3, Holder: "me", Claim: false, ExpiresAt: at(2000)}}, now: at(500), passOK: true,
			want: decision{renew, 3}},
		{name: "a term whose claim aged out is held by another renewer",
			rows: []LeaseRow{{Term: 3, Holder: "a", Claim: false, ExpiresAt: at(2000)}}, now: at(500), passOK: true,
			want: decision{notLeader, 3}},
		{name: "renewals tied on expiry: the greatest holder name holds the term",
			rows: []LeaseRow{{Term: 3, Holder: "me", Claim: false, ExpiresAt: at(4000)}, {Term: 3, Holder: "a", Claim: false, ExpiresAt: at(4000)}}, now: at(500), passOK: true,
			want: decision{renew, 3}},
		{name: "renewals tied on expiry, read in the other order",
			rows: []LeaseRow{{Term: 3, Holder: "a", Claim: false, ExpiresAt: at(4000)}, {Term: 3, Holder: "me", Claim: false, ExpiresAt: at(4000)}}, now: at(500), passOK: true,
			want: decision{renew, 3}},
		{name: "a renewer beats a late claim that outlived the real claim",
			rows: []LeaseRow{{Term: 3, Holder: "me", Claim: false, ExpiresAt: at(4000)}, {Term: 3, Holder: "late", Claim: true, ExpiresAt: at(9000)}}, now: at(5000), passOK: true,
			want: decision{renew, 3}},
		{name: "a holder whose pass failed yields an expired lease once",
			rows: []LeaseRow{{Term: 7, Holder: "me", Claim: true, ExpiresAt: at(1000)}}, now: at(1000), passOK: false,
			want: decision{notLeader, 7}},
		{name: "any error means not leader",
			rows: []LeaseRow{{Term: 7, Holder: "me", Claim: true, ExpiresAt: at(1000)}}, now: at(500), err: errors.New("boom"), passOK: true,
			want: decision{notLeader, 0}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := decide(c.rows, c.now, c.err, "me", c.passOK); got != c.want {
				t.Errorf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestHolderIDNamesHostAndPIDWithARandomSuffix(t *testing.T) {
	host, _ := os.Hostname()
	a, b := holderID(), holderID()
	if !strings.HasPrefix(a, host+"-"+strconv.Itoa(os.Getpid())+"-") {
		t.Errorf("holder %q, want it to start with host %q and pid %d", a, host, os.Getpid())
	}
	if a == b {
		t.Errorf("two holder ids are both %q", a)
	}
}
