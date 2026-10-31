package nestedset

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// assertOneWinner runs one election step of n instances at once on an expired
// term-1 lease and checks that exactly one leads and one term-2 claim is stored.
func assertOneWinner(t *testing.T, store Store, conn clickhouse.Conn, table string) {
	t.Helper()
	const n = 8
	seed := LeaseWrite{Term: 1, Holder: "expired", Claim: true, LeaseLen: -time.Second, Token: claimToken(1)}
	if err := store.WriteLease(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	won := make([]bool, n)
	start := make(chan struct{})
	for i := range n {
		e := &elector{store: store, me: holderID(), leaseLen: time.Minute, log: &logs{}, passOK: true}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			won[i] = e.step(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	winners := 0
	for _, w := range won {
		if w {
			winners++
		}
	}
	if claims := countClaims(t, conn, table, 2); winners != 1 || claims != 1 {
		t.Errorf("%d winners and %d stored claims among %d claimants, want 1 and 1", winners, claims, n)
	}
	term, winner := currentLeader(t, store)
	if term != 2 {
		t.Fatalf("term %d leads, want 2", term)
	}
	late := LeaseWrite{Term: term, Holder: "late", Claim: true, LeaseLen: time.Minute, Token: claimToken(term)}
	if err := store.WriteLease(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if claims := countClaims(t, conn, table, term); claims != 1 {
		t.Errorf("a second claim of term %d was stored: %d claims", term, claims)
	}
	late.Token += "-out-of-window"
	if err := store.WriteLease(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if _, holder := currentLeader(t, store); holder != winner {
		t.Errorf("a claim stored late took the term from %s: %s leads", winner, holder)
	}
}

func currentLeader(t *testing.T, store Store) (uint64, string) {
	t.Helper()
	rows, now, err := store.ReadLease(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	term, holder, _ := leader(rows, now)
	return term, holder
}

func countClaims(t *testing.T, conn clickhouse.Conn, table string, term uint64) uint64 {
	t.Helper()
	var claims uint64
	q := "SELECT count() FROM " + table + " WHERE claim AND term = ?"
	if err := conn.QueryRow(context.Background(), q, term).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	return claims
}

func TestLeaseHasOneWinnerAmongConcurrentClaimants(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := maintenance.Update(conn, db, "", maintenance.CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	assertOneWinner(t, ConnStore{Conn: conn}, conn, leaseTableName)
}

func TestClusterLeaseHasOneWinnerAmongConcurrentClaimants(t *testing.T) {
	conn, db, cluster := chtest.OpenCluster(t)
	mode := maintenance.CLUST_MODE_SINGLE | maintenance.CLUST_MODE_DISTRIBUTED
	if err := maintenance.Update(conn, db, cluster, mode, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	assertOneWinner(t, ClusterStore{Conn: conn, Cluster: cluster, Database: db}, conn, leaseTableName+"_dist")
}

func TestLeaseIsTakenOverWithinFourIntervalsOfTheLeaderStopping(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := maintenance.Update(conn, db, "", maintenance.CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Idle: time.Second, Interval: 200 * time.Millisecond, MaxSpans: 1000}
	la, lb := &logs{}, &logs{}
	a := Start(context.Background(), ConnStore{Conn: conn}, cfg, la)
	waitFor(t, func() bool { _, won := la.count("won term"); return won == 1 })
	b := Start(context.Background(), ConnStore{Conn: conn}, cfg, lb)
	defer b.Stop()
	time.Sleep(2 * cfg.LeaseLen())
	if _, won := lb.count("won term"); won != 0 {
		t.Fatalf("the follower won while the leader lived: %q", lb.infos)
	}
	a.Stop()
	stopped := time.Now()
	waitFor(t, func() bool { _, won := lb.count("won term"); return won == 1 })
	// Interval/4 is slack for scheduling and query jitter.
	if took, limit := time.Since(stopped), cfg.LeaseLen()+cfg.Interval; took > limit+cfg.Interval/4 {
		t.Errorf("takeover took %s, want within %s", took, limit)
	}
	if len(la.errors)+len(lb.errors) != 0 {
		t.Errorf("errors: %q %q", la.errors, lb.errors)
	}
}

func TestLeaseIsClaimedAgainAfterTheTableEmpties(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := maintenance.Update(conn, db, "", maintenance.CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	store := ConnStore{Conn: conn}
	for i := range 2 {
		if i == 1 {
			// Empty the table with a mutation; the deduplication window survives it.
			if err := conn.Exec(ctx, "ALTER TABLE "+leaseTableName+" DELETE WHERE 1 SETTINGS mutations_sync = 1"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second) // a later Unix second, so a new first term
		}
		e := &elector{store: store, me: holderID(), leaseLen: time.Minute, log: &logs{}, passOK: true}
		if !e.step(ctx) {
			t.Fatalf("claim %d on an empty table: no leader", i+1)
		}
	}
}

func TestAMissingLeaseTableMeansNotLeader(t *testing.T) {
	conn, _ := chtest.Open(t)
	l := &logs{}
	n := Start(context.Background(), ConnStore{Conn: conn}, Config{Idle: time.Second, Interval: 20 * time.Millisecond, MaxSpans: 1}, l)
	time.Sleep(300 * time.Millisecond)
	n.Stop()
	if errs, _ := l.count(leaseTableName); errs != 1 || len(l.errors) != 1 {
		t.Errorf("errors %q, want one naming %s", l.errors, leaseTableName)
	}
	if _, won := l.count("won term"); won != 0 {
		t.Errorf("infos %q, want no leadership", l.infos)
	}
}
