package clickhouse_transpiler

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/shared/chtest"
)

// boundaryIDs are trace ids at the edges of the id space and of its halves.
var boundaryIDs = []string{
	"00000000000000000000000000000000", "00000000000000000000000000000001",
	"0000000000000000ffffffffffffffff", "00000000000000010000000000000000",
	"00000000000000010000000000000001", "00000000000000000000000000000abc",
	"7fffffffffffffffffffffffffffffff", "80000000000000000000000000000000",
	"55555555555555550000000000000000", "55555555555555560000000000000000",
	"00000000000000005555555555555555", "00000000000000005555555555555554",
	"aaaaaaaaaaaaaaab0000000000000000", "aaaaaaaaaaaaaaaaffffffffffffffff",
	"ffffffffffffffff0000000000000000", "ffffffffffffffffffffffffffffffff",
	"0123456789abcdef0000000000000000",
}

func inRange(id []byte, r idRange) bool {
	return bytes.Compare(id, r.lo[:]) >= 0 && (r.hi == nil || bytes.Compare(id, r.hi[:]) < 0)
}

// Each trace id, of either half, falls in exactly one of the n portions.
func TestTraceIDPortionsPartitionEveryID(t *testing.T) {
	ids := make([][]byte, 0, len(boundaryIDs)+2000)
	for _, s := range boundaryIDs {
		b, _ := hex.DecodeString(s)
		ids = append(ids, b)
	}
	rnd := rand.New(rand.NewSource(1))
	for range 1000 {
		var full, short [16]byte
		rnd.Read(full[:])
		rnd.Read(short[8:])
		ids = append(ids, full[:], short[:])
	}
	for _, n := range []int{1, 2, 3, 5, 7, 64, 1000} {
		perPortion := make([]int, n)
		for _, id := range ids {
			var in []int
			for i := range n {
				for _, r := range traceIDPortion(n, i) {
					if inRange(id, r) {
						in = append(in, i)
					}
				}
			}
			if len(in) != 1 {
				t.Fatalf("%d portions: %x in portions %v", n, id, in)
			}
			perPortion[in[0]]++
		}
		if n <= 7 {
			for i, c := range perPortion {
				if c < len(ids)/n/2 {
					t.Errorf("%d portions: portion %d holds %d of %d ids", n, i, c, len(ids))
				}
			}
		}
	}
}

// ClickHouse puts every stored id, short hex ids padded by the ingest cast
// among them, in exactly one portion.
func TestTraceIDPortionsPartitionStoredIDs(t *testing.T) {
	conn, _ := chtest.Open(t)
	ctx := context.Background()
	if err := conn.Exec(ctx, "CREATE TABLE ids (trace_id FixedString(16)) ENGINE = MergeTree ORDER BY trace_id"); err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, id := range append(shortHexIDs(), boundaryIDs...) {
		rows = append(rows, fmt.Sprintf("(unhex('%s')::FixedString(16))", id))
	}
	if err := conn.Exec(ctx, "INSERT INTO ids SELECT * FROM VALUES('trace_id FixedString(16)', "+strings.Join(rows, ", ")+")"); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 2, 3, 5, 7} {
		conds := make([]string, n)
		for i := range n {
			conds[i] = "toUInt8(" + traceIDPortionSQL(n, i) + ")"
		}
		res, err := conn.Query(ctx, "SELECT hex(trace_id), toUInt64("+strings.Join(conds, " + ")+") FROM ids")
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for res.Next() {
			var id string
			var in uint64
			if err := res.Scan(&id, &in); err != nil {
				t.Fatal(err)
			}
			seen++
			if in != 1 {
				t.Errorf("%d portions: %s in %d portions", n, id, in)
			}
		}
		if err := res.Err(); err != nil {
			t.Fatal(err)
		}
		res.Close()
		if seen == 0 {
			t.Fatal("no ids read")
		}
	}
}

// shortHexIDs are hex ids shorter than 16 bytes.
func shortHexIDs() []string {
	return []string{"ab", "00", "0123456789abcdef", "ffffffffffffffff", "0000000000000001", "00000000000000000001"}
}
